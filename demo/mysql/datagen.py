#!/usr/bin/env python3
"""
datagen.py — Continuous MySQL data generator for the CDC pipeline demo.

Simulates realistic retail activity:
  - New orders placed every few seconds
  - Inventory updated when orders are placed
  - Occasional order status updates (pending → shipped → delivered)
  - Occasional new product added
  - Occasional store toggled active/inactive

Run from project root (after activating venv):
    python demo/mysql/datagen.py

Or with custom interval:
    python demo/mysql/datagen.py --interval 2
"""

import argparse
import logging
import os
import random
import signal
import time

import pymysql
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [datagen] %(message)s",
)
log = logging.getLogger("datagen")

# ── MySQL connection ──────────────────────────────────────────────────────────

DB_CONF = dict(
    host     = os.environ.get("MYSQL_HOST",     "localhost"),
    port     = int(os.environ.get("MYSQL_PORT", "3306")),
    db       = os.environ.get("MYSQL_DB",       "demo_db"),
    user     = os.environ.get("MYSQL_USER",     "app_user"),
    password = os.environ.get("MYSQL_PASSWORD", "app_pass"),
    charset  = "utf8mb4",
    autocommit = True,
)

ORDER_STATUSES = ["pending", "processing", "completed"]

PRODUCT_NAMES = [
    "Blue Dream Flower", "OG Kush Vape", "CBD Tincture 500mg",
    "Sativa Pre-Roll", "Indica Gummy", "Hybrid Concentrate",
    "CBG Capsule", "RSO Oil", "Delta-8 Gummy", "CBD Cream",
]

RUNNING = True


def _shutdown(sig, frame):
    global RUNNING
    log.info("Shutdown — stopping data generator.")
    RUNNING = False


signal.signal(signal.SIGTERM, _shutdown)
signal.signal(signal.SIGINT, _shutdown)


def _connect() -> pymysql.connections.Connection:
    for attempt in range(12):
        try:
            conn = pymysql.connect(**DB_CONF)
            log.info("Connected to MySQL.")
            return conn
        except pymysql.Error as e:
            log.info(f"Waiting for MySQL ({attempt + 1}/12): {e}")
            time.sleep(5)
    raise RuntimeError("MySQL never became reachable.")


def get_ids(cur, table: str, pk: str) -> list:
    cur.execute(f"SELECT {pk} FROM {table}")
    return [row[0] for row in cur.fetchall()]


# ── Event generators ──────────────────────────────────────────────────────────

def place_order(cur) -> None:
    """Insert a new order and decrement inventory."""
    store_ids   = get_ids(cur, "stores",   "store_id")
    product_ids = get_ids(cur, "products", "product_id")
    if not store_ids or not product_ids:
        return

    store_id   = random.choice(store_ids)
    product_id = random.choice(product_ids)
    quantity   = random.randint(1, 5)

    # Get unit_price from products table
    cur.execute("SELECT unit_price FROM products WHERE product_id = %s", (product_id,))
    price_row = cur.fetchone()
    unit_price = float(price_row[0]) if price_row else 10.0

    import uuid
    order_id = str(uuid.uuid4())
    cur.execute(
        "INSERT INTO orders (order_id, store_id, product_id, quantity, unit_price, status) VALUES (%s, %s, %s, %s, %s, 'pending')",
        (order_id, store_id, product_id, quantity, unit_price),
    )

    # Update inventory if record exists for this store+product
    cur.execute(
        """UPDATE inventory
           SET quantity_on_hand = GREATEST(0, quantity_on_hand - %s),
               last_updated = NOW()
           WHERE store_id = %s AND product_id = %s""",
        (quantity, store_id, product_id),
    )
    log.info(f"INSERT order {order_id[:8]}  store={store_id} product={product_id} qty={quantity} price={unit_price}")


def advance_order_status(cur) -> None:
    """Move a random pending/confirmed/shipped order to the next status."""
    cur.execute(
        "SELECT order_id, status FROM orders WHERE status IN ('pending','confirmed','shipped') ORDER BY RAND() LIMIT 1"
    )
    row = cur.fetchone()
    if not row:
        return
    order_id, current = row
    next_status = ORDER_STATUSES[ORDER_STATUSES.index(current) + 1]
    cur.execute("UPDATE orders SET status = %s WHERE order_id = %s", (next_status, order_id))
    log.info(f"UPDATE order #{order_id}  {current} → {next_status}")


def restock_inventory(cur) -> None:
    """Randomly restock a low-inventory item."""
    cur.execute(
        "SELECT inventory_id FROM inventory WHERE quantity_on_hand < reorder_threshold ORDER BY RAND() LIMIT 1"
    )
    row = cur.fetchone()
    if not row:
        # No low stock — restock a random item anyway
        cur.execute("SELECT inventory_id FROM inventory ORDER BY RAND() LIMIT 1")
        row = cur.fetchone()
    if not row:
        return
    inv_id  = row[0]
    restock = random.randint(10, 50)
    cur.execute(
        "UPDATE inventory SET quantity_on_hand = quantity_on_hand + %s, last_updated = NOW() WHERE inventory_id = %s",
        (restock, inv_id),
    )
    log.info(f"RESTOCK inventory #{inv_id}  +{restock} units")


def add_product(cur) -> None:
    """Occasionally add a new product."""
    name  = random.choice(PRODUCT_NAMES) + f" v{random.randint(2, 9)}"
    price = round(random.uniform(10.0, 120.0), 2)
    sku = f"GEN-{random.randint(10000,99999)}"
    cur.execute(
        "INSERT INTO products (sku, name, category, unit_price) VALUES (%s, %s, %s, %s)",
        (sku, name, random.choice(["Flower", "Vape", "Edible", "Tincture", "Topical"]), price),
    )
    log.info(f"INSERT product '{name}' sku={sku} @ ${price}")


def cancel_old_order(cur) -> None:
    """Cancel a random pending order."""
    cur.execute(
        "SELECT order_id FROM orders WHERE status = 'pending' ORDER BY RAND() LIMIT 1"
    )
    row = cur.fetchone()
    if not row:
        return
    cur.execute("UPDATE orders SET status = 'failed' WHERE order_id = %s", (row[0],))
    log.info(f"CANCEL order {str(row[0])[:8]}")


# ── Main loop ─────────────────────────────────────────────────────────────────

def main(interval: float) -> None:
    log.info(f"Starting data generator | interval={interval}s")
    conn = _connect()
    cur  = conn.cursor()

    tick = 0
    while RUNNING:
        try:
            if not conn.open:
                conn = _connect()
                cur  = conn.cursor()

            # Always place a new order
            place_order(cur)

            # Every 3 ticks: advance an existing order's status
            if tick % 3 == 0:
                advance_order_status(cur)

            # Every 5 ticks: restock inventory
            if tick % 5 == 0:
                restock_inventory(cur)

            # Every 20 ticks: add a new product
            if tick % 20 == 0:
                add_product(cur)

            # Every 15 ticks: cancel a pending order
            if tick % 15 == 0:
                cancel_old_order(cur)

            tick += 1

        except pymysql.Error as e:
            log.error(f"MySQL error: {e} — reconnecting.")
            try:
                conn.close()
            except Exception:
                pass
            time.sleep(3)
            conn = _connect()
            cur  = conn.cursor()
        except Exception as e:
            log.error(f"Unexpected error: {e}", exc_info=True)

        time.sleep(interval)

    cur.close()
    conn.close()
    log.info("Data generator stopped.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CDC demo data generator")
    parser.add_argument(
        "--interval", type=float, default=3.0,
        help="Seconds between each batch of writes (default: 3)"
    )
    args = parser.parse_args()
    main(args.interval)
