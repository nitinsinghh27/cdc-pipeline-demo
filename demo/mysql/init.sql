-- =============================================================================
-- MySQL init: runs on first container start
-- Creates Debezium replication user + demo schema + seed data
-- =============================================================================

-- ── Debezium replication user ────────────────────────────────────────────────
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED WITH mysql_native_password BY 'debezium_pass';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;

-- ── Schema ───────────────────────────────────────────────────────────────────
USE demo_db;

CREATE TABLE IF NOT EXISTS stores (
    store_id    INT          PRIMARY KEY AUTO_INCREMENT,
    name        VARCHAR(100) NOT NULL,
    city        VARCHAR(60),
    state       CHAR(2),
    active      TINYINT(1)   DEFAULT 1,
    created_at  TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    product_id   INT          PRIMARY KEY AUTO_INCREMENT,
    sku          VARCHAR(50)  UNIQUE NOT NULL,
    name         VARCHAR(100) NOT NULL,
    category     VARCHAR(50),
    unit_price   DECIMAL(8,2) NOT NULL,
    active       TINYINT(1)   DEFAULT 1
);

CREATE TABLE IF NOT EXISTS inventory (
    inventory_id      INT  PRIMARY KEY AUTO_INCREMENT,
    store_id          INT  NOT NULL,
    product_id        INT  NOT NULL,
    quantity_on_hand  INT  DEFAULT 0,
    reorder_threshold INT  DEFAULT 10,
    last_updated      TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (store_id)  REFERENCES stores(store_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    UNIQUE KEY uq_store_product (store_id, product_id)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id     CHAR(36)     PRIMARY KEY,
    store_id     INT          NOT NULL,
    product_id   INT          NOT NULL,
    quantity     INT          DEFAULT 1,
    unit_price   DECIMAL(8,2) NOT NULL,
    total_amount DECIMAL(10,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    status       ENUM('pending','processing','completed','failed') DEFAULT 'pending',
    created_at   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (store_id)  REFERENCES stores(store_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- ── Seed data ─────────────────────────────────────────────────────────────────
INSERT INTO stores (name, city, state) VALUES
    ('Green Valley Dispensary',  'Los Angeles',   'CA'),
    ('Pacific Wellness Co',      'San Francisco', 'CA'),
    ('Mountain High Shop',       'Denver',        'CO'),
    ('Sunrise Cannabis',         'Seattle',       'WA'),
    ('Desert Bloom',             'Phoenix',       'AZ');

INSERT INTO products (sku, name, category, unit_price) VALUES
    ('BD-3.5',  'Blue Dream 3.5g',          'Flower',     45.00),
    ('OK-7',    'OG Kush 7g',               'Flower',     80.00),
    ('SD-1',    'Sour Diesel 1g',           'Flower',     15.00),
    ('PH-14',   'Purple Haze 14g',          'Flower',    120.00),
    ('GSC-3.5', 'Girl Scout Cookies 3.5g',  'Flower',     50.00),
    ('CBD-30',  'CBD Tincture 1000mg',      'Tincture',   60.00),
    ('VPR-1',   'Vape Cartridge 1g',        'Vape',       45.00);

INSERT INTO inventory (store_id, product_id, quantity_on_hand, reorder_threshold) VALUES
    (1, 1, 50, 10), (1, 2, 30,  5), (1, 7, 25, 5),
    (2, 3, 100, 20), (2, 6, 40, 8),
    (3, 4, 15,  5), (3, 5, 75, 15),
    (4, 5, 60, 10), (4, 7, 30,  5),
    (5, 1, 20,  5), (5, 6, 10,  5);

INSERT INTO orders (order_id, store_id, product_id, quantity, unit_price, status) VALUES
    (UUID(), 1, 1, 2, 45.00, 'completed'),
    (UUID(), 1, 2, 1, 80.00, 'completed'),
    (UUID(), 2, 3, 3, 15.00, 'pending'),
    (UUID(), 3, 4, 1, 120.00,'processing'),
    (UUID(), 4, 5, 2, 50.00, 'completed'),
    (UUID(), 1, 1, 1, 45.00, 'failed'),
    (UUID(), 2, 6, 1, 60.00, 'completed'),
    (UUID(), 5, 7, 2, 45.00, 'pending');
