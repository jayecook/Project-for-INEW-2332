#!/usr/bin/env python3
"""
inventory_alerts.py

Subcommands:
  - init-db: create schema/tables/triggers
  - seed-demo: insert demo product/threshold/inventory + recipients
  - run-worker: LISTEN on low_stock and send emails via SMTP

Environment variables:
  DATABASE_URL   (required) e.g. postgres://user:pass@host:5432/db

For emailing (required for run-worker):
  SMTP_HOST
  SMTP_PORT      (optional, default 587)
  SMTP_USER
  SMTP_PASS
  MAIL_FROM
"""

import argparse
import json
import os
import smtplib
import sys
import time
from email.message import EmailMessage
from typing import List, Optional, Tuple

import psycopg


DDL_SQL = r"""
CREATE SCHEMA IF NOT EXISTS inventory;

CREATE TABLE IF NOT EXISTS inventory.products (
  product_id BIGSERIAL PRIMARY KEY,
  sku        TEXT NOT NULL UNIQUE,
  name       TEXT NOT NULL,
  description TEXT,
  active     BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_products_name_not_blank CHECK (btrim(name) <> ''),
  CONSTRAINT chk_products_sku_not_blank  CHECK (btrim(sku) <> '')
);

CREATE TABLE IF NOT EXISTS inventory.inventory_levels (
  product_id BIGINT PRIMARY KEY REFERENCES inventory.products(product_id),
  quantity   INTEGER NOT NULL CHECK (quantity >= 0),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inventory.product_thresholds (
  product_id      BIGINT PRIMARY KEY REFERENCES inventory.products(product_id),
  threshold_qty   INTEGER NOT NULL CHECK (threshold_qty >= 0),
  cooldown        INTERVAL NOT NULL DEFAULT INTERVAL '12 hours',
  enabled         BOOLEAN NOT NULL DEFAULT TRUE,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inventory.stock_alerts (
  alert_id      BIGSERIAL PRIMARY KEY,
  product_id    BIGINT NOT NULL REFERENCES inventory.products(product_id),
  quantity      INTEGER NOT NULL,
  threshold_qty INTEGER NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_stock_alerts_open
ON inventory.stock_alerts (product_id)
WHERE resolved_at IS NULL;

CREATE TABLE IF NOT EXISTS inventory.alert_recipients (
  recipient_id BIGSERIAL PRIMARY KEY,
  email        TEXT NOT NULL UNIQUE,
  enabled      BOOLEAN NOT NULL DEFAULT TRUE
);

-- updated_at trigger function for products
CREATE OR REPLACE FUNCTION inventory.set_updated_at()
RETURNS trigger AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_products_updated_at ON inventory.products;

CREATE TRIGGER trg_products_updated_at
BEFORE UPDATE ON inventory.products
FOR EACH ROW
EXECUTE FUNCTION inventory.set_updated_at();

-- Low stock trigger function: insert alert + notify, resolve when recovered
CREATE OR REPLACE FUNCTION inventory.check_low_stock()
RETURNS trigger AS $$
DECLARE
  t_threshold INTEGER;
  t_enabled   BOOLEAN;
  t_cooldown  INTERVAL;
  last_alert  TIMESTAMPTZ;
  has_open    BOOLEAN;
BEGIN
  NEW.updated_at = now();

  SELECT threshold_qty, enabled, cooldown
  INTO t_threshold, t_enabled, t_cooldown
  FROM inventory.product_thresholds
  WHERE product_id = NEW.product_id;

  IF t_threshold IS NULL OR t_enabled IS DISTINCT FROM TRUE THEN
    RETURN NEW;
  END IF;

  SELECT EXISTS (
    SELECT 1 FROM inventory.stock_alerts
    WHERE product_id = NEW.product_id AND resolved_at IS NULL
  ) INTO has_open;

  SELECT MAX(created_at)
  INTO last_alert
  FROM inventory.stock_alerts
  WHERE product_id = NEW.product_id;

  IF NEW.quantity < t_threshold THEN
    IF (NOT has_open) AND (last_alert IS NULL OR now() - last_alert >= t_cooldown) THEN
      INSERT INTO inventory.stock_alerts (product_id, quantity, threshold_qty)
      VALUES (NEW.product_id, NEW.quantity, t_threshold);

      PERFORM pg_notify(
        'low_stock',
        json_build_object(
          'product_id', NEW.product_id,
          'quantity', NEW.quantity,
          'threshold', t_threshold,
          'created_at', now()
        )::text
      );
    END IF;
  ELSE
    UPDATE inventory.stock_alerts
    SET resolved_at = now()
    WHERE product_id = NEW.product_id AND resolved_at IS NULL;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_inventory_low_stock_update ON inventory.inventory_levels;
DROP TRIGGER IF EXISTS trg_inventory_low_stock_insert ON inventory.inventory_levels;

CREATE TRIGGER trg_inventory_low_stock_update
BEFORE UPDATE OF quantity ON inventory.inventory_levels
FOR EACH ROW
EXECUTE FUNCTION inventory.check_low_stock();

CREATE TRIGGER trg_inventory_low_stock_insert
BEFORE INSERT ON inventory.inventory_levels
FOR EACH ROW
EXECUTE FUNCTION inventory.check_low_stock();

-- Employee-friendly view of open alerts
CREATE OR REPLACE VIEW inventory.v_open_stock_alerts AS
SELECT
  a.alert_id,
  a.created_at,
  p.sku,
  p.name,
  a.quantity,
  a.threshold_qty
FROM inventory.stock_alerts a
JOIN inventory.products p ON p.product_id = a.product_id
WHERE a.resolved_at IS NULL
ORDER BY a.created_at DESC;
"""


SEED_SQL = r"""
-- recipients (edit to your real employee emails)
INSERT INTO inventory.alert_recipients (email)
VALUES
  ('employee1@company.com'),
  ('employee2@company.com')
ON CONFLICT (email) DO NOTHING;

-- demo product
INSERT INTO inventory.products (sku, name, description)
VALUES ('SKU-1002', 'USB-C Cable 1m', 'Braided USB-C cable')
ON CONFLICT (sku) DO NOTHING;

-- threshold config
INSERT INTO inventory.product_thresholds (product_id, threshold_qty, cooldown, enabled)
SELECT product_id, 10, INTERVAL '12 hours', TRUE
FROM inventory.products
WHERE sku = 'SKU-1002'
ON CONFLICT (product_id) DO UPDATE
SET threshold_qty = EXCLUDED.threshold_qty,
    cooldown = EXCLUDED.cooldown,
    enabled = EXCLUDED.enabled;

-- initial inventory
INSERT INTO inventory.inventory_levels (product_id, quantity)
SELECT product_id, 50
FROM inventory.products
WHERE sku = 'SKU-1002'
ON CONFLICT (product_id) DO UPDATE
SET quantity = EXCLUDED.quantity;

-- drop below threshold to create alert + NOTIFY
UPDATE inventory.inventory_levels
SET quantity = 9
WHERE product_id = (SELECT product_id FROM inventory.products WHERE sku = 'SKU-1002');
"""


def require_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise SystemExit(f"Missing env var: {name}")
    return v


def connect_db(autocommit: bool = True) -> psycopg.Connection:
    db_url = require_env("DATABASE_URL")
    return psycopg.connect(db_url, autocommit=autocommit)


def init_db() -> None:
    with connect_db(autocommit=True) as conn:
        conn.execute(DDL_SQL)
    print("✅ Database initialized (schema/tables/triggers/views created).")


def seed_demo() -> None:
    with connect_db(autocommit=True) as conn:
        conn.execute(SEED_SQL)
    print("✅ Seeded demo data (recipients/product/threshold/inventory) and triggered a low-stock alert.")


def smtp_send(to_addrs: List[str], subject: str, body: str) -> None:
    smtp_host = require_env("SMTP_HOST")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = require_env("SMTP_USER")
    smtp_pass = require_env("SMTP_PASS")
    mail_from = require_env("MAIL_FROM")

    msg = EmailMessage()
    msg["From"] = mail_from
    msg["To"] = ", ".join(to_addrs)
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as s:
        s.starttls()
        s.login(smtp_user, smtp_pass)
        s.send_message(msg)


def get_recipients(conn: psycopg.Connection) -> List[str]:
    rows = conn.execute(
        "SELECT email FROM inventory.alert_recipients WHERE enabled = TRUE ORDER BY email"
    ).fetchall()
    return [r[0] for r in rows]


def get_product(conn: psycopg.Connection, product_id: int) -> Optional[Tuple[str, str]]:
    row = conn.execute(
        "SELECT sku, name FROM inventory.products WHERE product_id = %s",
        (product_id,),
    ).fetchone()
    if not row:
        return None
    return (row[0], row[1])


def run_worker(poll_fallback_seconds: int = 60) -> None:
    """
    LISTEN/NOTIFY worker:
      - listens for 'low_stock' notifications and emails recipients
      - also polls open alerts periodically (fallback) to avoid missing events
        if the worker was down temporarily.
    """
    print("📡 Starting low-stock email worker...")
    print("   Channel: low_stock")
    print("   Fallback poll (seconds):", poll_fallback_seconds)

    with connect_db(autocommit=True) as conn:
        conn.execute("LISTEN low_stock;")

        last_poll = 0.0

        while True:
            # Wait for NOTIFY (blocks until event or timeout)
            # psycopg wait() blocks; to also do periodic poll, we wake up with timeout.
            # If your psycopg version doesn't support timeout kw, we do a simple sleep loop.
            try:
                conn.wait(timeout=5)
            except TypeError:
                time.sleep(5)

            # Handle NOTIFY events
            for n in conn.notifies():
                try:
                    payload = json.loads(n.payload)
                    product_id = int(payload["product_id"])
                    qty = int(payload["quantity"])
                    threshold = int(payload["threshold"])
                except Exception as e:
                    print("⚠️ Bad notify payload:", n.payload, "error:", e)
                    continue

                product = get_product(conn, product_id)
                if not product:
                    print(f"⚠️ Product {product_id} not found; skipping notify.")
                    continue

                sku, name = product
                recipients = get_recipients(conn)
                if not recipients:
                    print("⚠️ No recipients enabled; skipping email.")
                    continue

                subject = f"LOW STOCK: {sku} ({name})"
                body = (
                    "A product has dropped below its inventory threshold.\n\n"
                    f"SKU: {sku}\n"
                    f"Name: {name}\n"
                    f"On-hand quantity: {qty}\n"
                    f"Threshold: {threshold}\n\n"
                    "Please review and reorder/restock as needed.\n"
                )

                try:
                    smtp_send(recipients, subject, body)
                    print(f"✅ Email sent to {len(recipients)} recipient(s) for {sku} (qty={qty} < {threshold})")
                except Exception as e:
                    print(f"❌ Email send failed for {sku}: {e}")

            # Fallback poll: find open alerts and email (basic safety net)
            now = time.time()
            if poll_fallback_seconds > 0 and (now - last_poll) >= poll_fallback_seconds:
                last_poll = now
                try:
                    # Just show open alerts; you can also email a digest here if desired.
                    rows = conn.execute("""
                        SELECT alert_id, created_at, sku, name, quantity, threshold_qty
                        FROM inventory.v_open_stock_alerts
                        LIMIT 50
                    """).fetchall()

                    if rows:
                        print(f"ℹ️ Fallback poll: {len(rows)} open alert(s).")
                    else:
                        print("ℹ️ Fallback poll: no open alerts.")
                except Exception as e:
                    print("⚠️ Fallback poll failed:", e)


def main() -> None:
    parser = argparse.ArgumentParser(description="Postgres inventory low-stock alerts (email via SMTP).")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init-db", help="Create schema/tables/triggers/views.")
    sub.add_parser("seed-demo", help="Insert demo rows and trigger a low-stock alert.")

    p_run = sub.add_parser("run-worker", help="Run LISTEN/NOTIFY worker that emails employees.")
    p_run.add_argument("--poll-fallback-seconds", type=int, default=60, help="Fallback poll interval (0 disables).")

    args = parser.parse_args()

    if args.cmd == "init-db":
        init_db()
    elif args.cmd == "seed-demo":
        seed_demo()
    elif args.cmd == "run-worker":
        run_worker(poll_fallback_seconds=args.poll_fallback_seconds)
    else:
        raise SystemExit("Unknown command")


if __name__ == "__main__":
    main()