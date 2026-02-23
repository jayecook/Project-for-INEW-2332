import json
import time
from typing import List, Optional, Tuple

from .db import connect_db
from .emailer import smtp_send


def get_recipients(conn) -> List[str]:
    rows = conn.execute(
        "SELECT email FROM inventory.alert_recipients WHERE enabled = TRUE ORDER BY email"
    ).fetchall()
    return [r[0] for r in rows]


def get_product(conn, product_id: int) -> Optional[Tuple[str, str]]:
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
      - periodic polling as a fallback observability mechanism
    """
    print("📡 Starting low-stock email worker...")
    print("   Channel: low_stock")
    print("   Fallback poll (seconds):", poll_fallback_seconds)

    with connect_db(autocommit=True) as conn:
        conn.execute("LISTEN low_stock;")

        last_poll = 0.0

        while True:
            # wake periodically to allow fallback polling
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

            # fallback polling: just prints open alerts (you can expand to digest emails)
            now = time.time()
            if poll_fallback_seconds > 0 and (now - last_poll) >= poll_fallback_seconds:
                last_poll = now
                try:
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
