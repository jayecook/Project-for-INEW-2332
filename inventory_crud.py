import os
import psycopg2
import psycopg2.extras
import sys
from typing import Optional, Dict, List, Any

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db_connection():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable not set!")
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)

def add_product(
    sku: str, 
    name: str, 
    description: str = "", 
    initial_quantity: int = 0,
    threshold_qty: int = 10,
    cooldown_hours: int = 12,
    enabled: bool = True
) -> Dict[str, Any]:

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:

            cur.execute("""
                INSERT INTO inventory.products (sku, name, description, active)
                VALUES (%s, %s, %s, TRUE)
                RETURNING product_id, sku, name, description, created_at
            """, (sku, name, description))
            
            product = cur.fetchone()
            product_id = product['product_id']
            
            cur.execute("""
                INSERT INTO inventory.inventory_levels (product_id, quantity)
                VALUES (%s, %s)
                RETURNING quantity, updated_at
            """, (product_id, initial_quantity))
            
            inventory = cur.fetchone()
            
            cur.execute("""
                INSERT INTO inventory.product_thresholds 
                    (product_id, threshold_qty, cooldown, enabled)
                VALUES (%s, %s, %s * INTERVAL '1 hour', %s)
                RETURNING threshold_qty, cooldown, enabled
            """, (product_id, threshold_qty, cooldown_hours, enabled))
            
            threshold = cur.fetchone()
            
            conn.commit()
            
            result = {
                **product,
                'initial_quantity': inventory['quantity'],
                'inventory_updated_at': inventory['updated_at'],
                'threshold': threshold
            }
            
            print(f" ADDED: Product {sku} (ID: {product_id}) with quantity {initial_quantity}")
            return result
            
    except Exception as e:
        conn.rollback()
        print(f" Error adding product: {e}")
        raise
    finally:
        conn.close()


def delete_product(product_id: int, force: bool = False) -> Dict[str, Any]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT p.product_id, p.sku, p.name
                FROM inventory.products p
                WHERE p.product_id = %s
            """, (product_id,))
            
            product_info = cur.fetchone()
            if not product_info:
                raise ValueError(f"Product {product_id} not found")
            
            if not force:
                print(f"\n  WARNING: You are about to DELETE:")
                print(f"   Product: {product_info['sku']} - {product_info['name']}")
                print("   This will also delete inventory levels, thresholds, and alerts!")
                print("   Use force=True to confirm deletion.\n")
                return {"warning": "Deletion requires force=True", "product": product_info}
            
            cur.execute("""
                DELETE FROM inventory.stock_alerts
                WHERE product_id = %s
            """, (product_id,))
            
            cur.execute("""
                DELETE FROM inventory.inventory_levels
                WHERE product_id = %s
            """, (product_id,))
            
            cur.execute("""
                DELETE FROM inventory.product_thresholds
                WHERE product_id = %s
            """, (product_id,))
            
            cur.execute("""
                DELETE FROM inventory.products
                WHERE product_id = %s
                RETURNING product_id, sku, name, created_at
            """, (product_id,))
            
            deleted = cur.fetchone()
            conn.commit()
            
            print(f" DELETED: Product {deleted['sku']} (ID: {product_id})")
            return deleted
            
    except Exception as e:
        conn.rollback()
        print(f" Error deleting product: {e}")
        raise
    finally:
        conn.close()

def get_all_products() -> List[Dict[str, Any]]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    p.product_id, p.sku, p.name, p.description, p.active,
                    il.quantity as current_quantity,
                    pt.threshold_qty, pt.enabled as threshold_enabled,
                    CASE 
                        WHEN il.quantity < pt.threshold_qty AND pt.enabled THEN 'LOW STOCK'
                        ELSE 'OK'
                    END as stock_status
                FROM inventory.products p
                LEFT JOIN inventory.inventory_levels il ON p.product_id = il.product_id
                LEFT JOIN inventory.product_thresholds pt ON p.product_id = pt.product_id
                ORDER BY p.product_id DESC
            """)
            return cur.fetchall()
    finally:
        conn.close()


def get_product(product_id: int) -> Optional[Dict[str, Any]]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    p.*,
                    il.quantity as current_quantity,
                    pt.threshold_qty, pt.cooldown, pt.enabled as threshold_enabled
                FROM inventory.products p
                LEFT JOIN inventory.inventory_levels il ON p.product_id = il.product_id
                LEFT JOIN inventory.product_thresholds pt ON p.product_id = pt.product_id
                WHERE p.product_id = %s
            """, (product_id,))
            return cur.fetchone()
    finally:
        conn.close()


def get_open_alerts() -> List[Dict[str, Any]]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM inventory.v_open_stock_alerts")
            return cur.fetchall()
    finally:
        conn.close()


def test_my_crud():
    print("\n" + "="*60)
    print("TESTING MY CRUD OPERATIONS")
    print("="*60)
    
    if not DATABASE_URL:
        print("Please set DATABASE_URL environment variable first!")
        print("   Example: export DATABASE_URL='postgres://user:pass@localhost:5432/dbname'")
        return
    
    try:
        print("\n  TESTING ADD OPERATION")
        print("-" * 40)
        new_product = add_product(
            sku=f"TEST-{os.urandom(2).hex()}",
            name="Test Product from CRUD",
            description="Created by my CRUD operations",
            initial_quantity=25,
            threshold_qty=5
        )
        print(f"   ✓ Product added with ID: {new_product['product_id']}")
        
        print("\n  CURRENT INVENTORY (last 5 items)")
        print("-" * 40)
        products = get_all_products()
        for p in products[-5:]:
            status = " LOW" if p['stock_status'] == 'LOW STOCK' else " OK"
            print(f"   {status} {p['sku']}: {p['name']} - Qty: {p['current_quantity']}")
        
        print("\n3️⃣  TESTING DELETE OPERATION")
        print("-" * 40)
        print("   First attempt (without force - should warn):")
        delete_product(new_product['product_id'])
        
        print("\n   Second attempt (with force=True):")
        deleted = delete_product(new_product['product_id'], force=True)
        if deleted:
            print(f"   Product deleted: {deleted['sku']}")
        
        print("\n4️⃣  VERIFYING DELETION")
        print("-" * 40)
        check = get_product(new_product['product_id'])
        if not check:
            print("   Product successfully removed from database")
        
        print("\n" + "="*60)
        print(" TEST COMPLETE - Both ADD and DELETE operations working!")
        print("="*60 + "\n")
        
    except Exception as e:
        print(f"\n Test failed: {e}")


if __name__ == "__main__":
    print("\nInventory CRUD Operations Module")
    print("================================")
    print("\nAvailable functions:")
    print("  - add_product(sku, name, description, initial_quantity, threshold_qty, cooldown_hours, enabled)")
    print("  - delete_product(product_id, force=False)")
    print("  - get_all_products()")
    print("  - get_product(product_id)")
    print("  - get_open_alerts()")
    print("\nTo test: python inventory_crud.py")
    print("Or in Python: from inventory_crud import *; test_my_crud()")
    
    if __name__ == "__main__":
        test_my_crud()