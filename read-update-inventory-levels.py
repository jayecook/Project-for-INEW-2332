from fastapi import APIRouter, HTTPException
from inventory_alerts.db import connect_db

router = APIRouter()

#Inventory Levels/Read

@router.get("/inventory-levels")
def read_inventory_levels():
    with connect_db() as conn:
        rows = conn.execute("""
            SELECT product_id, quantity, updated_at
            FROM inventory.inventory_levels
            ORDER BY product_id
        """).fetchall()

    return [
        {
            "product_id": r[0],
            "quantity": r[1],
            "updated_at": r[2],
        }
        for r in rows
    ]

@router.get("/inventory-levels/{product_id}")
def read_inventory_level(product_id: int):
    with connect_db() as conn:
        row = conn.execute("""
            SELECT product_id, quantity, updated_at
            FROM inventory.inventory_levels
            WHERE product_id = %s
        """, (product_id,)).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Inventory Level Not Found")

    return {
        "product_id": row[0],
        "quantity": row[1],
        "updated_at": row[2],
    }

#Update

@router.put("/inventory-levels/{product_id}")
def update_inventory_level(product_id: int, data: dict):
    if "quantity" not in data:
        raise HTTPException(status_code=400, detail="Missing 'Quantity' Field")

    quantity = data["quantity"]

    with connect_db() as conn:
        result = conn.execute("""
            UPDATE inventory.inventory_levels
            SET quantity = %s
            WHERE product_id = %s
            RETURNING product_id, quantity, updated_at
        """, (quantity, product_id)).fetchone()

    if not result:
        raise HTTPException(status_code=404, detail="Inventory Level Not Found")

    return {
        "product_id": result[0],
        "quantity": result[1],
        "updated_at": result[2],
    }