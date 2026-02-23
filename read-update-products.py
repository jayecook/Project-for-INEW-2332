from fastapi import APIRouter, HTTPException
from inventory_alerts.db import connect_db

router = APIRouter()

#Products/Read

@router.get("/products")
def read_products():
    with connect_db() as conn:
        rows = conn.execute("""
            SELECT product_id, sku, name, description, active, created_at, updated_at
            FROM inventory.products
            ORDER BY product_id
        """).fetchall()
    
    return [
        {
            "product_id": r[0],
            "sku": r[1],
            "name": r[2],
            "description": r[3],
            "active": r[4],
            "created_at": r[5],
            "updated_at": r[6],
        }
        for r in rows
    ]

@router.get("/products/{product_id}")
def read_product(product_id: int):
    with connect_db() as conn:
        row = conn.execute("""
            SELECT product_id, sku, name, description, active, created_at, updated_at
            FROM inventory.products
            WHERE product_id = %s
        """, (product_id,)).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Product Not Found")

    return {
        "product_id": row[0],
        "sku": row[1],
        "name": row[2],
        "description": row[3],
        "active": row[4],
        "created_at": row[5],
        "updated_at": row[6],
    }

#Products/Update

@router.put("/products/{product_id}")
def update_product(product_id: int, data: dict):
    allowed_fields = {"sku", "name", "description", "active"}
    updates = []
    values = []

    for key, value in data.items():
        if key in allowed_fields:
            updates.append(f"{key} = %s")
            values.append(value)

    if not updates:
        raise HTTPException(status_code=400, detail="No Fields To Update")

    values.append(product_id)

    with connect_db() as conn:
        result = conn.execute(f"""
            UPDATE inventory.products
            SET {", ".join(updates)}
            WHERE product_id = %s
            RETURNING product_id, sku, name, description, active, created_at, updated_at
        """, tuple(values)).fetchone()

    if not result:
        raise HTTPException(status_code=404, detail="Product Not Found")

    return {
        "product_id": result[0],
        "sku": result[1],
        "name": result[2],
        "description": result[3],
        "active": result[4],
        "created_at": result[5],
        "updated_at": result[6],
    }      