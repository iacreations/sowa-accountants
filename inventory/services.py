from decimal import Decimal
from django.db import transaction
from django.db.models import Sum
from django.utils import timezone

from .models import InventoryMovement, Product

def D(v) -> Decimal:
    return Decimal(str(v or "0"))

def is_inventory(p: Product) -> bool:
    return p and p.type == "Inventory"

@transaction.atomic
def rebuild_inventory_movements(source_type: str, source_id: int, *, date=None, rows=None):
    """
    rows = [{"product": Product, "qty_in": x, "qty_out": y, "unit_cost": cost}]
    - deletes old movements for this document
    - inserts new movements
    - recalculates Product.quantity and Product.avg_cost
    """
    date = date or timezone.localdate()
    rows = rows or []

    # 1) delete old (so edits don’t duplicate)
    InventoryMovement.objects.filter(source_type=source_type, source_id=source_id).delete()

    movements = []
    affected = set()

    for r in rows:
        p = r["product"]
        if not is_inventory(p):
            continue

        qty_in = D(r.get("qty_in"))
        qty_out = D(r.get("qty_out"))
        unit_cost = D(r.get("unit_cost"))

        if qty_in <= 0 and qty_out <= 0:
            continue

        # store positive value = qty * cost
        qty = qty_in if qty_in > 0 else qty_out
        value = qty * unit_cost

        movements.append(
            InventoryMovement(
                product=p,
                date=date,
                qty_in=qty_in,
                qty_out=qty_out,
                unit_cost=unit_cost,
                value=value,
                source_type=source_type,
                source_id=source_id,
            )
        )
        affected.add(p.id)

    InventoryMovement.objects.bulk_create(movements)

    # 2) update each affected product qty + avg_cost from movements
    for pid in affected:
        p = Product.objects.select_for_update().get(id=pid)

        agg = p.movements.aggregate(tin=Sum("qty_in"), tout=Sum("qty_out"))
        tin = agg["tin"] or Decimal("0")
        tout = agg["tout"] or Decimal("0")

        p.quantity = tin - tout

        # avg_cost from PURCHASES only
        purch = p.movements.filter(qty_in__gt=0).aggregate(q=Sum("qty_in"), v=Sum("value"))
        q = purch["q"] or Decimal("0")
        v = purch["v"] or Decimal("0")

        if q > 0:
            p.avg_cost = v / q

        p.save(update_fields=["quantity", "avg_cost"])


@transaction.atomic
def rebuild_movements_for_bill(bill):
    # delete old movements for edits (safe even if none exist)
    InventoryMovement.objects.filter(source_type="BILL", source_id=bill.id).delete()

    movements = []
    affected = set()

    for line in bill.item_lines.select_related("product"):
        p = line.product
        if not is_inventory(p):
            continue

        qty = D(line.qty)
        cost = D(line.rate)

        if qty <= 0 or cost < 0:
            continue

        movements.append(InventoryMovement(
            product=p,
            date=bill.bill_date or timezone.localdate(),
            qty_in=qty,
            qty_out=Decimal("0.00"),
            unit_cost=cost,
            value=(qty * cost),
            source_type="BILL",
            source_id=bill.id,
        ))
        affected.add(p.id)

    InventoryMovement.objects.bulk_create(movements)

    # update stock + avg_cost
    for pid in affected:
        p = Product.objects.select_for_update().get(id=pid)
        agg = p.movements.aggregate(tin=Sum("qty_in"), tout=Sum("qty_out"))
        tin = agg["tin"] or Decimal("0")
        tout = agg["tout"] or Decimal("0")
        p.quantity = tin - tout  # NOTE: change Product.quantity to DecimalField

        purch = p.movements.filter(qty_in__gt=0).aggregate(q=Sum("qty_in"), v=Sum("value"))
        q = purch["q"] or Decimal("0")
        v = purch["v"] or Decimal("0")
        if q > 0:
            p.avg_cost = v / q

        p.save(update_fields=["quantity", "avg_cost"])


@transaction.atomic
def rebuild_movements_for_sales_receipt(receipt):
    InventoryMovement.objects.filter(source_type="SALES_RECEIPT", source_id=receipt.id).delete()

    movements = []
    affected = set()

    for line in receipt.lines.select_related("product"):
        p = line.product
        if not is_inventory(p):
            continue

        qty = D(line.qty)
        if qty <= 0:
            continue

        unit_cost = D(getattr(p, "avg_cost", "0"))

        movements.append(InventoryMovement(
            product=p,
            date=receipt.receipt_date or timezone.localdate(),
            qty_in=Decimal("0.00"),
            qty_out=qty,
            unit_cost=unit_cost,
            value=(qty * unit_cost),
            source_type="SALES_RECEIPT",
            source_id=receipt.id,
        ))
        affected.add(p.id)

    InventoryMovement.objects.bulk_create(movements)

    # update stock only (avg_cost comes from purchases)
    for pid in affected:
        p = Product.objects.select_for_update().get(id=pid)
        agg = p.movements.aggregate(tin=Sum("qty_in"), tout=Sum("qty_out"))
        tin = agg["tin"] or Decimal("0")
        tout = agg["tout"] or Decimal("0")
        p.quantity = tin - tout
        p.save(update_fields=["quantity"])
