from decimal import Decimal
from django.db import transaction
from django.db.models import Sum
from django.utils import timezone

from .models import InventoryMovement, Product


# -----------------------
# Helpers
# -----------------------
def D(v) -> Decimal:
    return Decimal(str(v or "0"))


def is_inventory(p: Product) -> bool:
    return bool(p and p.type == "Inventory")


def _recalc_product_qty_and_avg_cost(product_id: int):
    """
    Recalculate cached Product.quantity and Product.avg_cost based on movements ledger.

    quantity = total_in - total_out
    avg_cost = total purchase value / total purchase qty  (purchases only, qty_in > 0)
    """
    p = Product.objects.select_for_update().get(id=product_id)

    agg = p.movements.aggregate(tin=Sum("qty_in"), tout=Sum("qty_out"))
    tin = agg["tin"] or Decimal("0")
    tout = agg["tout"] or Decimal("0")

    # cached qty
    p.quantity = tin - tout

    # avg_cost from PURCHASES only
    purch = p.movements.filter(qty_in__gt=0).aggregate(q=Sum("qty_in"), v=Sum("value"))
    q = purch["q"] or Decimal("0")
    v = purch["v"] or Decimal("0")

    if q > 0:
        p.avg_cost = v / q
    else:
        # no purchases => avg cost resets
        p.avg_cost = Decimal("0")

    p.save(update_fields=["quantity", "avg_cost"])


# -----------------------
# Generic rebuilder
# -----------------------
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

    # 1) delete old movements (so edits don’t duplicate)
    InventoryMovement.objects.filter(source_type=source_type, source_id=source_id).delete()

    movements = []
    affected = set()

    for r in rows:
        p = r.get("product")
        if not is_inventory(p):
            continue

        qty_in = D(r.get("qty_in"))
        qty_out = D(r.get("qty_out"))
        unit_cost = D(r.get("unit_cost"))

        if qty_in <= 0 and qty_out <= 0:
            continue

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

    if movements:
        InventoryMovement.objects.bulk_create(movements)

    # 2) update each affected product qty + avg_cost from movements
    for pid in affected:
        _recalc_product_qty_and_avg_cost(pid)

# -----------------------
# Expennses

@transaction.atomic
def rebuild_movements_for_expense(expense):
    """
    EXPENSE:
      - Stock IN for Inventory products (from expense.item_lines)
      - Updates Product.quantity and Product.avg_cost from ledger (same method as bills)
    """
    InventoryMovement.objects.filter(source_type="EXPENSE", source_id=expense.id).delete()

    movements = []
    affected = set()

    for line in expense.item_lines.select_related("product"):
        p = line.product
        if not is_inventory(p):
            continue

        qty = D(line.qty)
        cost = D(line.rate)

        if qty <= 0 or cost < 0:
            continue

        movements.append(InventoryMovement(
            product=p,
            date=expense.payment_date or timezone.localdate(),
            qty_in=qty,
            qty_out=Decimal("0.00"),
            unit_cost=cost,
            value=(qty * cost),
            source_type="EXPENSE",
            source_id=expense.id,
        ))
        affected.add(p.id)

    InventoryMovement.objects.bulk_create(movements)

    for pid in affected:
        p = Product.objects.select_for_update().get(id=pid)

        agg = p.movements.aggregate(tin=Sum("qty_in"), tout=Sum("qty_out"))
        tin = agg["tin"] or Decimal("0")
        tout = agg["tout"] or Decimal("0")
        p.quantity = tin - tout

        purch = p.movements.filter(qty_in__gt=0).aggregate(q=Sum("qty_in"), v=Sum("value"))
        q = purch["q"] or Decimal("0")
        v = purch["v"] or Decimal("0")
        if q > 0:
            p.avg_cost = v / q

        p.save(update_fields=["quantity", "avg_cost"])
# -----------------------
# -----------------------
# Bills
# -----------------------
@transaction.atomic
def rebuild_movements_for_bill(bill):
    """
    Called by your Bill views:
        rebuild_movements_for_bill(bill)

    Stock IN for Inventory items using BillItemLine.qty and BillItemLine.rate.
    """
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

        movements.append(
            InventoryMovement(
                product=p,
                date=bill.bill_date or timezone.localdate(),
                qty_in=qty,
                qty_out=Decimal("0.00"),
                unit_cost=cost,
                value=(qty * cost),
                source_type="BILL",
                source_id=bill.id,
            )
        )
        affected.add(p.id)

    if movements:
        InventoryMovement.objects.bulk_create(movements)

    for pid in affected:
        _recalc_product_qty_and_avg_cost(pid)


# -----------------------
# Expenses (Stock IN if buying inventory via ExpenseItemLine)
# -----------------------
@transaction.atomic
def rebuild_movements_for_expense(expense):
    """
    Stock IN for Inventory items on Expense.
    Uses ExpenseItemLine.qty and rate as unit_cost.

    Call this after saving expense + item lines.
    """
    InventoryMovement.objects.filter(source_type="EXPENSE", source_id=expense.id).delete()

    movements = []
    affected = set()

    for line in expense.item_lines.select_related("product"):
        p = line.product
        if not is_inventory(p):
            continue

        qty = D(line.qty)
        cost = D(line.rate)

        if qty <= 0 or cost < 0:
            continue

        movements.append(
            InventoryMovement(
                product=p,
                date=expense.payment_date or timezone.localdate(),
                qty_in=qty,
                qty_out=Decimal("0.00"),
                unit_cost=cost,
                value=(qty * cost),
                source_type="EXPENSE",
                source_id=expense.id,
            )
        )
        affected.add(p.id)

    if movements:
        InventoryMovement.objects.bulk_create(movements)

    for pid in affected:
        _recalc_product_qty_and_avg_cost(pid)


# -----------------------
# Cheques (Stock IN if buying inventory via ChequeItemLine)
# -----------------------
@transaction.atomic
def rebuild_movements_for_cheque(cheque):
    """
    Stock IN for Inventory items on Cheque.
    Uses ChequeItemLine.qty and rate.

    Call this after saving cheque + item lines.
    """
    InventoryMovement.objects.filter(source_type="CHEQUE", source_id=cheque.id).delete()

    movements = []
    affected = set()

    for line in cheque.item_lines.select_related("product"):
        p = line.product
        if not is_inventory(p):
            continue

        qty = D(line.qty)
        cost = D(line.rate)

        if qty <= 0 or cost < 0:
            continue

        movements.append(
            InventoryMovement(
                product=p,
                date=cheque.payment_date or timezone.localdate(),
                qty_in=qty,
                qty_out=Decimal("0.00"),
                unit_cost=cost,
                value=(qty * cost),
                source_type="CHEQUE",
                source_id=cheque.id,
            )
        )
        affected.add(p.id)

    if movements:
        InventoryMovement.objects.bulk_create(movements)

    for pid in affected:
        _recalc_product_qty_and_avg_cost(pid)


# -----------------------
# Invoices (Stock OUT)
# -----------------------
@transaction.atomic
def rebuild_movements_for_invoice(invoice):
    """
    Stock OUT for Inventory items on Invoice.
    Uses InvoiceItem.qty and CURRENT product.avg_cost as unit_cost.

    Call this after saving invoice + invoice items.
    """
    InventoryMovement.objects.filter(source_type="INVOICE", source_id=invoice.id).delete()

    movements = []
    affected = set()

    # local import to avoid circular imports
    from sales.models import InvoiceItem

    inv_date = invoice.date_created.date() if invoice.date_created else timezone.localdate()

    for line in InvoiceItem.objects.filter(invoice=invoice).select_related("product"):
        p = line.product
        if not is_inventory(p):
            continue

        qty = D(line.qty)
        if qty <= 0:
            continue

        # use current avg_cost as cost basis
        unit_cost = D(getattr(p, "avg_cost", "0"))
        value = qty * unit_cost

        movements.append(
            InventoryMovement(
                product=p,
                date=inv_date,
                qty_in=Decimal("0.00"),
                qty_out=qty,
                unit_cost=unit_cost,
                value=value,
                source_type="INVOICE",
                source_id=invoice.id,
            )
        )
        affected.add(p.id)

    if movements:
        InventoryMovement.objects.bulk_create(movements)

    # update stock only (avg_cost comes from purchases, but we recalc both safely)
    for pid in affected:
        _recalc_product_qty_and_avg_cost(pid)


# -----------------------
# Sales Receipts (Stock OUT)
# -----------------------
@transaction.atomic
def rebuild_movements_for_sales_receipt(receipt):
    """
    Stock OUT for Inventory items on SalesReceipt.
    Uses SalesReceiptLine.qty and CURRENT product.avg_cost as unit_cost.
    """
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
        value = qty * unit_cost

        movements.append(
            InventoryMovement(
                product=p,
                date=receipt.receipt_date or timezone.localdate(),
                qty_in=Decimal("0.00"),
                qty_out=qty,
                unit_cost=unit_cost,
                value=value,
                source_type="SALES_RECEIPT",
                source_id=receipt.id,
            )
        )
        affected.add(p.id)

    if movements:
        InventoryMovement.objects.bulk_create(movements)

    for pid in affected:
        _recalc_product_qty_and_avg_cost(pid)
  