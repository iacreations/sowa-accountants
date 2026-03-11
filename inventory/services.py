from decimal import Decimal
from typing import Iterable, Dict, Any, Optional, Set

from django.db import transaction
from django.db.models import Sum
from django.utils import timezone

from .models import InventoryMovement, InventoryLocation, Product

ZERO = Decimal("0.00")


# -----------------------
# Helpers
# -----------------------
def D(v) -> Decimal:
    """Safe Decimal conversion."""
    try:
        return Decimal(str(v if v is not None else "0"))
    except Exception:
        return Decimal("0")


def is_inventory(p: Optional[Product]) -> bool:
    """Inventory items only."""
    return bool(p and (p.type or "").strip().lower() == "inventory")


def safe_qty(v) -> Decimal:
    q = D(v)
    return q if q > ZERO else ZERO


def safe_cost(v) -> Decimal:
    c = D(v)
    return c if c >= ZERO else ZERO


def get_default_location() -> InventoryLocation:
    """
    Ensures there is always exactly one default active location.
    """
    loc = InventoryLocation.objects.filter(is_default=True, is_active=True).first()
    if not loc:
        loc = InventoryLocation.objects.create(name="Main Store", is_default=True, is_active=True)
        return loc

    # Safety: if multiple defaults exist, keep the first and clear the rest
    InventoryLocation.objects.filter(is_default=True).exclude(id=loc.id).update(is_default=False)
    return loc


def resolve_location_from_doc(doc) -> InventoryLocation:
    """
    Uses doc.location (TEXT FIELD) to create/select InventoryLocation.

    Client requirement: every input form has a location field.
    This works if:
      - your forms submit a 'location' string (e.g. "Main Store", "Branch A")
      - you want the system to auto-create locations as users type them

    If doc.location is empty -> default location is used.
    """
    name = (getattr(doc, "location", "") or "").strip()
    if name:
        loc, _ = InventoryLocation.objects.get_or_create(
            name=name,
            defaults={"is_default": False, "is_active": True},
        )
        # If it existed but was inactive, re-activate it (nice UX)
        if not loc.is_active:
            loc.is_active = True
            loc.save(update_fields=["is_active"])
        return loc
    return get_default_location()


def qty_on_hand(product: Product, location: Optional[InventoryLocation] = None) -> Decimal:
    """
    Ledger-based quantity:
      qty = total_in - total_out
    If location is passed, computes per-location stock.
    """
    qs = product.movements.all()
    if location:
        qs = qs.filter(location=location)

    agg = qs.aggregate(tin=Sum("qty_in"), tout=Sum("qty_out"))
    tin = agg["tin"] or ZERO
    tout = agg["tout"] or ZERO
    return tin - tout


# -----------------------
# Product cache recalculation
# -----------------------
def _recalc_product_qty_and_avg_cost(product_id: int):
    """
    Cached fields:
      Product.quantity = total_in - total_out (ALL locations)
      Product.avg_cost = total purchase value / total purchase qty (qty_in only)
    """
    p = Product.objects.select_for_update().get(id=product_id)

    agg = p.movements.aggregate(tin=Sum("qty_in"), tout=Sum("qty_out"))
    tin = agg["tin"] or ZERO
    tout = agg["tout"] or ZERO
    p.quantity = tin - tout

    purch = p.movements.filter(qty_in__gt=0).aggregate(q=Sum("qty_in"), v=Sum("value"))
    q = purch["q"] or ZERO
    v = purch["v"] or ZERO
    p.avg_cost = (v / q) if q > ZERO else ZERO

    p.save(update_fields=["quantity", "avg_cost"])


# -----------------------
# Generic ledger rebuilder (supports per-row location)
# -----------------------
@transaction.atomic
def rebuild_inventory_movements(
    source_type: str,
    source_id: int,
    *,
    date=None,
    rows: Optional[Iterable[Dict[str, Any]]] = None,
    location: Optional[InventoryLocation] = None,
):
    """
    rows = [
      {"product": Product, "qty_in": x, "qty_out": y, "unit_cost": cost, "location": InventoryLocation (optional)},
      ...
    ]

    Rules:
    - Deletes old movements for this (source_type, source_id) -> edit-safe
    - Inserts new movements
    - Recalculates Product.quantity and Product.avg_cost
    """
    date = date or timezone.localdate()
    rows = rows or []
    default_location = location or get_default_location()

    InventoryMovement.objects.filter(source_type=source_type, source_id=source_id).delete()

    movements = []
    affected: Set[int] = set()

    for r in rows:
        p = r.get("product")
        if not is_inventory(p):
            continue

        qty_in = safe_qty(r.get("qty_in"))
        qty_out = safe_qty(r.get("qty_out"))
        unit_cost = safe_cost(r.get("unit_cost"))

        if qty_in <= ZERO and qty_out <= ZERO:
            continue

        # allow per-row location override (needed for transfers)
        row_loc = r.get("location") or default_location

        qty = qty_in if qty_in > ZERO else qty_out
        value = qty * unit_cost

        movements.append(
            InventoryMovement(
                product=p,
                location=row_loc,
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

    for pid in affected:
        _recalc_product_qty_and_avg_cost(pid)


# -----------------------
# Bills (Stock IN)
# -----------------------
@transaction.atomic
def rebuild_movements_for_bill(bill):
    loc = resolve_location_from_doc(bill)

    rows = []
    for line in bill.item_lines.select_related("product").all():
        p = line.product
        if not is_inventory(p):
            continue

        qty = safe_qty(line.qty)
        cost = safe_cost(line.rate)
        if qty <= ZERO:
            continue

        rows.append({"product": p, "qty_in": qty, "qty_out": ZERO, "unit_cost": cost, "location": loc})

    rebuild_inventory_movements(
        "BILL",
        bill.id,
        date=bill.bill_date or timezone.localdate(),
        rows=rows,
    )


# -----------------------
# Expenses (Stock IN)
# -----------------------

@transaction.atomic
def rebuild_movements_for_expense(expense):
    """
    Stock IN for Inventory items on Expense.
    Uses ExpenseItemLine.qty and rate as unit_cost.
    Uses expense.location (FK InventoryLocation)
    """
    loc = getattr(expense, "location", None)  # may be null

    rows = []
    for line in expense.item_lines.select_related("product").all():
        p = line.product
        if not is_inventory(p):
            continue

        qty = safe_qty(line.qty)
        cost = safe_cost(line.rate)
        if qty <= ZERO:
            continue

        rows.append({
            "product": p,
            "qty_in": qty,
            "qty_out": ZERO,
            "unit_cost": cost,
            "location": loc,   # can be None; your engine will fallback to default if you coded it that way
        })

    rebuild_inventory_movements(
        "EXPENSE",
        expense.id,
        date=expense.payment_date or timezone.localdate(),
        rows=rows,
    )


# -----------------------
# Cheques (Stock IN)
# -----------------------
@transaction.atomic
def rebuild_movements_for_cheque(cheque):
    loc = resolve_location_from_doc(cheque)

    rows = []
    for line in cheque.item_lines.select_related("product").all():
        p = line.product
        if not is_inventory(p):
            continue

        qty = safe_qty(line.qty)
        cost = safe_cost(line.rate)
        if qty <= ZERO:
            continue

        rows.append({"product": p, "qty_in": qty, "qty_out": ZERO, "unit_cost": cost, "location": loc})

    rebuild_inventory_movements(
        "CHEQUE",
        cheque.id,
        date=cheque.payment_date or timezone.localdate(),
        rows=rows,
    )


# -----------------------
# Invoices (Stock OUT)
# -----------------------
@transaction.atomic
def rebuild_movements_for_invoice(invoice):
    loc = resolve_location_from_doc(invoice)
    inv_date = invoice.date_created.date() if invoice.date_created else timezone.localdate()

    rows = []
    for line in invoice.items.select_related("product").all():
        p = line.product
        if not is_inventory(p):
            continue

        qty = safe_qty(line.qty)
        if qty <= ZERO:
            continue

        unit_cost = safe_cost(getattr(p, "avg_cost", ZERO))
        rows.append({"product": p, "qty_in": ZERO, "qty_out": qty, "unit_cost": unit_cost, "location": loc})

    rebuild_inventory_movements(
        "INVOICE",
        invoice.id,
        date=inv_date,
        rows=rows,
    )


# -----------------------
# Sales Receipts (Stock OUT)
# -----------------------
@transaction.atomic
def rebuild_movements_for_sales_receipt(receipt):
    """
    Stock OUT for Inventory items on Sales Receipt.
    Uses receipt.lines.qty and CURRENT product.avg_cost as unit_cost (Average Cost).

    Expected:
      - receipt.lines related_name exists and each line has: product, qty
      - receipt has receipt_date OR we fall back to today
      - receipt has location (text) OR we fall back to default location
    """
    loc = resolve_location_from_doc(receipt)
    rcpt_date = getattr(receipt, "receipt_date", None) or timezone.localdate()

    # if receipt_date is datetime, convert to date
    if hasattr(rcpt_date, "date"):
        try:
            rcpt_date = rcpt_date.date()
        except Exception:
            pass

    rows = []
    for line in receipt.lines.select_related("product").all():
        p = getattr(line, "product", None)
        if not is_inventory(p):
            continue

        qty = safe_qty(getattr(line, "qty", None))
        if qty <= ZERO:
            continue

        unit_cost = safe_cost(getattr(p, "avg_cost", ZERO))
        rows.append({"product": p, "qty_in": ZERO, "qty_out": qty, "unit_cost": unit_cost, "location": loc})

    rebuild_inventory_movements(
        "SALES_RECEIPT",
        receipt.id,
        date=rcpt_date,
        rows=rows,
    )


# -----------------------
# Stock Transfer (OUT from A, IN to B) - DOES NOT TOUCH GL
# -----------------------
@transaction.atomic
def rebuild_movements_for_stock_transfer(transfer):
    """
    Creates 2 movements per line:
      - qty_out at from_location
      - qty_in  at to_location
    Unit cost uses product.avg_cost (transfer is not a purchase).
    """
    from_loc = transfer.from_location
    to_loc = transfer.to_location
    tdate = transfer.transfer_date or timezone.localdate()

    rows = []
    for ln in transfer.lines.select_related("product").all():
        p = ln.product
        if not is_inventory(p):
            continue

        qty = safe_qty(ln.qty)
        if qty <= ZERO:
            continue

        # Optional: prevent negative stock at FROM location
        available = qty_on_hand(p, location=from_loc)
        if qty > available:
            raise ValueError(
                f"Not enough stock for {p.name} at {from_loc.name}. "
                f"Available {available}, trying to transfer {qty}."
            )

        unit_cost = safe_cost(getattr(p, "avg_cost", ZERO))

        # OUT movement at FROM
        rows.append({
            "product": p,
            "qty_in": ZERO,
            "qty_out": qty,
            "unit_cost": unit_cost,
            "location": from_loc,
        })

        # IN movement at TO
        rows.append({
            "product": p,
            "qty_in": qty,
            "qty_out": ZERO,
            "unit_cost": unit_cost,
            "location": to_loc,
        })

    rebuild_inventory_movements(
        "TRANSFER",
        transfer.id,
        date=tdate,
        rows=rows,
    )

from .models import MainStore, InventoryLocation

def get_main_store() -> MainStore:
    store = MainStore.objects.filter(is_active=True).first()
    if not store:
        store = MainStore.objects.create(name="Main Store", is_active=True)
    return store

def get_default_location() -> InventoryLocation:
    store = get_main_store()
    loc = InventoryLocation.objects.filter(store=store, is_default=True, is_active=True).first()
    if not loc:
        loc = InventoryLocation.objects.create(store=store, name="Main Store", is_default=True, is_active=True)
    InventoryLocation.objects.filter(store=store).exclude(id=loc.id).update(is_default=False)
    return loc
