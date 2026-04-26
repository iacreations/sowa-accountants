from decimal import Decimal, ROUND_HALF_UP
from typing import Iterable, Dict, Any, Optional, Set

from django.db import transaction
from django.db.models import Sum
from django.utils import timezone

from .models import InventoryMovement, InventoryLocation, Product, MainStore
from .fifo import record_purchase_layer, simulate_fifo_consumption, rebuild_layers_from_movements

ZERO = Decimal("0.00")

# Source types that represent actual purchases and should contribute to avg_cost
PURCHASE_SOURCE_TYPES = ["BILL", "EXPENSE", "CHEQUE", "OPENING", "ASSEMBLY"]


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


def get_main_store(company=None) -> MainStore:
    """
    Always return one active main store for the active company.
    """
    qs = MainStore.objects.filter(is_active=True)
    if company is not None and hasattr(MainStore, "company_id"):
        qs = qs.filter(company=company)

    store = qs.first()
    if not store:
        create_kwargs = {"name": "Main Store", "is_active": True}
        if company is not None and hasattr(MainStore, "company_id"):
            create_kwargs["company"] = company
        store = MainStore.objects.create(**create_kwargs)
    return store


def get_default_location(company=None) -> InventoryLocation:
    """
    Ensures there is always one default active location.
    Supports tenant-aware InventoryLocation if company exists on the model.
    """
    store = get_main_store(company)

    qs = InventoryLocation.objects.filter(store=store, is_active=True)
    if company is not None and hasattr(InventoryLocation, "company_id"):
        qs = qs.filter(company=company)

    loc = qs.filter(is_default=True).first()

    if not loc:
        create_kwargs = {
            "store": store,
            "name": "Main Store",
            "is_default": True,
            "is_active": True,
        }
        if company is not None and hasattr(InventoryLocation, "company_id"):
            create_kwargs["company"] = company

        loc = InventoryLocation.objects.create(**create_kwargs)

    # Safety: keep only one default
    cleanup_qs = InventoryLocation.objects.filter(store=store, is_default=True)
    if company is not None and hasattr(InventoryLocation, "company_id"):
        cleanup_qs = cleanup_qs.filter(company=company)

    cleanup_qs.exclude(id=loc.id).update(is_default=False)

    return loc


def resolve_location_from_doc(doc) -> InventoryLocation:
    """
    Resolve location from document.

    NEW LOGIC:
    - If doc.location is already an InventoryLocation FK, use it
    - If doc.location is empty, fall back to default location
    - If doc.location is a string somehow, try to find/create a location by that name
    """
    company = getattr(doc, "company", None)
    loc = getattr(doc, "location", None)

    # FK already set
    if isinstance(loc, InventoryLocation):
        return loc

    # If location is just an ID somehow
    if loc and hasattr(loc, "id"):
        return loc

    # If old code still passes text
    if isinstance(loc, str):
        name = loc.strip()
        if name:
            store = get_main_store(company)
            qs = InventoryLocation.objects.filter(store=store, name__iexact=name)
            if company is not None and hasattr(InventoryLocation, "company_id"):
                qs = qs.filter(company=company)

            found = qs.first()
            if found:
                if not found.is_active:
                    found.is_active = True
                    found.save(update_fields=["is_active"])
                return found

            create_kwargs = {
                "store": store,
                "name": name,
                "is_default": False,
                "is_active": True,
            }
            if company is not None and hasattr(InventoryLocation, "company_id"):
                create_kwargs["company"] = company

            return InventoryLocation.objects.create(**create_kwargs)

    return get_default_location(company=company)


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
def _recalc_product_quantity(product_id: int):
    """Update Product.quantity from the movement ledger without touching avg_cost."""
    p = Product.objects.select_for_update().get(id=product_id)
    agg = p.movements.aggregate(tin=Sum("qty_in"), tout=Sum("qty_out"))
    p.quantity = (agg["tin"] or ZERO) - (agg["tout"] or ZERO)
    p.save(update_fields=["quantity"])


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

    purch = p.movements.filter(
        qty_in__gt=0,
        source_type__in=PURCHASE_SOURCE_TYPES,
    ).aggregate(q=Sum("qty_in"), v=Sum("value"))
    q = purch["q"] or ZERO
    v = purch["v"] or ZERO
    p.avg_cost = ((v / q).quantize(Decimal("1"), rounding=ROUND_HALF_UP)) if q > ZERO else ZERO

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
    company=None,
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
    default_location = location or get_default_location(company=company)

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

        row_loc = r.get("location") or default_location
        qty = qty_in if qty_in > ZERO else qty_out
        value = qty * unit_cost

        movement_kwargs = {
            "product": p,
            "location": row_loc,
            "date": date,
            "qty_in": qty_in,
            "qty_out": qty_out,
            "unit_cost": unit_cost,
            "value": value,
            "source_type": source_type,
            "source_id": source_id,
        }
        if company is not None and hasattr(InventoryMovement, "company_id"):
            movement_kwargs["company"] = company

        movements.append(InventoryMovement(**movement_kwargs))
        affected.add(p.id)

    if movements:
        InventoryMovement.objects.bulk_create(movements)

    # Recalculate product quantity cache
    if source_type in PURCHASE_SOURCE_TYPES:
        for pid in affected:
            _recalc_product_qty_and_avg_cost(pid)
    else:
        for pid in affected:
            _recalc_product_quantity(pid)

    # Rebuild FIFO layers for all affected products from the full movement ledger.
    # This is always correct regardless of whether this is a purchase or sale.
    for pid in affected:
        try:
            p = Product.objects.get(id=pid)
            rebuild_layers_from_movements(p, company=company)
        except Product.DoesNotExist:
            pass


# -----------------------
# Bills (Stock IN)
# -----------------------
@transaction.atomic
def rebuild_movements_for_bill(bill):
    company = getattr(bill, "company", None)
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

        rows.append({
            "product": p,
            "qty_in": qty,
            "qty_out": ZERO,
            "unit_cost": cost,
            "location": loc,
        })

    rebuild_inventory_movements(
        "BILL",
        bill.id,
        date=bill.bill_date or timezone.localdate(),
        rows=rows,
        company=company,
    )


# -----------------------
# Expenses (Stock IN)
# -----------------------
@transaction.atomic
def rebuild_movements_for_expense(expense):
    """
    Stock IN for Inventory items on Expense.
    Uses ExpenseItemLine.qty and rate as unit_cost.
    Uses expense.location FK if available.
    """
    company = getattr(expense, "company", None)
    loc = getattr(expense, "location", None) or get_default_location(company=company)

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
            "location": loc,
        })

    rebuild_inventory_movements(
        "EXPENSE",
        expense.id,
        date=expense.payment_date or timezone.localdate(),
        rows=rows,
        company=company,
    )


# -----------------------
# Cheques (Stock IN)
# -----------------------
@transaction.atomic
def rebuild_movements_for_cheque(cheque):
    company = getattr(cheque, "company", None)
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

        rows.append({
            "product": p,
            "qty_in": qty,
            "qty_out": ZERO,
            "unit_cost": cost,
            "location": loc,
        })

    rebuild_inventory_movements(
        "CHEQUE",
        cheque.id,
        date=cheque.payment_date or timezone.localdate(),
        rows=rows,
        company=company,
    )


# -----------------------
# Invoices (Stock OUT)
# -----------------------
@transaction.atomic
def rebuild_movements_for_invoice(invoice):
    company = getattr(invoice, "company", None)
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

        fifo_rows = simulate_fifo_consumption(p, qty)
        for fifo_cost, fifo_qty in fifo_rows:
            rows.append({
                "product": p,
                "qty_in": ZERO,
                "qty_out": fifo_qty,
                "unit_cost": fifo_cost,
                "location": loc,
            })

    rebuild_inventory_movements(
        "INVOICE",
        invoice.id,
        date=inv_date,
        rows=rows,
        company=company,
    )


# -----------------------
# Sales Receipts (Stock OUT)
# -----------------------
@transaction.atomic
def rebuild_movements_for_sales_receipt(receipt):
    """
    Stock OUT for Inventory items on Sales Receipt.
    Uses receipt.lines.qty and CURRENT product.avg_cost as unit_cost.
    """
    company = getattr(receipt, "company", None)
    loc = resolve_location_from_doc(receipt)
    rcpt_date = getattr(receipt, "receipt_date", None) or timezone.localdate()

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

        fifo_rows = simulate_fifo_consumption(p, qty)
        for fifo_cost, fifo_qty in fifo_rows:
            rows.append({
                "product": p,
                "qty_in": ZERO,
                "qty_out": fifo_qty,
                "unit_cost": fifo_cost,
                "location": loc,
            })

    rebuild_inventory_movements(
        "SALES_RECEIPT",
        receipt.id,
        date=rcpt_date,
        rows=rows,
        company=company,
    )


# -----------------------
# Stock Transfer (OUT from A, IN to B)
# -----------------------
@transaction.atomic
def rebuild_movements_for_stock_transfer(transfer):
    """
    Creates 2 movements per line:
      - qty_out at from_location
      - qty_in  at to_location
    """
    company = getattr(transfer, "company", None)
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

        available = qty_on_hand(p, location=from_loc)
        if qty > available:
            raise ValueError(
                f"Not enough stock for {p.name} at {from_loc.name}. "
                f"Available {available}, trying to transfer {qty}."
            )

        # Use the oldest available FIFO layer cost for the transfer record
        from .fifo import get_available_layers
        layers = get_available_layers(p)
        unit_cost = safe_cost(layers[0].unit_cost if layers else ZERO)

        rows.append({
            "product": p,
            "qty_in": ZERO,
            "qty_out": qty,
            "unit_cost": unit_cost,
            "location": from_loc,
        })
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
        company=company,
    )