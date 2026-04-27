# inventory/validators.py
"""
Inventory safeguards and validators.
"""
from decimal import Decimal

ZERO = Decimal("0.00")


def validate_fifo_stock_available(product, qty_to_consume: Decimal) -> None:
    """
    Raise ValueError if requesting more stock than available FIFO layers.
    """
    from inventory.fifo import get_available_layers, _dec
    qty_to_consume = _dec(qty_to_consume)
    if qty_to_consume <= ZERO:
        return
    layers = get_available_layers(product)
    total_available = sum(_dec(layer.qty_remaining) for layer in layers)
    if total_available < qty_to_consume:
        raise ValueError(
            f"Insufficient FIFO stock for '{product.name}': "
            f"requested {qty_to_consume}, available {total_available}."
        )


def validate_no_backdated_sale_before_purchase(product, sale_date) -> None:
    """
    Warn if a sale date is before the earliest purchase for this product.
    """
    from inventory.models import InventoryMovement
    from inventory.services import PURCHASE_SOURCE_TYPES
    first_purchase = (
        InventoryMovement.objects.filter(
            product=product, source_type__in=PURCHASE_SOURCE_TYPES, qty_in__gt=ZERO,
        ).order_by("date", "id").values_list("date", flat=True).first()
    )
    if first_purchase and sale_date < first_purchase:
        raise ValueError(
            f"Backdated sale for '{product.name}': sale date {sale_date} is before "
            f"the earliest purchase on {first_purchase}. "
            "Add an opening balance movement dated before this sale."
        )


def validate_non_zero_purchase_cost(product, unit_cost: Decimal, is_free: bool = False) -> None:
    """Raise ValueError if purchase cost is zero and product is not marked as free."""
    if is_free:
        return
    cost = Decimal(str(unit_cost)) if unit_cost is not None else ZERO
    if cost <= ZERO:
        raise ValueError(
            f"Zero-cost purchase blocked for '{product.name}'. "
            "Set a valid purchase cost or mark the item as free/sample."
        )


def validate_not_service_item(product) -> None:
    """Raise ValueError if product is a service item (cannot be stocked)."""
    ptype = (product.type or "").strip().lower()
    if ptype == "service":
        raise ValueError(
            f"'{product.name}' is a service item and cannot be tracked in inventory."
        )


def validate_transfer_stock_available(product, qty, from_location) -> None:
    """Raise ValueError if insufficient stock at source location for a transfer."""
    from inventory.services import qty_on_hand
    available = qty_on_hand(product, location=from_location)
    qty = Decimal(str(qty))
    if qty > available:
        raise ValueError(
            f"Not enough stock for '{product.name}' at '{from_location}'. "
            f"Available: {available}, requested: {qty}."
        )


def validate_movement_has_product(product) -> None:
    """Raise ValueError if product is None."""
    if product is None:
        raise ValueError("Inventory movement must have a product.")


def validate_movement_qty(qty_in: Decimal, qty_out: Decimal) -> None:
    """Raise ValueError if both qty_in and qty_out are zero or both positive."""
    qi = Decimal(str(qty_in or 0))
    qo = Decimal(str(qty_out or 0))
    if qi <= ZERO and qo <= ZERO:
        raise ValueError("Inventory movement must have either qty_in or qty_out greater than zero.")
    if qi > ZERO and qo > ZERO:
        raise ValueError("Inventory movement cannot have both qty_in and qty_out greater than zero.")


def validate_fifo_layer_qty(qty_in: Decimal) -> None:
    """Raise ValueError if FIFO layer quantity is zero or negative."""
    qty = Decimal(str(qty_in or 0))
    if qty <= ZERO:
        raise ValueError(f"FIFO layer quantity must be greater than zero, got {qty}.")


def validate_fifo_layer_cost(unit_cost: Decimal) -> None:
    """Raise ValueError if FIFO layer cost is zero or negative."""
    cost = Decimal(str(unit_cost or 0))
    if cost <= ZERO:
        raise ValueError(f"FIFO layer cost must be greater than zero, got {cost}.")


def validate_document_not_posted_before_delete(document) -> None:
    """Raise ValueError if trying to delete a posted document without reversing."""
    status = getattr(document, "status", None)
    is_posted = getattr(document, "is_posted", False)
    if status == "posted" or is_posted:
        raise ValueError(
            f"Cannot delete posted document #{document.id}. "
            "Reverse the inventory movements first or void the document."
        )


def check_negative_inventory_balances(company=None) -> list:
    """Return a list of (product, balance) tuples where on-hand quantity is negative."""
    from django.db.models import Sum
    from inventory.models import Product, InventoryMovement
    problems = []
    qs = Product.objects.all()
    if company is not None:
        qs = qs.filter(company=company)
    for product in qs.filter(type="Inventory"):
        agg = product.movements.aggregate(tin=Sum("qty_in"), tout=Sum("qty_out"))
        balance = (agg["tin"] or ZERO) - (agg["tout"] or ZERO)
        if balance < ZERO:
            problems.append((product, balance))
    return problems
