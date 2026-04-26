# inventory/validators.py
"""
Inventory safeguards and validators.

These validators protect the integrity of FIFO inventory data by:
- Blocking sales that exceed available FIFO stock
- Catching backdated sales before purchases
- Checking for negative inventory balances
"""

from decimal import Decimal

ZERO = Decimal("0.00")


def validate_fifo_stock_available(product, qty_to_consume: Decimal) -> None:
    """
    Raise ValueError if requesting more stock than available FIFO layers.

    Call this before posting a sale/invoice line to prevent negative inventory.
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

    Raises ValueError when a sale is backdated before the first purchase
    and there is no opening balance movement to cover it.
    """
    from inventory.models import InventoryMovement
    from inventory.services import PURCHASE_SOURCE_TYPES

    first_purchase = (
        InventoryMovement.objects.filter(
            product=product,
            source_type__in=PURCHASE_SOURCE_TYPES,
            qty_in__gt=ZERO,
        )
        .order_by("date", "id")
        .values_list("date", flat=True)
        .first()
    )

    if first_purchase and sale_date < first_purchase:
        raise ValueError(
            f"Backdated sale for '{product.name}': sale date {sale_date} is before "
            f"the earliest purchase on {first_purchase}. "
            "Add an opening balance movement dated before this sale."
        )


def check_negative_inventory_balances(company=None) -> list:
    """
    Return a list of (product, balance) tuples where on-hand quantity is negative.

    Used for integrity audits.
    """
    from django.db.models import Sum
    from inventory.models import Product, InventoryMovement
    from inventory.services import is_inventory

    problems = []

    qs = Product.objects.all()
    if company is not None:
        qs = qs.filter(company=company)

    for product in qs.filter(type="Inventory"):
        agg = product.movements.aggregate(
            tin=Sum("qty_in"),
            tout=Sum("qty_out"),
        )
        balance = (agg["tin"] or ZERO) - (agg["tout"] or ZERO)
        if balance < ZERO:
            problems.append((product, balance))

    return problems
