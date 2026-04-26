# inventory/fifo.py
"""
FIFO (First In, First Out) costing engine.

Purchases create InventoryLayer records.
Sales consume layers starting from the oldest available purchase layer.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import List, Tuple

from django.db import transaction

ZERO = Decimal("0.00")
_Q2 = Decimal("0.01")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _dec(v) -> Decimal:
    if v is None:
        return ZERO
    if isinstance(v, Decimal):
        return v
    try:
        return Decimal(str(v))
    except Exception:
        return ZERO


def _q2(v: Decimal) -> Decimal:
    return _dec(v).quantize(_Q2, rounding=ROUND_HALF_UP)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
@transaction.atomic
def record_purchase_layer(
    product,
    unit_cost: Decimal,
    qty_in: Decimal,
    date=None,
    movement=None,
    company=None,
):
    """
    Create one FIFO layer for a stock-in transaction.
    """
    from django.utils import timezone
    from inventory.models import InventoryLayer

    company = company or getattr(product, "company", None)
    date = date or timezone.localdate()

    unit_cost = _q2(unit_cost)
    qty_in = _q2(qty_in)

    if qty_in <= ZERO:
        raise ValueError(f"FIFO purchase layer quantity must be greater than zero for {product}.")

    if unit_cost <= ZERO:
        raise ValueError(f"FIFO purchase layer cost must be greater than zero for {product}.")

    create_kwargs = {
        "product": product,
        "unit_cost": unit_cost,
        "qty_in": qty_in,
        "qty_remaining": qty_in,
        "source_movement": movement,
        "date_created": date,
        "is_exhausted": False,
    }

    if company is not None:
        create_kwargs["company"] = company

    return InventoryLayer.objects.create(**create_kwargs)


def get_available_layers(product):
    """
    Return active FIFO layers for a product, oldest first.
    """
    return list(
        product.fifo_layers
        .filter(is_exhausted=False, qty_remaining__gt=ZERO)
        .order_by("date_created", "id")
    )


def simulate_fifo_consumption(product, qty_to_consume: Decimal) -> List[Tuple[Decimal, Decimal]]:
    """
    Read-only FIFO simulation.

    Returns:
        [(unit_cost, qty), ...]

    Important:
        This does NOT update layers.
        It raises an error if there is not enough FIFO stock.
    """
    qty_to_consume = _q2(qty_to_consume)

    if qty_to_consume <= ZERO:
        return []

    layers = get_available_layers(product)

    total_available = sum(_dec(layer.qty_remaining) for layer in layers)

    if total_available < qty_to_consume:
        raise ValueError(
            f"Not enough FIFO stock for {product.name}. "
            f"Available {total_available}, trying to consume {qty_to_consume}."
        )

    result: List[Tuple[Decimal, Decimal]] = []
    remaining = qty_to_consume

    for layer in layers:
        if remaining <= ZERO:
            break

        available = _q2(layer.qty_remaining)

        if available <= ZERO:
            continue

        take = min(available, remaining)

        result.append((_q2(layer.unit_cost), _q2(take)))

        remaining = _q2(remaining - take)

    if remaining > ZERO:
        raise ValueError(
            f"FIFO simulation failed for {product.name}. "
            f"Remaining quantity: {remaining}."
        )

    return result


@transaction.atomic
def consume_fifo_layers(product, qty_to_consume: Decimal) -> List[Tuple[Decimal, Decimal]]:
    """
    Consume FIFO layers permanently.

    Use this only when you really want to update InventoryLayer.qty_remaining.
    If your system rebuilds layers from movements after posting, use
    simulate_fifo_consumption() instead.
    """
    from inventory.models import InventoryLayer

    qty_to_consume = _q2(qty_to_consume)

    if qty_to_consume <= ZERO:
        return []

    layers = list(
        InventoryLayer.objects.select_for_update()
        .filter(
            product=product,
            is_exhausted=False,
            qty_remaining__gt=ZERO,
        )
        .order_by("date_created", "id")
    )

    total_available = sum(_dec(layer.qty_remaining) for layer in layers)

    if total_available < qty_to_consume:
        raise ValueError(
            f"Not enough FIFO stock for {product.name}. "
            f"Available {total_available}, trying to consume {qty_to_consume}."
        )

    result: List[Tuple[Decimal, Decimal]] = []
    remaining = qty_to_consume

    for layer in layers:
        if remaining <= ZERO:
            break

        available = _q2(layer.qty_remaining)
        take = min(available, remaining)

        layer.qty_remaining = _q2(available - take)

        if layer.qty_remaining <= ZERO:
            layer.qty_remaining = ZERO
            layer.is_exhausted = True

        layer.save(update_fields=["qty_remaining", "is_exhausted"])

        result.append((_q2(layer.unit_cost), _q2(take)))

        remaining = _q2(remaining - take)

    if remaining > ZERO:
        raise ValueError(
            f"FIFO consumption failed for {product.name}. "
            f"Remaining quantity: {remaining}."
        )

    return result


def compute_fifo_cogs(product, qty: Decimal) -> Decimal:
    """
    Calculate total FIFO COGS without consuming stock.
    """
    qty = _q2(qty)

    if qty <= ZERO:
        return ZERO

    fifo_rows = simulate_fifo_consumption(product, qty)

    total_cost = sum(
        _q2(unit_cost) * _q2(qty_used)
        for unit_cost, qty_used in fifo_rows
    )

    return _q2(total_cost)


@transaction.atomic
def rebuild_layers_from_movements(product, company=None, from_date=None):
    """
    Rebuild FIFO layers for a product by replaying InventoryMovement records.

    Purchases create layers.
    Sales consume the oldest layers.
    This function is idempotent.

    Args:
        product: The product whose layers to rebuild.
        company: Optional company instance (defaults to product.company).
        from_date: Optional date (datetime.date).  When supplied, only movements
                   on or after this date are replayed.  Movements before this
                   date are excluded from FIFO reconstruction; the assumption is
                   that an OPENING movement at the cut-off date already captures
                   the historical position.
    """
    from inventory.models import InventoryLayer, InventoryMovement
    from inventory.services import PURCHASE_SOURCE_TYPES

    company = company or getattr(product, "company", None)

    InventoryLayer.objects.filter(product=product).delete()

    movements_qs = (
        InventoryMovement.objects
        .filter(product=product)
        .order_by("date", "id")
    )

    if from_date is not None:
        movements_qs = movements_qs.filter(date__gte=from_date)

    pending_layers = []

    for mv in movements_qs:
        qty_in = _q2(mv.qty_in)
        qty_out = _q2(mv.qty_out)
        unit_cost = _q2(mv.unit_cost)
        source_type = mv.source_type or ""

        if qty_in > ZERO and source_type in PURCHASE_SOURCE_TYPES:
            if unit_cost <= ZERO:
                raise ValueError(
                    f"Cannot rebuild FIFO layer for {product.name}: "
                    f"movement #{mv.id} has zero unit cost."
                )

            pending_layers.append({
                "unit_cost": unit_cost,
                "qty_in": qty_in,
                "qty_remaining": qty_in,
                "movement": mv,
                "date": mv.date,
            })

        elif qty_out > ZERO:
            remaining = qty_out

            for layer in pending_layers:
                if remaining <= ZERO:
                    break

                available = _q2(layer["qty_remaining"])

                if available <= ZERO:
                    continue

                take = min(available, remaining)

                layer["qty_remaining"] = _q2(available - take)
                remaining = _q2(remaining - take)

            if remaining > ZERO:
                raise ValueError(
                    f"Cannot rebuild FIFO layers for {product.name}: "
                    f"stock goes negative by {remaining} on movement #{mv.id}."
                )

    for layer in pending_layers:
        qty_remaining = _q2(layer["qty_remaining"])
        is_exhausted = qty_remaining <= ZERO

        create_kwargs = {
            "product": product,
            "unit_cost": _q2(layer["unit_cost"]),
            "qty_in": _q2(layer["qty_in"]),
            "qty_remaining": ZERO if is_exhausted else qty_remaining,
            "source_movement": layer["movement"],
            "date_created": layer["date"],
            "is_exhausted": is_exhausted,
        }

        if company is not None:
            create_kwargs["company"] = company

        InventoryLayer.objects.create(**create_kwargs)
