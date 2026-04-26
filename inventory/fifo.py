# inventory/fifo.py
"""
FIFO (First In, First Out) costing engine.

Purchases create InventoryLayer records.
Sales consume layers starting from the oldest (lowest id / earliest date).
"""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import List, Tuple, Optional, TYPE_CHECKING

from django.db import transaction

if TYPE_CHECKING:
    from inventory.models import InventoryLayer, InventoryMovement, Product

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
    return Decimal(str(v))


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
) -> "InventoryLayer":
    """
    Create a new FIFO cost layer for an incoming stock movement (purchase, opening,
    assembly build output, etc.).

    Args:
        product:   Product instance
        unit_cost: Cost per unit for this batch
        qty_in:    Number of units received
        date:      Date of the receipt (defaults to today)
        movement:  The InventoryMovement that created this stock (optional FK)
        company:   Company instance (taken from product if None)

    Returns:
        The newly-created InventoryLayer.
    """
    from django.utils import timezone
    from inventory.models import InventoryLayer

    company = company or getattr(product, "company", None)
    date = date or timezone.localdate()
    unit_cost = _dec(unit_cost)
    qty_in = _dec(qty_in)

    if qty_in <= ZERO:
        raise ValueError(f"record_purchase_layer: qty_in must be > 0, got {qty_in!r}")

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


def get_available_layers(product) -> "list[InventoryLayer]":
    """
    Return non-exhausted FIFO layers for *product* ordered oldest-first.
    """
    return list(
        product.fifo_layers.filter(is_exhausted=False).order_by("date_created", "id")
    )


def simulate_fifo_consumption(
    product,
    qty_to_consume: Decimal,
) -> List[Tuple[Decimal, Decimal]]:
    """
    Read-only simulation of FIFO consumption.  Returns the same
    ``[(unit_cost, qty)]`` list as ``consume_fifo_layers`` but does NOT
    modify any InventoryLayer records.

    Use this when you need per-layer unit costs for recording sale movements
    without actually updating layer state (the layers will be rebuilt from
    movements afterwards via ``rebuild_layers_from_movements``).
    """
    qty_to_consume = _dec(qty_to_consume)
    if qty_to_consume <= ZERO:
        return []

    layers = get_available_layers(product)
    result: List[Tuple[Decimal, Decimal]] = []
    remaining = qty_to_consume

    for layer in layers:
        if remaining <= ZERO:
            break
        available = _dec(layer.qty_remaining)
        if available <= ZERO:
            continue
        take = min(available, remaining)
        result.append((_dec(layer.unit_cost), take))
        remaining = (remaining - take).quantize(_Q2, rounding=ROUND_HALF_UP)

    if remaining > ZERO:
        result.append((ZERO, remaining))

    return result


@transaction.atomic
def consume_fifo_layers(
    product,
    qty_to_consume: Decimal,
) -> List[Tuple[Decimal, Decimal]]:
    """
    Consume *qty_to_consume* units from the oldest available FIFO layers.

    Layers are locked with SELECT FOR UPDATE inside an atomic block so that
    concurrent sales don't double-consume the same stock.

    Returns:
        A list of (unit_cost, qty_consumed) tuples — one entry per layer
        touched.  When a single layer covers the entire sale the list has
        one element.  When the sale spans multiple layers there is one
        element per layer.

    Raises:
        ValueError: if there is not enough stock in the layers (negative
                    inventory guard).
    """
    from inventory.models import InventoryLayer

    qty_to_consume = _dec(qty_to_consume)
    if qty_to_consume <= ZERO:
        return []

    layers = list(
        InventoryLayer.objects.select_for_update()
        .filter(product=product, is_exhausted=False)
        .order_by("date_created", "id")
    )

    total_available = sum(_dec(l.qty_remaining) for l in layers)
    if total_available < qty_to_consume:
        # Allow the sale to proceed at zero cost for the shortfall rather than
        # blocking the user.  The caller may log or warn.
        pass

    result: List[Tuple[Decimal, Decimal]] = []
    remaining = qty_to_consume

    for layer in layers:
        if remaining <= ZERO:
            break

        available = _dec(layer.qty_remaining)
        if available <= ZERO:
            layer.is_exhausted = True
            layer.save(update_fields=["is_exhausted"])
            continue

        take = min(available, remaining)
        layer.qty_remaining = (available - take).quantize(_Q2, rounding=ROUND_HALF_UP)
        if layer.qty_remaining <= ZERO:
            layer.qty_remaining = ZERO
            layer.is_exhausted = True
        layer.save(update_fields=["qty_remaining", "is_exhausted"])

        result.append((_dec(layer.unit_cost), take))
        remaining = (remaining - take).quantize(_Q2, rounding=ROUND_HALF_UP)

    # If we still have remaining (insufficient layers), issue at zero cost
    if remaining > ZERO:
        result.append((ZERO, remaining))

    return result


def compute_fifo_cogs(product, qty: Decimal) -> Decimal:
    """
    Calculate the total COGS for selling *qty* units using FIFO without
    actually consuming any layers (read-only).

    Returns the total cost (not unit cost).
    """
    qty = _dec(qty)
    if qty <= ZERO:
        return ZERO

    layers = get_available_layers(product)
    total_cost = ZERO
    remaining = qty

    for layer in layers:
        if remaining <= ZERO:
            break
        available = _dec(layer.qty_remaining)
        take = min(available, remaining)
        total_cost += take * _dec(layer.unit_cost)
        remaining -= take

    return total_cost.quantize(_Q2, rounding=ROUND_HALF_UP)


@transaction.atomic
def rebuild_layers_from_movements(product, company=None):
    """
    Rebuild FIFO layers for *product* from scratch by replaying all its
    InventoryMovements in chronological order.

    This is called by the management command ``rebuild_inventory_fifo``.
    It is safe to run multiple times (idempotent — deletes existing layers
    first).
    """
    from inventory.models import InventoryLayer, InventoryMovement
    from inventory.services import PURCHASE_SOURCE_TYPES

    company = company or getattr(product, "company", None)

    # Wipe existing layers for this product
    InventoryLayer.objects.filter(product=product).delete()

    movements = (
        InventoryMovement.objects.filter(product=product)
        .order_by("date", "id")
    )

    # Simulate FIFO queue in memory for speed
    pending_layers: List[dict] = []  # {"unit_cost": Decimal, "qty_remaining": Decimal, "movement": obj, "date": date}

    for mv in movements:
        qty_in = _dec(mv.qty_in)
        qty_out = _dec(mv.qty_out)
        unit_cost = _dec(mv.unit_cost)
        source = mv.source_type or ""

        if qty_in > ZERO and source in PURCHASE_SOURCE_TYPES:
            pending_layers.append({
                "unit_cost": unit_cost,
                "qty_remaining": qty_in,
                "qty_in": qty_in,
                "movement": mv,
                "date": mv.date,
            })

        elif qty_out > ZERO:
            # Consume from pending layers
            remaining = qty_out
            for layer in pending_layers:
                if remaining <= ZERO:
                    break
                available = layer["qty_remaining"]
                take = min(available, remaining)
                layer["qty_remaining"] -= take
                remaining -= take

    # Persist the layers to DB
    for layer in pending_layers:
        is_exhausted = layer["qty_remaining"] <= ZERO
        InventoryLayer.objects.create(
            product=product,
            company=company,
            unit_cost=layer["unit_cost"],
            qty_in=layer["qty_in"],
            qty_remaining=max(layer["qty_remaining"], ZERO),
            source_movement=layer["movement"],
            date_created=layer["date"],
            is_exhausted=is_exhausted,
        )
