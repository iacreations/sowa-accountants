# inventory/management/commands/reset_inventory_fifo_from_opening_stock.py
"""
Management command: reset_inventory_fifo_from_opening_stock

Establishes a known-good FIFO starting position (cut-off date) by:

1. Deleting all existing FIFO layers.
2. Removing any old OPENING stock movements dated on or after the cut-off date.
3. Creating fresh OPENING inventory movements dated at the cut-off date using
   confirmed current stock quantities and unit costs from product fields.
4. Rebuilding FIFO layers from those OPENING movements only.
5. Recalculating product.quantity from movements on or after the cut-off date.
6. Recording the cut-off date on each product.

Usage:
    python manage.py reset_inventory_fifo_from_opening_stock
    python manage.py reset_inventory_fifo_from_opening_stock --date 2026-04-26
    python manage.py reset_inventory_fifo_from_opening_stock --date 2026-04-26 --dry-run
    python manage.py reset_inventory_fifo_from_opening_stock --date 2026-04-26 --company 1
    python manage.py reset_inventory_fifo_from_opening_stock --date 2026-04-26 --product 5
"""
from datetime import date as date_type
from decimal import Decimal

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone


ZERO = Decimal("0.00")


class Command(BaseCommand):
    help = (
        "Reset FIFO inventory from opening stock at a cut-off date.  "
        "Deletes old FIFO layers, creates fresh OPENING movements, and "
        "rebuilds layers from the cut-off date forward."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--date",
            default=None,
            help="Cut-off date in YYYY-MM-DD format (default: today).",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Print what would be done without making any database changes.",
        )
        parser.add_argument(
            "--company",
            type=int,
            default=None,
            help="Limit reset to a specific company ID.",
        )
        parser.add_argument(
            "--product",
            type=int,
            default=None,
            help="Limit reset to a specific product ID.",
        )

    def handle(self, *args, **options):
        from inventory.models import (
            Product, InventoryLayer, InventoryMovement,
        )
        from inventory.fifo import rebuild_layers_from_movements
        from inventory.services import (
            get_default_location,
            is_inventory,
            PURCHASE_SOURCE_TYPES,
        )

        dry_run = options["dry_run"]
        company_id = options["company"]
        product_id = options["product"]

        # --- Parse cut-off date ---
        if options["date"]:
            try:
                cut_off = date_type.fromisoformat(options["date"])
            except ValueError:
                raise CommandError(
                    f"Invalid date format: '{options['date']}'. Use YYYY-MM-DD."
                )
        else:
            cut_off = timezone.localdate()

        prefix = "[DRY RUN] " if dry_run else ""
        self.stdout.write(
            f"{prefix}Resetting FIFO from opening stock at cut-off date: {cut_off}"
        )

        # --- Resolve product queryset ---
        qs = Product.objects.filter(type="Inventory")
        if company_id:
            qs = qs.filter(company_id=company_id)
        if product_id:
            qs = qs.filter(id=product_id)

        total = qs.count()
        self.stdout.write(f"{prefix}Processing {total} inventory product(s)…")

        reset_count = 0
        skip_count = 0
        errors = 0

        for product in qs.iterator():
            try:
                company = getattr(product, "company", None)

                # --- Determine opening quantity and cost ---
                # Use movements BEFORE the cut-off date to determine what the
                # historical balance was (total_in - total_out before cutoff),
                # then take the confirmed unit_cost from purchase-type movements.
                pre_movements = InventoryMovement.objects.filter(
                    product=product,
                    date__lt=cut_off,
                ).order_by("date", "id")

                opening_qty = ZERO
                last_purchase_cost = ZERO

                for mv in pre_movements:
                    qty_in = Decimal(str(mv.qty_in or 0))
                    qty_out = Decimal(str(mv.qty_out or 0))
                    unit_cost = Decimal(str(mv.unit_cost or 0))
                    opening_qty += qty_in - qty_out
                    if qty_in > ZERO and mv.source_type in PURCHASE_SOURCE_TYPES:
                        if unit_cost > ZERO:
                            last_purchase_cost = unit_cost

                # Also check current product.avg_cost / purchase_price for cost
                if last_purchase_cost <= ZERO:
                    last_purchase_cost = Decimal(str(product.avg_cost or 0))
                if last_purchase_cost <= ZERO:
                    last_purchase_cost = Decimal(str(product.purchase_price or 0))

                if opening_qty <= ZERO:
                    self.stdout.write(
                        f"  {prefix}SKIP {product.name} (id={product.id}): "
                        f"opening qty = {opening_qty} (no stock before cut-off)"
                    )
                    skip_count += 1
                    continue

                if last_purchase_cost <= ZERO:
                    self.stderr.write(
                        self.style.WARNING(
                            f"  {prefix}WARN {product.name} (id={product.id}): "
                            f"opening qty={opening_qty} but no unit cost found — "
                            "skipping FIFO layer (will log opening balance only)."
                        )
                    )

                if dry_run:
                    self.stdout.write(
                        f"  [DRY] {product.name} (id={product.id}): "
                        f"would create OPENING movement: qty={opening_qty}, "
                        f"unit_cost={last_purchase_cost}, date={cut_off}"
                    )
                    reset_count += 1
                    continue

                with transaction.atomic():
                    # 1. Delete all existing FIFO layers for this product
                    InventoryLayer.objects.filter(product=product).delete()

                    # 2. Remove any OPENING movements on or after the cut-off date
                    #    (clean up previous resets for this date or later)
                    InventoryMovement.objects.filter(
                        product=product,
                        source_type="OPENING",
                        date__gte=cut_off,
                    ).delete()

                    # 3. Create a fresh OPENING movement at cut-off date
                    if last_purchase_cost > ZERO:
                        loc = get_default_location(company=company)
                        opening_mv = InventoryMovement(
                            product=product,
                            location=loc,
                            date=cut_off,
                            qty_in=opening_qty,
                            qty_out=ZERO,
                            unit_cost=last_purchase_cost,
                            value=opening_qty * last_purchase_cost,
                            source_type="OPENING",
                            source_id=product.id,
                            is_opening_balance=True,
                        )
                        if company is not None:
                            opening_mv.company = company
                        opening_mv.save()

                    # 4. Rebuild FIFO layers from the cut-off date onward
                    rebuild_layers_from_movements(product, company=company, from_date=cut_off)

                    # 5. Recalculate product.quantity from movements >= cut-off only
                    from django.db.models import Sum
                    agg = product.movements.filter(date__gte=cut_off).aggregate(
                        tin=Sum("qty_in"),
                        tout=Sum("qty_out"),
                    )
                    new_qty = (agg["tin"] or ZERO) - (agg["tout"] or ZERO)
                    product.quantity = new_qty
                    product.cut_off_date = cut_off
                    product.save(update_fields=["quantity", "cut_off_date"])

                layer_count = InventoryLayer.objects.filter(product=product).count()
                self.stdout.write(
                    self.style.SUCCESS(
                        f"  ✓ {product.name} (id={product.id}): "
                        f"opening qty={opening_qty} @ {last_purchase_cost}, "
                        f"{layer_count} FIFO layer(s) created"
                    )
                )
                reset_count += 1

            except Exception as exc:  # noqa: BLE001
                errors += 1
                self.stderr.write(
                    self.style.ERROR(
                        f"  ✗ {product.name} (id={product.id}): {exc}"
                    )
                )

        summary_style = self.style.SUCCESS if errors == 0 else self.style.WARNING
        self.stdout.write(
            summary_style(
                f"\n{prefix}Done. Reset={reset_count}, Skipped={skip_count}, Errors={errors}"
            )
        )

        if not dry_run and errors == 0:
            self.stdout.write(
                self.style.SUCCESS(
                    f"\nInventory FIFO reset to {cut_off}. "
                    "Now run: python manage.py rebuild_inventory_fifo "
                    f"--from-date {cut_off}"
                )
            )
