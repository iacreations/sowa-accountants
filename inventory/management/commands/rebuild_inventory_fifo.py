# inventory/management/commands/rebuild_inventory_fifo.py
"""
Management command to rebuild FIFO inventory layers from historical movements.

Usage:
    python manage.py rebuild_inventory_fifo
    python manage.py rebuild_inventory_fifo --dry-run
    python manage.py rebuild_inventory_fifo --company <id>
    python manage.py rebuild_inventory_fifo --product <id>
    python manage.py rebuild_inventory_fifo --from-date 2026-04-26
"""
from datetime import date as date_type
from decimal import Decimal

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction


class Command(BaseCommand):
    help = (
        "Rebuild FIFO inventory layers (InventoryLayer) from the existing "
        "InventoryMovement ledger.  Safe, idempotent, and dry-run capable."
    )

    def add_arguments(self, parser):
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
            help="Limit rebuild to a specific company ID.",
        )
        parser.add_argument(
            "--product",
            type=int,
            default=None,
            help="Limit rebuild to a specific product ID.",
        )
        parser.add_argument(
            "--from-date",
            default=None,
            dest="from_date",
            help=(
                "Only replay movements on or after this date (YYYY-MM-DD). "
                "Movements before this date are excluded from FIFO reconstruction. "
                "Requires that an OPENING movement exists at the cut-off date to "
                "seed the starting balance."
            ),
        )

    def handle(self, *args, **options):
        from inventory.models import Product, InventoryLayer
        from inventory.fifo import rebuild_layers_from_movements

        dry_run = options["dry_run"]
        company_id = options["company"]
        product_id = options["product"]

        # --- Parse optional from_date ---
        from_date = None
        if options.get("from_date"):
            try:
                from_date = date_type.fromisoformat(options["from_date"])
            except ValueError:
                raise CommandError(
                    f"Invalid date format: '{options['from_date']}'. Use YYYY-MM-DD."
                )

        qs = Product.objects.all()
        if company_id:
            qs = qs.filter(company_id=company_id)
        if product_id:
            qs = qs.filter(id=product_id)

        total = qs.count()
        date_suffix = f" (from {from_date})" if from_date else ""
        self.stdout.write(
            f"{'[DRY RUN] ' if dry_run else ''}Processing {total} product(s){date_suffix}…"
        )

        rebuilt = 0
        errors = 0

        for product in qs.iterator():
            try:
                # Use product.cut_off_date as fallback if no --from-date provided
                effective_from_date = from_date
                if effective_from_date is None:
                    effective_from_date = getattr(product, "cut_off_date", None)

                if dry_run:
                    # Simulate without saving
                    from inventory.models import InventoryMovement
                    from inventory.services import PURCHASE_SOURCE_TYPES

                    movements_qs = InventoryMovement.objects.filter(
                        product=product
                    ).order_by("date", "id")

                    if effective_from_date:
                        movements_qs = movements_qs.filter(date__gte=effective_from_date)

                    pending = []
                    for mv in movements_qs:
                        qty_in = Decimal(str(mv.qty_in or 0))
                        qty_out = Decimal(str(mv.qty_out or 0))
                        unit_cost = Decimal(str(mv.unit_cost or 0))
                        source = mv.source_type or ""

                        if qty_in > 0 and source in PURCHASE_SOURCE_TYPES:
                            pending.append({
                                "unit_cost": unit_cost,
                                "qty_remaining": qty_in,
                                "qty_in": qty_in,
                                "date": mv.date,
                            })
                        elif qty_out > 0:
                            remaining = qty_out
                            for layer in pending:
                                if remaining <= 0:
                                    break
                                take = min(layer["qty_remaining"], remaining)
                                layer["qty_remaining"] -= take
                                remaining -= take

                    active = [l for l in pending if l["qty_remaining"] > 0]
                    exhausted = [l for l in pending if l["qty_remaining"] <= 0]
                    self.stdout.write(
                        f"  [DRY] {product.name} (id={product.id}): "
                        f"{len(active)} active layer(s), "
                        f"{len(exhausted)} exhausted layer(s)"
                    )
                else:
                    with transaction.atomic():
                        company = getattr(product, "company", None)
                        rebuild_layers_from_movements(
                            product,
                            company=company,
                            from_date=effective_from_date,
                        )

                    layer_count = InventoryLayer.objects.filter(product=product).count()
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"  ✓ {product.name} (id={product.id}): "
                            f"{layer_count} layer(s) created"
                        )
                    )
                rebuilt += 1

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
                f"\n{'[DRY RUN] ' if dry_run else ''}"
                f"Done. Processed={rebuilt}, Errors={errors}"
            )
        )

