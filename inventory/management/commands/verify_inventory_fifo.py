# inventory/management/commands/verify_inventory_fifo.py
"""
Management command: verify_inventory_fifo

Checks all products for FIFO consistency and reports any issues.

Usage:
    python manage.py verify_inventory_fifo
    python manage.py verify_inventory_fifo --date 2026-04-26
    python manage.py verify_inventory_fifo --product 1
    python manage.py verify_inventory_fifo --company 1
    python manage.py verify_inventory_fifo --fix
"""
from datetime import date as date_type
from decimal import Decimal

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Sum


ZERO = Decimal("0.00")


class Command(BaseCommand):
    help = (
        "Verify FIFO inventory integrity for all products. "
        "Reports negative stock, missing layers, zero-cost layers, and more."
    )

    def add_arguments(self, parser):
        parser.add_argument("--date", default=None, help="Check as of this date (YYYY-MM-DD).")
        parser.add_argument("--product", type=int, default=None, help="Limit to a specific product ID.")
        parser.add_argument("--company", type=int, default=None, help="Limit to a specific company ID.")
        parser.add_argument(
            "--fix",
            action="store_true",
            default=False,
            help="Attempt to fix issues by rebuilding FIFO layers.",
        )

    def handle(self, *args, **options):
        from inventory.models import Product, InventoryLayer, InventoryMovement
        from inventory.fifo import rebuild_layers_from_movements
        from inventory.services import PURCHASE_SOURCE_TYPES

        company_id = options["company"]
        product_id = options["product"]
        fix = options["fix"]

        as_of_date = None
        if options.get("date"):
            try:
                as_of_date = date_type.fromisoformat(options["date"])
            except ValueError:
                raise CommandError(f"Invalid date format: '{options['date']}'. Use YYYY-MM-DD.")

        qs = Product.objects.filter(type="Inventory")
        if company_id:
            qs = qs.filter(company_id=company_id)
        if product_id:
            qs = qs.filter(id=product_id)

        total = qs.count()
        self.stdout.write(f"Verifying FIFO integrity for {total} product(s)…\n")

        issues = 0
        ok = 0

        for product in qs.select_related("company").iterator():
            product_issues = []

            # Check 1: Negative stock balance
            movements_qs = product.movements.all()
            if as_of_date:
                movements_qs = movements_qs.filter(date__lte=as_of_date)
            agg = movements_qs.aggregate(tin=Sum("qty_in"), tout=Sum("qty_out"))
            balance = (Decimal(str(agg["tin"] or 0))) - (Decimal(str(agg["tout"] or 0)))
            if balance < ZERO:
                product_issues.append(f"  ⚠ Negative stock balance: {balance}")

            # Check 2: Movements without FIFO layers
            purchase_movements = movements_qs.filter(
                source_type__in=PURCHASE_SOURCE_TYPES,
                qty_in__gt=ZERO,
            ).count()
            layer_count = product.fifo_layers.count()
            if purchase_movements > 0 and layer_count == 0:
                product_issues.append(f"  ⚠ {purchase_movements} purchase movement(s) but no FIFO layers")

            # Check 3: Layers with zero cost
            zero_cost_layers = product.fifo_layers.filter(unit_cost__lte=ZERO).count()
            if zero_cost_layers > 0:
                product_issues.append(f"  ⚠ {zero_cost_layers} FIFO layer(s) with zero/negative cost")

            # Check 4: Products without opening stock (when has post-cutoff sales)
            if product.cut_off_date:
                opening_mvs = product.movements.filter(
                    source_type="OPENING",
                    date=product.cut_off_date,
                    is_opening_balance=True,
                ).count()
                if opening_mvs == 0:
                    product_issues.append(
                        f"  ⚠ cut_off_date={product.cut_off_date} set but no OPENING movement found"
                    )

            # Check 5: Purchase movements with zero cost
            zero_cost_purchases = movements_qs.filter(
                source_type__in=PURCHASE_SOURCE_TYPES,
                qty_in__gt=ZERO,
                unit_cost__lte=ZERO,
            ).count()
            if zero_cost_purchases > 0:
                product_issues.append(f"  ⚠ {zero_cost_purchases} purchase movement(s) with zero cost")

            if product_issues:
                issues += 1
                self.stdout.write(
                    self.style.WARNING(
                        f"✗ {product.name} (id={product.id}, company={getattr(product.company, 'name', '?')}):"
                    )
                )
                for msg in product_issues:
                    self.stdout.write(self.style.WARNING(msg))

                if fix:
                    try:
                        company = getattr(product, "company", None)
                        from_date = getattr(product, "cut_off_date", None)
                        rebuild_layers_from_movements(product, company=company, from_date=from_date)
                        self.stdout.write(self.style.SUCCESS(f"  ✓ Rebuilt FIFO layers for {product.name}"))
                    except Exception as exc:
                        self.stderr.write(self.style.ERROR(f"  ✗ Could not fix {product.name}: {exc}"))
            else:
                ok += 1
                self.stdout.write(self.style.SUCCESS(f"✓ {product.name} (id={product.id}) — OK"))

        self.stdout.write(f"\n{'─'*50}")
        summary_style = self.style.SUCCESS if issues == 0 else self.style.WARNING
        self.stdout.write(
            summary_style(f"Done. OK={ok}, Issues={issues}, Total={total}")
        )
        if issues > 0:
            self.stdout.write(
                self.style.WARNING(
                    "\nSuggested fixes:\n"
                    "  python manage.py rebuild_inventory_fifo\n"
                    "  python manage.py reset_inventory_fifo_from_opening_stock --date YYYY-MM-DD\n"
                    "  python manage.py verify_inventory_fifo --fix"
                )
            )
