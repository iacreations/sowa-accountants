# inventory/management/commands/recalculate_inventory_balances.py
"""
Management command: recalculate_inventory_balances

Recalculates Product.quantity from the InventoryMovement ledger and verifies
that FIFO layer totals match the calculated quantities.

Usage:
    python manage.py recalculate_inventory_balances
    python manage.py recalculate_inventory_balances --dry-run
    python manage.py recalculate_inventory_balances --company-id 1
    python manage.py recalculate_inventory_balances --product-id 5
    python manage.py recalculate_inventory_balances --from-date 2026-01-01
    python manage.py recalculate_inventory_balances --to-date 2026-12-31
"""
import time
from datetime import date as date_type
from decimal import Decimal, ROUND_HALF_UP

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models import Sum, F, ExpressionWrapper, DecimalField

ZERO = Decimal("0.00")
_Q2 = Decimal("0.01")


def _dec(v) -> Decimal:
    if v is None:
        return ZERO
    return Decimal(str(v)).quantize(_Q2, rounding=ROUND_HALF_UP)


class Command(BaseCommand):
    help = (
        "Recalculate Product.quantity from InventoryMovement ledger. "
        "Verifies FIFO layer totals match movement totals. "
        "Reports quantity corrections and reconciliation status."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Show corrections without writing to the database.",
        )
        parser.add_argument(
            "--company-id",
            dest="company_id",
            type=int,
            default=None,
            help="Limit to a specific company ID.",
        )
        parser.add_argument(
            "--product-id",
            dest="product_id",
            type=int,
            default=None,
            help="Limit to a specific product ID.",
        )
        parser.add_argument(
            "--from-date",
            dest="from_date",
            default=None,
            help="Only include movements on or after this date (YYYY-MM-DD).",
        )
        parser.add_argument(
            "--to-date",
            dest="to_date",
            default=None,
            help="Only include movements up to and including this date (YYYY-MM-DD).",
        )

    # ------------------------------------------------------------------
    def handle(self, *args, **options):
        from inventory.models import Product, InventoryMovement, InventoryLayer, InventoryLocation

        dry_run = options["dry_run"]
        company_id = options["company_id"]
        product_id = options["product_id"]
        from_date = self._parse_date(options.get("from_date"), "--from-date")
        to_date = self._parse_date(options.get("to_date"), "--to-date")

        if from_date and to_date and from_date > to_date:
            raise CommandError("--from-date must be before --to-date.")

        prefix = "[DRY RUN] " if dry_run else ""

        self.stdout.write(
            f"\n{'═' * 70}\n"
            f"  {prefix}Recalculate Inventory Balances\n"
            f"  Company   : {company_id or 'ALL'}\n"
            f"  Product   : {product_id or 'ALL'}\n"
            f"  From date : {from_date or 'beginning'}\n"
            f"  To date   : {to_date or 'today'}\n"
            f"{'═' * 70}\n"
        )

        product_qs = Product.objects.filter(
            type="Inventory",
            track_inventory=True,
        ).select_related("company")
        if company_id:
            product_qs = product_qs.filter(company_id=company_id)
        if product_id:
            product_qs = product_qs.filter(id=product_id)

        total_products = product_qs.count()
        self.stdout.write(f"  Products to process: {total_products}\n")

        start_time = time.time()
        corrections = 0
        matches = 0
        fifo_mismatches = 0
        errors = 0

        for product in product_qs.iterator():
            try:
                result = self._process_product(
                    product,
                    from_date=from_date,
                    to_date=to_date,
                    dry_run=dry_run,
                )

                if result["qty_corrected"]:
                    corrections += 1
                    self.stdout.write(
                        self.style.WARNING(
                            f"  CORRECT {product.name} (id={product.id}): "
                            f"qty {result['qty_before']} → {result['qty_calculated']}"
                        )
                    )
                else:
                    matches += 1
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"  ✓ {product.name} (id={product.id}): "
                            f"qty={result['qty_calculated']}"
                        )
                    )

                if result["fifo_qty_mismatch"]:
                    fifo_mismatches += 1
                    self.stdout.write(
                        self.style.ERROR(
                            f"    ⚠ FIFO mismatch: "
                            f"fifo_qty={result['fifo_qty']} ≠ "
                            f"movement_qty={result['qty_calculated']}"
                        )
                    )

                # Report per-location quantities
                for loc_name, loc_qty in result["location_qtys"].items():
                    self.stdout.write(
                        f"    Location '{loc_name}': qty={loc_qty}"
                    )

            except Exception as exc:
                errors += 1
                self.stderr.write(
                    self.style.ERROR(
                        f"  ✗ {product.name} (id={product.id}): {exc}"
                    )
                )

        elapsed = time.time() - start_time

        # ── Summary ───────────────────────────────────────────────────────
        self.stdout.write(f"\n{'═' * 70}")
        status_style = (
            self.style.SUCCESS
            if (errors == 0 and fifo_mismatches == 0)
            else self.style.WARNING
        )
        self.stdout.write(
            status_style(
                f"  {prefix}Recalculation complete\n"
                f"  Products processed  : {total_products}\n"
                f"  Qty matched         : {matches}\n"
                f"  Qty corrected       : {corrections}\n"
                f"  FIFO mismatches     : {fifo_mismatches}\n"
                f"  Errors              : {errors}\n"
                f"  Elapsed             : {elapsed:.2f}s\n"
                f"  Status              : "
                f"{'✓ RECONCILED' if errors == 0 and fifo_mismatches == 0 else '✗ ISSUES FOUND'}"
            )
        )

        if fifo_mismatches > 0:
            self.stdout.write(
                self.style.WARNING(
                    "\n  FIFO mismatches detected. Suggested fix:\n"
                    "    python manage.py rebuild_fifo_layers --dry-run\n"
                    "    python manage.py rebuild_fifo_layers\n"
                )
            )

    # ------------------------------------------------------------------
    def _process_product(self, product, *, from_date, to_date, dry_run):
        """
        Recalculate and (optionally) update Product.quantity for one product.
        Returns a status dict.
        """
        from inventory.models import InventoryMovement, InventoryLayer, InventoryLocation

        # ── Calculate qty from movements ──────────────────────────────────
        mv_qs = InventoryMovement.objects.filter(product=product)
        if from_date:
            mv_qs = mv_qs.filter(date__gte=from_date)
        if to_date:
            mv_qs = mv_qs.filter(date__lte=to_date)

        agg = mv_qs.aggregate(
            total_in=Sum("qty_in"),
            total_out=Sum("qty_out"),
        )
        qty_calculated = _dec(agg["total_in"]) - _dec(agg["total_out"])
        qty_before = _dec(product.quantity)
        qty_corrected = abs(qty_calculated - qty_before) > _dec("0.005")

        # ── FIFO remaining qty ────────────────────────────────────────────
        fifo_agg = InventoryLayer.objects.filter(
            product=product,
            is_exhausted=False,
        ).aggregate(total=Sum("qty_remaining"))
        fifo_qty = _dec(fifo_agg["total"])
        fifo_qty_mismatch = abs(fifo_qty - qty_calculated) > _dec("0.02")

        # ── Quantity per location ─────────────────────────────────────────
        location_qtys = {}
        loc_agg = (
            mv_qs
            .values("location__name")
            .annotate(
                total_in=Sum("qty_in"),
                total_out=Sum("qty_out"),
            )
        )
        for row in loc_agg:
            loc_name = row["location__name"] or "No Location"
            loc_net = _dec(row["total_in"]) - _dec(row["total_out"])
            if loc_net != ZERO:
                location_qtys[loc_name] = loc_net

        # ── Apply correction if needed ────────────────────────────────────
        if qty_corrected and not dry_run:
            with transaction.atomic():
                product.quantity = qty_calculated
                product.save(update_fields=["quantity"])

        return {
            "qty_before": qty_before,
            "qty_calculated": qty_calculated,
            "qty_corrected": qty_corrected,
            "fifo_qty": fifo_qty,
            "fifo_qty_mismatch": fifo_qty_mismatch,
            "location_qtys": location_qtys,
        }

    @staticmethod
    def _parse_date(value, flag_name):
        if not value:
            return None
        try:
            return date_type.fromisoformat(value)
        except ValueError:
            raise CommandError(
                f"Invalid date format for {flag_name}: '{value}'. Use YYYY-MM-DD."
            )
