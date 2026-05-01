# inventory/management/commands/detect_inventory_variances.py
"""
Management command: detect_inventory_variances

Detects inventory valuation variances by comparing FIFO layer values against
GL Inventory Asset balances for each product and company.  Designed for weekly
automated monitoring.

A variance is flagged when:
  |FIFO layer total - GL Inventory Asset balance| > TOLERANCE

Usage:
    python manage.py detect_inventory_variances
    python manage.py detect_inventory_variances --company-id 1
    python manage.py detect_inventory_variances --weekly
    python manage.py detect_inventory_variances --threshold 1.00
    python manage.py detect_inventory_variances --as-of 2026-03-31
"""
import time
from datetime import date as date_type
from decimal import Decimal, ROUND_HALF_UP

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Sum, Q

ZERO = Decimal("0.00")
_Q2 = Decimal("0.01")
DEFAULT_TOLERANCE = Decimal("0.02")


def _dec(v) -> Decimal:
    if v is None:
        return ZERO
    return Decimal(str(v)).quantize(_Q2, rounding=ROUND_HALF_UP)


class Command(BaseCommand):
    help = (
        "Detect inventory valuation variances between FIFO layers and GL Inventory "
        "Asset accounts.  Reports products where the two values diverge beyond the "
        "configured tolerance."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--company-id",
            dest="company_id",
            type=int,
            default=None,
            help="Limit to a specific company ID.",
        )
        parser.add_argument(
            "--weekly",
            action="store_true",
            default=False,
            help="Weekly mode: emit a condensed summary line suitable for automated alerting.",
        )
        parser.add_argument(
            "--threshold",
            dest="threshold",
            type=str,
            default=str(DEFAULT_TOLERANCE),
            help=(
                f"Variance threshold (absolute, in base currency). "
                f"Default: {DEFAULT_TOLERANCE}."
            ),
        )
        parser.add_argument(
            "--as-of",
            dest="as_of",
            default=None,
            help="Detect variances as of this date (YYYY-MM-DD). Defaults to today.",
        )
        parser.add_argument(
            "--show-ok",
            action="store_true",
            default=False,
            help="Also print products that are within tolerance (no variance).",
        )

    # ------------------------------------------------------------------
    def handle(self, *args, **options):
        from inventory.models import Product, InventoryLayer
        from accounts.models import Account, JournalLine

        company_id = options["company_id"]
        weekly = options["weekly"]
        show_ok = options["show_ok"]

        try:
            tolerance = Decimal(options["threshold"]).quantize(_Q2, rounding=ROUND_HALF_UP)
        except Exception:
            raise CommandError(
                f"Invalid threshold '{options['threshold']}'. Must be a decimal number."
            )

        as_of = date_type.today()
        if options.get("as_of"):
            try:
                as_of = date_type.fromisoformat(options["as_of"])
            except ValueError:
                raise CommandError(
                    f"Invalid date '{options['as_of']}'. Use YYYY-MM-DD."
                )

        self.stdout.write(
            f"\n{'═' * 70}\n"
            f"  Inventory Variance Detection\n"
            f"  As-of     : {as_of}\n"
            f"  Company   : {company_id or 'ALL'}\n"
            f"  Threshold : {tolerance}\n"
            f"{'═' * 70}\n"
        )

        start_time = time.time()

        product_qs = Product.objects.filter(
            type="Inventory", track_inventory=True
        ).select_related("company", "inventory_asset_account")
        if company_id:
            product_qs = product_qs.filter(company_id=company_id)

        total_products = 0
        variances_found = 0
        total_fifo = ZERO
        total_gl = ZERO

        col_w = 14

        header = (
            f"  {'Product':<30} {'FIFO Value':>{col_w}} "
            f"{'GL Asset':>{col_w}} {'Variance':>{col_w}} {'Status':<10}"
        )
        self.stdout.write(header)
        self.stdout.write(f"  {'─' * (30 + col_w * 3 + 14)}")

        for product in product_qs.order_by("company_id", "name"):
            total_products += 1

            # FIFO layer total
            fifo_total = _dec(
                InventoryLayer.objects.filter(
                    product=product, is_exhausted=False, date_created__lte=as_of,
                ).aggregate(
                    total=Sum("qty_remaining") * Sum("unit_cost")
                )["total"]
            )
            # More accurate: row-by-row sum
            fifo_total = _dec(
                sum(
                    _dec(l.qty_remaining) * _dec(l.unit_cost)
                    for l in InventoryLayer.objects.filter(
                        product=product, is_exhausted=False, date_created__lte=as_of,
                    )
                )
            )

            # GL Inventory Asset balance
            inv_acc = product.inventory_asset_account
            if inv_acc:
                agg = JournalLine.objects.filter(
                    account=inv_acc,
                    entry__date__lte=as_of,
                ).aggregate(dr=Sum("debit"), cr=Sum("credit"))
                gl_total = _dec(agg["dr"]) - _dec(agg["cr"])
            else:
                gl_total = ZERO

            variance = abs(fifo_total - gl_total)
            has_variance = variance > tolerance

            total_fifo += fifo_total
            total_gl += gl_total

            if has_variance:
                variances_found += 1
                status = "⚠ VARIANCE"
                line = (
                    f"  {str(product.name):<30} {fifo_total:>{col_w}.2f} "
                    f"{gl_total:>{col_w}.2f} {variance:>{col_w}.2f} {status}"
                )
                self.stdout.write(self.style.WARNING(line))
            elif show_ok:
                status = "✓ OK"
                self.stdout.write(
                    f"  {str(product.name):<30} {fifo_total:>{col_w}.2f} "
                    f"{gl_total:>{col_w}.2f} {variance:>{col_w}.2f} {status}"
                )

        elapsed = time.time() - start_time

        self.stdout.write(f"\n{'═' * 70}")
        all_ok = variances_found == 0
        status_style = self.style.SUCCESS if all_ok else self.style.ERROR
        self.stdout.write(
            status_style(
                f"  Variance Detection Complete\n"
                f"  Products checked  : {total_products}\n"
                f"  Variances found   : {variances_found}\n"
                f"  Total FIFO value  : {total_fifo:.2f}\n"
                f"  Total GL value    : {total_gl:.2f}\n"
                f"  Elapsed           : {elapsed:.2f}s\n"
                f"  Status            : {'✓ NO VARIANCES' if all_ok else '✗ VARIANCES DETECTED'}"
            )
        )

        if not all_ok:
            self.stdout.write(
                self.style.WARNING(
                    "\n  Investigation steps:\n"
                    "  1. python manage.py reconcile_inventory_gl --company-id <id>\n"
                    "  2. python manage.py rebuild_fifo_layers --company <id>\n"
                    "  3. python manage.py verify_inventory_fifo\n"
                )
            )

        if weekly:
            status_token = "OK" if all_ok else "VARIANCE"
            self.stdout.write(
                f"[WEEKLY] {as_of} | company={company_id or 'ALL'} | "
                f"products={total_products} | variances={variances_found} | "
                f"fifo={total_fifo:.2f} | gl={total_gl:.2f} | status={status_token}"
            )
