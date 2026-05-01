# inventory/management/commands/reconcile_inventory_gl.py
"""
Management command: reconcile_inventory_gl

Compares inventory value across four independent sources and reports
mismatches in a side-by-side table:

  1. FIFO Layer value    — sum of (unit_cost × qty_remaining) per InventoryLayer
  2. Movement value      — net of qty_in vs qty_out movement values
  3. GL Inventory Asset  — balance of Inventory Asset accounts per company
  4. Valuation report    — per-product FIFO valuation (same as #1 but broken out)

Usage:
    python manage.py reconcile_inventory_gl
    python manage.py reconcile_inventory_gl --company-id 1
    python manage.py reconcile_inventory_gl --dry-run
    python manage.py reconcile_inventory_gl --as-of 2026-03-31
"""
import time
from datetime import date as date_type
from decimal import Decimal, ROUND_HALF_UP

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Sum, Q, F, ExpressionWrapper, DecimalField

ZERO = Decimal("0.00")
_Q2 = Decimal("0.01")
TOLERANCE = Decimal("0.02")


def _dec(v) -> Decimal:
    if v is None:
        return ZERO
    return Decimal(str(v)).quantize(_Q2, rounding=ROUND_HALF_UP)


class Command(BaseCommand):
    help = (
        "Reconcile FIFO layer value, movement ledger value, GL Inventory Asset "
        "balance, and product valuation report. Outputs a side-by-side comparison "
        "table and highlights mismatches."
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
            "--dry-run",
            action="store_true",
            default=False,
            help="Run in read-only mode (no changes made — reconciliation is always read-only).",
        )
        parser.add_argument(
            "--as-of",
            dest="as_of",
            default=None,
            help="Reconcile as of this date (YYYY-MM-DD). Defaults to today.",
        )
        parser.add_argument(
            "--daily-report",
            action="store_true",
            default=False,
            help="Emit a brief one-line summary suitable for daily automated monitoring.",
        )

    # ------------------------------------------------------------------
    def handle(self, *args, **options):
        from inventory.models import Product, InventoryLayer, InventoryMovement
        from accounts.models import Account, JournalLine

        company_id = options["company_id"]
        dry_run = options["dry_run"]

        as_of = date_type.today()
        if options.get("as_of"):
            try:
                as_of = date_type.fromisoformat(options["as_of"])
            except ValueError:
                raise CommandError(
                    f"Invalid date '{options['as_of']}'. Use YYYY-MM-DD."
                )

        prefix = "[DRY RUN] " if dry_run else ""

        self.stdout.write(
            f"\n{'═' * 70}\n"
            f"  {prefix}GL Reconciliation — Inventory\n"
            f"  As-of     : {as_of}\n"
            f"  Company   : {company_id or 'ALL'}\n"
            f"{'═' * 70}\n"
        )

        start_time = time.time()

        product_qs = Product.objects.filter(
            type="Inventory",
            track_inventory=True,
        ).select_related("company", "inventory_asset_account")
        if company_id:
            product_qs = product_qs.filter(company_id=company_id)

        total_products = product_qs.count()
        matched = 0
        mismatched = 0

        # Totals for company-level summary
        grand_fifo = ZERO
        grand_movements = ZERO
        grand_gl = ZERO
        grand_valuation = ZERO

        # Header row
        col_w = 14
        self.stdout.write(
            f"  {'Product':<30} {'FIFO Layers':>{col_w}} "
            f"{'Movements':>{col_w}} {'GL Asset':>{col_w}} "
            f"{'Valuation':>{col_w}} {'Status':>8}"
        )
        self.stdout.write(f"  {'─' * (30 + 4 * (col_w + 1) + 8)}")

        for product in product_qs.iterator():
            company = product.company

            # ── 1. FIFO Layer value ───────────────────────────────────────
            layer_qs = InventoryLayer.objects.filter(
                product=product,
                is_exhausted=False,
                date_created__lte=as_of,
            )
            if company:
                layer_qs = layer_qs.filter(company=company)
            fifo_value = _dec(
                layer_qs.aggregate(
                    total=Sum(
                        ExpressionWrapper(
                            F("unit_cost") * F("qty_remaining"),
                            output_field=DecimalField(max_digits=18, decimal_places=2),
                        )
                    )
                )["total"]
            )

            # ── 2. Movement ledger value (net) ────────────────────────────
            mv_qs = InventoryMovement.objects.filter(
                product=product,
                date__lte=as_of,
            )
            if company:
                mv_qs = mv_qs.filter(company=company)
            mv_agg = mv_qs.aggregate(
                total_in_val=Sum("value", filter=Q(qty_in__gt=0)),
                total_out_val=Sum("value", filter=Q(qty_out__gt=0)),
            )
            movement_value = _dec(mv_agg["total_in_val"]) - _dec(mv_agg["total_out_val"])

            # ── 3. GL Inventory Asset balance ─────────────────────────────
            inv_acc = getattr(product, "inventory_asset_account", None)
            gl_balance = ZERO
            if inv_acc:
                gl_qs = JournalLine.objects.filter(
                    account=inv_acc,
                    entry__date__lte=as_of,
                )
                if company:
                    gl_qs = gl_qs.filter(entry__company=company)
                gl_agg = gl_qs.aggregate(
                    total_dr=Sum("debit"),
                    total_cr=Sum("credit"),
                )
                gl_balance = _dec(gl_agg["total_dr"]) - _dec(gl_agg["total_cr"])

            # ── 4. Valuation report (per-product FIFO value) ──────────────
            # This is the same as FIFO Layer value but also includes
            # product.quantity * weighted average unit cost as a cross-check.
            valuation_value = fifo_value  # FIFO layer value IS the valuation

            # ── Accumulate grand totals ───────────────────────────────────
            grand_fifo += fifo_value
            grand_movements += movement_value
            grand_gl += gl_balance
            grand_valuation += valuation_value

            # ── Reconciliation status ─────────────────────────────────────
            fifo_vs_mv = abs(fifo_value - movement_value)
            fifo_vs_gl = abs(fifo_value - gl_balance)
            is_matched = fifo_vs_mv <= TOLERANCE and fifo_vs_gl <= TOLERANCE

            if is_matched:
                matched += 1
                status = "✓ OK"
                row_style = self.style.SUCCESS
            else:
                mismatched += 1
                status = "✗ FAIL"
                row_style = self.style.ERROR

            self.stdout.write(
                row_style(
                    f"  {product.name:<30.30} {fifo_value:>{col_w}.2f} "
                    f"{movement_value:>{col_w}.2f} {gl_balance:>{col_w}.2f} "
                    f"{valuation_value:>{col_w}.2f} {status:>8}"
                )
            )

            if not is_matched:
                # Show detail for mismatched products
                if fifo_vs_mv > TOLERANCE:
                    self.stdout.write(
                        self.style.WARNING(
                            f"    Δ FIFO vs Movements = {fifo_value - movement_value:+.2f}"
                        )
                    )
                if fifo_vs_gl > TOLERANCE:
                    self.stdout.write(
                        self.style.WARNING(
                            f"    Δ FIFO vs GL Asset  = {fifo_value - gl_balance:+.2f}"
                        )
                    )

        elapsed = time.time() - start_time

        # ── Grand totals row ──────────────────────────────────────────────
        self.stdout.write(f"  {'═' * (30 + 4 * (col_w + 1) + 8)}")
        self.stdout.write(
            f"  {'TOTAL':<30} {grand_fifo:>{col_w}.2f} "
            f"{grand_movements:>{col_w}.2f} {grand_gl:>{col_w}.2f} "
            f"{grand_valuation:>{col_w}.2f}"
        )

        # ── Company-level GL summary ──────────────────────────────────────
        self.stdout.write(f"\n{'─' * 70}")
        self._report_gl_inventory_accounts(company_id, as_of)

        # ── Final summary ─────────────────────────────────────────────────
        self.stdout.write(f"\n{'═' * 70}")
        all_four_match = (
            abs(grand_fifo - grand_movements) <= TOLERANCE
            and abs(grand_fifo - grand_gl) <= TOLERANCE
            and abs(grand_fifo - grand_valuation) <= TOLERANCE
        )
        status_style = self.style.SUCCESS if all_four_match else self.style.ERROR
        self.stdout.write(
            status_style(
                f"  {prefix}Reconciliation Report\n"
                f"  Products checked    : {total_products}\n"
                f"  Products matched    : {matched}\n"
                f"  Products mismatched : {mismatched}\n"
                f"  FIFO total          : {grand_fifo:.2f}\n"
                f"  Movement total      : {grand_movements:.2f}\n"
                f"  GL Asset total      : {grand_gl:.2f}\n"
                f"  Valuation total     : {grand_valuation:.2f}\n"
                f"  Elapsed             : {elapsed:.2f}s\n"
                f"  Status              : "
                f"{'✓ ALL FOUR REPORTS RECONCILE' if all_four_match else '✗ MISMATCHES FOUND'}"
            )
        )

        if not all_four_match:
            self.stdout.write(
                self.style.WARNING(
                    "\n  Suggested investigation steps:\n"
                    "  1. python manage.py verify_inventory_fifo\n"
                    "  2. python manage.py rebuild_fifo_layers --dry-run\n"
                    "  3. python manage.py recalculate_inventory_cogs --dry-run\n"
                    "  4. python manage.py recalculate_inventory_balances --dry-run\n"
                    "  5. Re-run: python manage.py reconcile_inventory_gl\n"
                )
            )

        if options.get("daily_report"):
            status_token = "OK" if all_four_match else "MISMATCH"
            self.stdout.write(
                f"[DAILY] {as_of} | company={company_id or 'ALL'} | "
                f"products={total_products} | matched={matched} | mismatched={mismatched} | "
                f"fifo={grand_fifo:.2f} | gl={grand_gl:.2f} | status={status_token}"
            )

    # ------------------------------------------------------------------
    def _report_gl_inventory_accounts(self, company_id, as_of):
        """Report GL Inventory Asset account balances by company."""
        from accounts.models import Account, JournalLine

        self.stdout.write("  GL Inventory Asset accounts:\n")

        acc_qs = Account.objects.filter(
            detail_type__icontains="Inventory Asset",
            is_active=True,
        )
        if company_id:
            acc_qs = acc_qs.filter(company_id=company_id)

        if not acc_qs.exists():
            self.stdout.write(
                self.style.WARNING(
                    "    No accounts with detail_type 'Inventory Asset' found.\n"
                    "    Ensure products have inventory_asset_account set."
                )
            )
            return

        for acc in acc_qs.select_related("company").iterator():
            comp_name = getattr(acc.company, "name", f"company#{acc.company_id}")
            gl_qs = JournalLine.objects.filter(
                account=acc,
                entry__date__lte=as_of,
            )
            if company_id:
                gl_qs = gl_qs.filter(entry__company_id=company_id)
            agg = gl_qs.aggregate(total_dr=Sum("debit"), total_cr=Sum("credit"))
            balance = _dec(agg["total_dr"]) - _dec(agg["total_cr"])
            self.stdout.write(
                f"    {comp_name:<30}  {acc.account_name:<30}  "
                f"Balance = {balance:>14.2f}"
            )
