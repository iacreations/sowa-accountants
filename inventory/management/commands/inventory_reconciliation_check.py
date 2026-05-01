# inventory/management/commands/inventory_reconciliation_check.py
"""
Management command: inventory_reconciliation_check

Compares inventory value across four independent sources to prove that the
accounting engine is internally consistent:

  1. FIFO Layer value         — sum of (unit_cost × qty_remaining) per InventoryLayer
  2. Movement value           — sum of value for qty_in movements minus value for qty_out movements
  3. GL Inventory Asset       — balance of all accounts with detail_type "Inventory Asset"
  4. Balance Sheet inventory  — same as (3), reported separately for cross-check

A mismatch between any of these indicates a posting error that must be
investigated.

Usage:
    python manage.py inventory_reconciliation_check
    python manage.py inventory_reconciliation_check --company 1
    python manage.py inventory_reconciliation_check --product 5
    python manage.py inventory_reconciliation_check --as-of 2026-03-31
    python manage.py inventory_reconciliation_check --show-mismatches-only
"""
from decimal import Decimal, ROUND_HALF_UP
from datetime import date as date_type

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Sum, Q

ZERO = Decimal("0.00")
_Q2 = Decimal("0.01")


def _dec(v) -> Decimal:
    if v is None:
        return ZERO
    return Decimal(str(v)).quantize(_Q2, rounding=ROUND_HALF_UP)


class Command(BaseCommand):
    help = (
        "Reconcile FIFO layer values, inventory movement values, and GL "
        "Inventory Asset account balances.  Reports matched/mismatched items "
        "by company and product."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--company",
            type=int,
            default=None,
            help="Limit to a specific company ID.",
        )
        parser.add_argument(
            "--product",
            type=int,
            default=None,
            help="Limit to a specific product ID.",
        )
        parser.add_argument(
            "--as-of",
            dest="as_of",
            default=None,
            help="Reconcile as of this date (YYYY-MM-DD).  Defaults to today.",
        )
        parser.add_argument(
            "--show-mismatches-only",
            dest="mismatches_only",
            action="store_true",
            default=False,
            help="Only print products with mismatches.",
        )

    # ------------------------------------------------------------------
    def handle(self, *args, **options):
        from inventory.models import Product, InventoryLayer, InventoryMovement
        from accounts.models import Account, JournalLine

        company_id = options["company"]
        product_id = options["product"]
        mismatches_only = options["mismatches_only"]

        as_of = date_type.today()
        if options.get("as_of"):
            try:
                as_of = date_type.fromisoformat(options["as_of"])
            except ValueError:
                raise CommandError(
                    f"Invalid date '{options['as_of']}'. Use YYYY-MM-DD."
                )

        self.stdout.write(
            f"\n{'═'*70}\n"
            f"  Inventory Reconciliation Check  (as of {as_of})\n"
            f"{'═'*70}\n"
        )

        # ── 1. Product-level reconciliation ────────────────────────────────
        product_qs = Product.objects.filter(
            type="Inventory",
            track_inventory=True,
        ).select_related("company", "inventory_asset_account")
        if company_id:
            product_qs = product_qs.filter(company_id=company_id)
        if product_id:
            product_qs = product_qs.filter(id=product_id)

        total_products = product_qs.count()
        matched = 0
        mismatched = 0

        for product in product_qs.iterator():
            company = product.company
            comp_name = getattr(company, "name", f"company#{getattr(company, 'id', '?')}")

            # ── FIFO Layer value ──────────────────────────────────────────
            layer_agg = InventoryLayer.objects.filter(
                product=product,
                is_exhausted=False,
            )
            if company:
                layer_agg = layer_agg.filter(company=company)
            # Only layers created on or before as_of
            layer_agg = layer_agg.filter(date_created__lte=as_of)
            from django.db.models import F, ExpressionWrapper, DecimalField
            layer_value = _dec(
                layer_agg.aggregate(
                    total=Sum(
                        ExpressionWrapper(
                            F("unit_cost") * F("qty_remaining"),
                            output_field=DecimalField(max_digits=18, decimal_places=2),
                        )
                    )
                )["total"]
            )

            # ── Movement value (net) ──────────────────────────────────────
            mv_qs = InventoryMovement.objects.filter(
                product=product,
                date__lte=as_of,
            )
            if company:
                mv_qs = mv_qs.filter(company=company)
            mv_agg = mv_qs.aggregate(
                total_in_val=Sum("value", filter=Q(qty_in__gt=0)),
                total_out_val=Sum("value", filter=Q(qty_out__gt=0)),
                total_in_qty=Sum("qty_in"),
                total_out_qty=Sum("qty_out"),
            )
            movement_value = _dec(mv_agg["total_in_val"]) - _dec(mv_agg["total_out_val"])
            total_in_qty = _dec(mv_agg["total_in_qty"])
            total_out_qty = _dec(mv_agg["total_out_qty"])
            net_qty = total_in_qty - total_out_qty

            # ── GL Inventory Asset balance ────────────────────────────────
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

            # ── Variance calculation ──────────────────────────────────────
            layer_vs_movement = layer_value - movement_value
            layer_vs_gl = layer_value - gl_balance
            movement_vs_gl = movement_value - gl_balance

            # Reconciliation status
            tolerance = _dec("0.02")  # 2-cent rounding tolerance
            is_matched = (
                abs(layer_vs_movement) <= tolerance
                and abs(layer_vs_gl) <= tolerance
            )

            if is_matched:
                matched += 1
                if not mismatches_only:
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"  ✓  {comp_name} | {product.name} (id={product.id})\n"
                            f"       Layers={layer_value:>12.2f}  Movements={movement_value:>12.2f}  "
                            f"GL={gl_balance:>12.2f}  Qty={net_qty}"
                        )
                    )
            else:
                mismatched += 1
                self.stdout.write(
                    self.style.ERROR(
                        f"  ✗  {comp_name} | {product.name} (id={product.id})\n"
                        f"       Layers   = {layer_value:>12.2f}\n"
                        f"       Movements= {movement_value:>12.2f}  "
                        f"(Δ vs layers = {layer_vs_movement:+.2f})\n"
                        f"       GL Asset = {gl_balance:>12.2f}  "
                        f"(Δ vs layers = {layer_vs_gl:+.2f})\n"
                        f"       Net Qty  = {net_qty}"
                    )
                )
                # Try to identify source documents causing the mismatch
                self._report_mismatch_sources(product, company, as_of)

        # ── 2. Company-level GL summary ─────────────────────────────────────
        self.stdout.write(f"\n{'─'*70}")
        self._report_company_gl_summary(company_id, as_of)

        # ── 3. Summary ──────────────────────────────────────────────────────
        self.stdout.write(f"\n{'═'*70}")
        status_style = self.style.SUCCESS if mismatched == 0 else self.style.ERROR
        self.stdout.write(
            status_style(
                f"  Reconciliation Complete\n"
                f"  Total products checked : {total_products}\n"
                f"  Matched                : {matched}\n"
                f"  Mismatched             : {mismatched}\n"
                f"  Status                 : {'✓ RECONCILED' if mismatched == 0 else '✗ MISMATCHES FOUND'}\n"
            )
        )

        if mismatched > 0:
            self.stdout.write(
                self.style.WARNING(
                    "\nSuggested investigation steps:\n"
                    "  1. python manage.py verify_inventory_fifo --company <id>\n"
                    "  2. python manage.py rebuild_inventory_fifo --company <id>\n"
                    "  3. Check for invoices where COGS GL entry is missing\n"
                    "  4. Check for stock transfers with missing TRANSFER IN layers\n"
                    "  5. Re-run: python manage.py inventory_reconciliation_check\n"
                )
            )

    # ------------------------------------------------------------------
    def _report_mismatch_sources(self, product, company, as_of):
        """Print the top source documents contributing to the mismatch."""
        from inventory.models import InventoryMovement
        from django.db.models import Sum, Count

        mv_qs = InventoryMovement.objects.filter(
            product=product,
            date__lte=as_of,
        )
        if company:
            mv_qs = mv_qs.filter(company=company)

        # Group by source_type + source_id to spot patterns
        sources = (
            mv_qs.values("source_type", "source_id")
            .annotate(
                total_in=Sum("qty_in"),
                total_out=Sum("qty_out"),
                total_value_in=Sum("value", filter=Q(qty_in__gt=0)),
                total_value_out=Sum("value", filter=Q(qty_out__gt=0)),
                cnt=Count("id"),
            )
            .order_by("-cnt")[:10]
        )
        if sources:
            self.stdout.write(
                self.style.WARNING(
                    "       Top source documents (max 10):"
                )
            )
            for s in sources:
                net = _dec(s["total_in"] or 0) - _dec(s["total_out"] or 0)
                val_net = _dec(s["total_value_in"] or 0) - _dec(s["total_value_out"] or 0)
                self.stdout.write(
                    f"         {s['source_type']:15s} #{s['source_id']:6}  "
                    f"qty_net={net:+8.2f}  val_net={val_net:+12.2f}  rows={s['cnt']}"
                )

        # Check for movements without GL linkage (unposted)
        unposted = mv_qs.filter(is_gl_posted=False).count()
        if unposted:
            self.stdout.write(
                self.style.WARNING(
                    f"       ⚠  {unposted} movement(s) not linked to any GL entry"
                )
            )

    # ------------------------------------------------------------------
    def _report_company_gl_summary(self, company_id, as_of):
        """Report total Inventory Asset GL balance per company."""
        from accounts.models import Account, JournalLine

        self.stdout.write("  GL Inventory Asset balances by company:\n")

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
                f"    {comp_name:30s}  {acc.account_name:30s}  Balance={balance:>14.2f}\n"
            )
