# inventory/management/commands/audit_inventory_changes.py
"""
Management command: audit_inventory_changes

Reviews inventory movement and GL posting activity over a configurable
lookback window.  Designed for monthly audit trail review.

Reports:
  1. InventoryMovements without a linked GL entry (where expected)
  2. GL journal entries sourced from INVOICE/BILL with no corresponding movements
  3. Products whose cached quantity differs from movement sum
  4. Summary counts of movements per source_type

Usage:
    python manage.py audit_inventory_changes
    python manage.py audit_inventory_changes --company-id 1
    python manage.py audit_inventory_changes --monthly
    python manage.py audit_inventory_changes --days 30
    python manage.py audit_inventory_changes --since 2026-01-01
"""
import time
from datetime import date as date_type, timedelta
from decimal import Decimal, ROUND_HALF_UP

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Sum, Count, Q

ZERO = Decimal("0.00")
_Q2 = Decimal("0.01")
DEFAULT_LOOKBACK_DAYS = 30


def _dec(v) -> Decimal:
    if v is None:
        return ZERO
    return Decimal(str(v)).quantize(_Q2, rounding=ROUND_HALF_UP)


class Command(BaseCommand):
    help = (
        "Audit inventory movement and GL posting activity.  Identifies movements "
        "missing GL entries, orphaned GL entries, quantity cache drift, and "
        "provides a per-source-type activity summary."
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
            "--monthly",
            action="store_true",
            default=False,
            help="Monthly mode: emit a condensed summary line suitable for automated reporting.",
        )
        parser.add_argument(
            "--days",
            dest="days",
            type=int,
            default=DEFAULT_LOOKBACK_DAYS,
            help=f"Lookback window in days. Default: {DEFAULT_LOOKBACK_DAYS}.",
        )
        parser.add_argument(
            "--since",
            dest="since",
            default=None,
            help="Review movements from this date (YYYY-MM-DD). Overrides --days.",
        )

    # ------------------------------------------------------------------
    def handle(self, *args, **options):
        from inventory.models import Product, InventoryMovement
        from accounts.models import JournalEntry

        company_id = options["company_id"]
        monthly = options["monthly"]

        # Determine lookback window
        today = date_type.today()
        if options.get("since"):
            try:
                since = date_type.fromisoformat(options["since"])
            except ValueError:
                raise CommandError(
                    f"Invalid date '{options['since']}'. Use YYYY-MM-DD."
                )
        else:
            since = today - timedelta(days=options["days"])

        self.stdout.write(
            f"\n{'═' * 70}\n"
            f"  Inventory Audit Trail Review\n"
            f"  Period    : {since} → {today}\n"
            f"  Company   : {company_id or 'ALL'}\n"
            f"{'═' * 70}\n"
        )

        start_time = time.time()

        movement_qs = InventoryMovement.objects.filter(date__gte=since)
        if company_id:
            movement_qs = movement_qs.filter(company_id=company_id)

        # ── 1. Movements missing GL entry (for INVOICE/BILL source types) ──
        self.stdout.write("  [1] Invoice/Bill movements missing GL entry:")
        gl_required_types = ("INVOICE", "BILL")
        missing_gl = movement_qs.filter(
            source_type__in=gl_required_types,
            is_gl_posted=False,
        ).select_related("product", "company")

        if missing_gl.exists():
            for mv in missing_gl[:20]:
                self.stdout.write(
                    self.style.WARNING(
                        f"    ⚠ Movement #{mv.id} | {mv.source_type} #{mv.source_id} | "
                        f"product={mv.product} | date={mv.date} | "
                        f"qty_in={mv.qty_in} qty_out={mv.qty_out}"
                    )
                )
            if missing_gl.count() > 20:
                self.stdout.write(
                    self.style.WARNING(f"    ... and {missing_gl.count() - 20} more.")
                )
        else:
            self.stdout.write(self.style.SUCCESS("    ✓ None found."))

        # ── 2. Activity summary by source_type ────────────────────────────
        self.stdout.write("\n  [2] Movement activity by source_type:")
        activity = (
            movement_qs.values("source_type")
            .annotate(
                count=Count("id"),
                total_in=Sum("qty_in"),
                total_out=Sum("qty_out"),
                total_value_in=Sum("value", filter=Q(qty_in__gt=0)),
                total_value_out=Sum("value", filter=Q(qty_out__gt=0)),
            )
            .order_by("-count")
        )
        if activity.exists():
            col_w = 12
            self.stdout.write(
                f"    {'Source':<20} {'Count':>{col_w}} {'Qty In':>{col_w}} "
                f"{'Qty Out':>{col_w}} {'Value In':>{col_w}} {'Value Out':>{col_w}}"
            )
            self.stdout.write(f"    {'─' * (20 + col_w * 5 + 5)}")
            for row in activity:
                self.stdout.write(
                    f"    {str(row['source_type'] or '—'):<20} "
                    f"{row['count']:>{col_w}} "
                    f"{_dec(row['total_in']):>{col_w}.2f} "
                    f"{_dec(row['total_out']):>{col_w}.2f} "
                    f"{_dec(row['total_value_in']):>{col_w}.2f} "
                    f"{_dec(row['total_value_out']):>{col_w}.2f}"
                )
        else:
            self.stdout.write("    No movements in this period.")

        # ── 3. Product quantity cache drift ───────────────────────────────
        self.stdout.write("\n  [3] Products with cached quantity drift:")
        product_qs = Product.objects.filter(type="Inventory")
        if company_id:
            product_qs = product_qs.filter(company_id=company_id)

        drifted = 0
        for product in product_qs.select_related("company"):
            agg = product.movements.aggregate(tin=Sum("qty_in"), tout=Sum("qty_out"))
            expected_qty = _dec(agg["tin"]) - _dec(agg["tout"])
            cached_qty = _dec(product.quantity)
            if abs(expected_qty - cached_qty) > Decimal("0.01"):
                drifted += 1
                self.stdout.write(
                    self.style.WARNING(
                        f"    ⚠ {product} | cached={cached_qty:.2f} | "
                        f"actual={expected_qty:.2f} | "
                        f"drift={abs(expected_qty - cached_qty):.2f}"
                    )
                )

        if drifted == 0:
            self.stdout.write(self.style.SUCCESS("    ✓ No quantity cache drift found."))

        # ── 4. GL journal entries without any movements ───────────────────
        self.stdout.write("\n  [4] GL journal entries (INVOICE/BILL) with no movements:")
        invoice_je_qs = JournalEntry.objects.filter(
            source_type__in=gl_required_types,
            date__gte=since,
        )
        if company_id:
            invoice_je_qs = invoice_je_qs.filter(company_id=company_id)

        orphaned_je = 0
        for je in invoice_je_qs.select_related("company"):
            has_movements = InventoryMovement.objects.filter(
                source_type=je.source_type, source_id=je.source_id,
            ).exists()
            if not has_movements:
                orphaned_je += 1
                self.stdout.write(
                    self.style.WARNING(
                        f"    ⚠ JournalEntry #{je.id} | {je.source_type} #{je.source_id} | "
                        f"date={je.date} | company={getattr(je, 'company', '—')}"
                    )
                )

        if orphaned_je == 0:
            self.stdout.write(self.style.SUCCESS("    ✓ No orphaned GL entries found."))

        elapsed = time.time() - start_time
        total_issues = missing_gl.count() + drifted + orphaned_je

        # ── Summary ────────────────────────────────────────────────────────
        self.stdout.write(f"\n{'═' * 70}")
        all_ok = total_issues == 0
        status_style = self.style.SUCCESS if all_ok else self.style.ERROR
        self.stdout.write(
            status_style(
                f"  Audit Complete\n"
                f"  Movements missing GL   : {missing_gl.count()}\n"
                f"  Qty cache drift        : {drifted}\n"
                f"  Orphaned GL entries    : {orphaned_je}\n"
                f"  Total issues           : {total_issues}\n"
                f"  Elapsed                : {elapsed:.2f}s\n"
                f"  Status                 : {'✓ CLEAN' if all_ok else '✗ ISSUES FOUND'}"
            )
        )

        if not all_ok:
            self.stdout.write(
                self.style.WARNING(
                    "\n  Remediation steps:\n"
                    "  1. python manage.py reconcile_inventory_gl --company-id <id>\n"
                    "  2. python manage.py rebuild_fifo_layers --company <id>\n"
                    "  3. python manage.py recalculate_inventory_balances\n"
                    "  4. Review and re-post any flagged invoices/bills.\n"
                )
            )

        if monthly:
            status_token = "CLEAN" if all_ok else "ISSUES"
            self.stdout.write(
                f"[MONTHLY] {since}..{today} | company={company_id or 'ALL'} | "
                f"missing_gl={missing_gl.count()} | qty_drift={drifted} | "
                f"orphaned_je={orphaned_je} | total_issues={total_issues} | "
                f"status={status_token}"
            )
