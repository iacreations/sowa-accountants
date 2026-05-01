# inventory/management/commands/recalculate_inventory_cogs.py
"""
Management command: recalculate_inventory_cogs

For each posted invoice with inventory items, recalculates COGS using the
current FIFO layers and compares against the posted GL COGS. Identifies
variances and (in live mode) creates reversal + correction journal entries.

Usage:
    python manage.py recalculate_inventory_cogs
    python manage.py recalculate_inventory_cogs --dry-run
    python manage.py recalculate_inventory_cogs --company-id 1
    python manage.py recalculate_inventory_cogs --from-date 2026-01-01
    python manage.py recalculate_inventory_cogs --to-date 2026-12-31
"""
import time
from datetime import date as date_type
from decimal import Decimal, ROUND_HALF_UP

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models import Sum, Q
from django.utils import timezone

ZERO = Decimal("0.00")
_Q2 = Decimal("0.01")
VARIANCE_THRESHOLD = Decimal("0.01")


def _dec(v) -> Decimal:
    if v is None:
        return ZERO
    if isinstance(v, Decimal):
        return v
    return Decimal(str(v))


def _q2(v) -> Decimal:
    return _dec(v).quantize(_Q2, rounding=ROUND_HALF_UP)


class Command(BaseCommand):
    help = (
        "Recalculate COGS for posted invoices using current FIFO layers. "
        "Flags variances > 0.01 and creates reversal+correction journal entries. "
        "Dry-run mode reports what would change without writing to the database."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Show variances without creating any journal entries.",
        )
        parser.add_argument(
            "--company-id",
            dest="company_id",
            type=int,
            default=None,
            help="Limit to a specific company ID.",
        )
        parser.add_argument(
            "--from-date",
            dest="from_date",
            default=None,
            help="Only process invoices from this date (YYYY-MM-DD).",
        )
        parser.add_argument(
            "--to-date",
            dest="to_date",
            default=None,
            help="Only process invoices up to this date (YYYY-MM-DD).",
        )

    # ------------------------------------------------------------------
    def handle(self, *args, **options):
        from sales.models import Newinvoice, InvoiceItem

        dry_run = options["dry_run"]
        company_id = options["company_id"]
        from_date = self._parse_date(options.get("from_date"), "--from-date")
        to_date = self._parse_date(options.get("to_date"), "--to-date")

        if from_date and to_date and from_date > to_date:
            raise CommandError("--from-date must be before --to-date.")

        prefix = "[DRY RUN] " if dry_run else ""

        self.stdout.write(
            f"\n{'═' * 70}\n"
            f"  {prefix}Recalculate Inventory COGS\n"
            f"  Company   : {company_id or 'ALL'}\n"
            f"  From date : {from_date or 'beginning'}\n"
            f"  To date   : {to_date or 'today'}\n"
            f"{'═' * 70}\n"
        )

        # ── Load posted invoices ──────────────────────────────────────────
        invoice_qs = Newinvoice.objects.filter(
            is_posted=True,
            journal_entry__isnull=False,
        ).select_related("company", "customer", "journal_entry")

        if company_id:
            invoice_qs = invoice_qs.filter(company_id=company_id)
        if from_date:
            invoice_qs = invoice_qs.filter(date_created__date__gte=from_date)
        if to_date:
            invoice_qs = invoice_qs.filter(date_created__date__lte=to_date)

        total_invoices = invoice_qs.count()
        self.stdout.write(f"  Posted invoices to check: {total_invoices}\n")

        start_time = time.time()
        checked = 0
        ok_count = 0
        variance_count = 0
        correction_count = 0
        error_count = 0
        total_variance = ZERO

        variances_report = []

        for invoice in invoice_qs.iterator():
            try:
                result = self._check_invoice_cogs(invoice, dry_run=dry_run)
                checked += 1

                if result is None:
                    # No inventory lines — skip silently
                    continue

                variance = result["variance"]

                if abs(variance) <= VARIANCE_THRESHOLD:
                    ok_count += 1
                    continue

                # Variance found
                variance_count += 1
                total_variance += abs(variance)
                variances_report.append(result)

                self.stdout.write(
                    self.style.WARNING(
                        f"  ⚠ Invoice #{invoice.id}"
                        f" ({getattr(invoice, 'date_created', '?')}): "
                        f"posted_cogs={result['posted_cogs']:.2f}  "
                        f"recalc_cogs={result['recalc_cogs']:.2f}  "
                        f"variance={variance:+.2f}"
                    )
                )

                if not dry_run and result["correction_created"]:
                    correction_count += 1
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"    ✓ Correction JE created: reversal #{result['reversal_je_id']} "
                            f"+ correction #{result['correction_je_id']}"
                        )
                    )

            except Exception as exc:
                error_count += 1
                self.stderr.write(
                    self.style.ERROR(
                        f"  ✗ Invoice #{invoice.id}: {exc}"
                    )
                )

        elapsed = time.time() - start_time

        # ── Summary ───────────────────────────────────────────────────────
        self.stdout.write(f"\n{'─' * 70}")
        self.stdout.write(f"  Invoices checked    : {checked}")
        self.stdout.write(f"  OK (no variance)    : {ok_count}")
        self.stdout.write(f"  With variance       : {variance_count}")
        self.stdout.write(f"  Total variance      : {total_variance:.2f}")
        if not dry_run:
            self.stdout.write(f"  Corrections created : {correction_count}")
        self.stdout.write(f"  Errors              : {error_count}")
        self.stdout.write(f"  Elapsed             : {elapsed:.2f}s")

        self.stdout.write(f"\n{'═' * 70}")
        status_style = (
            self.style.SUCCESS if variance_count == 0 and error_count == 0
            else self.style.WARNING
        )
        self.stdout.write(
            status_style(
                f"  {prefix}COGS Reconciliation\n"
                f"  Status : "
                f"{'✓ ALL COGS MATCH' if variance_count == 0 else f'✗ {variance_count} VARIANCE(S) FOUND'}"
            )
        )

        if variances_report and dry_run:
            self.stdout.write(
                self.style.WARNING(
                    "\n  To create correction journal entries, re-run without --dry-run."
                )
            )

    # ------------------------------------------------------------------
    def _check_invoice_cogs(self, invoice, *, dry_run):
        """
        Compare posted COGS vs recalculated COGS for one invoice.
        Returns None if the invoice has no tracked inventory lines.
        """
        from sales.models import InvoiceItem
        from inventory.fifo import simulate_fifo_consumption
        from accounts.models import JournalLine, JournalEntry

        company = invoice.company
        je = invoice.journal_entry

        # ── Get inventory lines ───────────────────────────────────────────
        inv_lines = InvoiceItem.objects.filter(
            invoice=invoice,
            product__type="Inventory",
            product__track_inventory=True,
        ).select_related("product")

        if not inv_lines.exists():
            return None

        # ── COGS from GL (posted) ─────────────────────────────────────────
        cogs_accounts = set()
        for ln in inv_lines:
            p = ln.product
            cogs_acc = _get_cogs_account(p, company)
            if cogs_acc:
                cogs_accounts.add(cogs_acc.id)

        posted_cogs = ZERO
        if cogs_accounts and je:
            gl_cogs_agg = JournalLine.objects.filter(
                entry=je,
                account_id__in=cogs_accounts,
            ).aggregate(total_dr=Sum("debit"), total_cr=Sum("credit"))
            posted_cogs = _q2(gl_cogs_agg["total_dr"]) - _q2(gl_cogs_agg["total_cr"])

        # ── COGS recalculated using current FIFO layers ───────────────────
        recalc_cogs = ZERO
        for ln in inv_lines:
            product = ln.product
            qty = _q2(ln.qty)
            if qty <= ZERO:
                continue
            try:
                fifo_rows = simulate_fifo_consumption(product, qty)
                line_cogs = sum(
                    _q2(cost) * _q2(consumed_qty)
                    for cost, consumed_qty in fifo_rows
                )
                recalc_cogs += _q2(line_cogs)
            except ValueError:
                # Insufficient stock — can't recalculate this line
                pass

        recalc_cogs = _q2(recalc_cogs)
        variance = recalc_cogs - posted_cogs

        result = {
            "invoice_id": invoice.id,
            "posted_cogs": posted_cogs,
            "recalc_cogs": recalc_cogs,
            "variance": variance,
            "correction_created": False,
            "reversal_je_id": None,
            "correction_je_id": None,
        }

        # ── Create correction JEs if needed ───────────────────────────────
        if abs(variance) > VARIANCE_THRESHOLD and not dry_run:
            self._create_cogs_correction(invoice, posted_cogs, recalc_cogs, variance, result)

        return result

    def _create_cogs_correction(self, invoice, posted_cogs, recalc_cogs, variance, result):
        """
        Create a reversal JE and a correction JE for a COGS variance.
        Never overwrites the original — preserves full audit trail.
        """
        from accounts.models import JournalEntry, JournalLine
        from sales.models import InvoiceItem

        company = invoice.company
        post_date = timezone.localdate()

        inv_lines = InvoiceItem.objects.filter(
            invoice=invoice,
            product__type="Inventory",
            product__track_inventory=True,
        ).select_related("product")

        # Get accounts from first inventory line (best effort)
        cogs_acc = None
        inv_acc = None
        for ln in inv_lines:
            p = ln.product
            cogs_acc = cogs_acc or _get_cogs_account(p, company)
            inv_acc = inv_acc or _get_inv_asset_account(p, company)
            if cogs_acc and inv_acc:
                break

        if not cogs_acc or not inv_acc:
            return  # Can't create correction without accounts

        try:
            with transaction.atomic():
                # Step 1: Reversal JE (reverse the original COGS)
                reversal_je = JournalEntry.objects.create(
                    company=company,
                    date=post_date,
                    description=(
                        f"COGS Reversal — Invoice #{invoice.id} "
                        f"(posted={posted_cogs:.2f})"
                    ),
                    source_type="COGS_REVERSAL",
                    source_id=invoice.id,
                )
                # Reverse: CR COGS, DR Inventory Asset
                JournalLine.objects.create(
                    entry=reversal_je,
                    account=cogs_acc,
                    debit=ZERO,
                    credit=posted_cogs,
                )
                JournalLine.objects.create(
                    entry=reversal_je,
                    account=inv_acc,
                    debit=posted_cogs,
                    credit=ZERO,
                )

                # Step 2: Correction JE (post correct COGS)
                correction_je = JournalEntry.objects.create(
                    company=company,
                    date=post_date,
                    description=(
                        f"COGS Correction — Invoice #{invoice.id} "
                        f"(correct={recalc_cogs:.2f})"
                    ),
                    source_type="COGS_CORRECTION",
                    source_id=invoice.id,
                )
                JournalLine.objects.create(
                    entry=correction_je,
                    account=cogs_acc,
                    debit=recalc_cogs,
                    credit=ZERO,
                )
                JournalLine.objects.create(
                    entry=correction_je,
                    account=inv_acc,
                    debit=ZERO,
                    credit=recalc_cogs,
                )

                result["correction_created"] = True
                result["reversal_je_id"] = reversal_je.id
                result["correction_je_id"] = correction_je.id

        except Exception as exc:
            self.stderr.write(
                self.style.ERROR(
                    f"    ✗ Could not create correction for Invoice "
                    f"#{invoice.id}: {exc}"
                )
            )

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


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------
def _get_cogs_account(product, company=None):
    """Return the COGS account for a product (with fallback)."""
    from accounts.models import Account
    from inventory.accounting import _fallback_cogs_account
    return _fallback_cogs_account(product, company=company)


def _get_inv_asset_account(product, company=None):
    """Return the inventory asset account for a product (with fallback)."""
    from inventory.accounting import _fallback_inventory_asset_account
    return _fallback_inventory_asset_account(product, company=company)
