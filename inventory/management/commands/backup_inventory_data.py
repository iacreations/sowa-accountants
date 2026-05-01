# inventory/management/commands/backup_inventory_data.py
"""
Management command: backup_inventory_data

Exports inventory-related records to JSON backup files with a timestamp.

Usage:
    python manage.py backup_inventory_data
    python manage.py backup_inventory_data --company-id 1
    python manage.py backup_inventory_data --dry-run
    python manage.py backup_inventory_data --output-dir /path/to/backups
"""
import json
import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction


def _json_default(obj):
    """Custom JSON encoder for Decimal, date, and datetime types."""
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


class Command(BaseCommand):
    help = (
        "Export inventory data (movements, layers, GL entries, products) "
        "to timestamped JSON backup files. Safe, read-only, dry-run capable."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--company-id",
            dest="company_id",
            type=int,
            default=None,
            help="Limit export to a specific company ID.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Show what would be exported without writing any files.",
        )
        parser.add_argument(
            "--output-dir",
            dest="output_dir",
            default=None,
            help=(
                "Directory where backup files will be saved. "
                "Defaults to <BASE_DIR>/inventory_backups/."
            ),
        )

    def handle(self, *args, **options):
        from inventory.models import InventoryMovement, InventoryLayer, Product

        dry_run = options["dry_run"]
        company_id = options["company_id"]
        output_dir = options["output_dir"]

        prefix = "[DRY RUN] " if dry_run else ""

        # Determine output directory
        if output_dir:
            backup_dir = Path(output_dir)
        else:
            backup_dir = Path(settings.BASE_DIR) / "inventory_backups"

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_dir = backup_dir / timestamp

        self.stdout.write(
            f"\n{'═' * 70}\n"
            f"  {prefix}Inventory Data Backup\n"
            f"  Timestamp : {timestamp}\n"
            f"  Company   : {company_id or 'ALL'}\n"
            f"  Output    : {run_dir}\n"
            f"{'═' * 70}\n"
        )

        exported_paths = []

        try:
            with transaction.atomic():
                # ── 1. InventoryMovement ──────────────────────────────────
                mv_qs = InventoryMovement.objects.all().select_related(
                    "product", "company", "location", "gl_entry"
                )
                if company_id:
                    mv_qs = mv_qs.filter(company_id=company_id)

                movements = list(mv_qs.values(
                    "id", "company_id", "product_id", "location_id",
                    "date", "qty_in", "qty_out", "unit_cost", "value",
                    "source_type", "source_id",
                    "is_opening_balance", "gl_entry_id", "is_gl_posted",
                    "created_at",
                ))
                path = self._write_or_report(
                    run_dir, "inventory_movements.json", movements,
                    dry_run=dry_run, label="InventoryMovement",
                )
                exported_paths.append(path)

                # ── 2. InventoryLayer ─────────────────────────────────────
                layer_qs = InventoryLayer.objects.all().select_related(
                    "product", "company", "source_movement"
                )
                if company_id:
                    layer_qs = layer_qs.filter(company_id=company_id)

                layers = list(layer_qs.values(
                    "id", "company_id", "product_id", "source_movement_id",
                    "unit_cost", "qty_in", "qty_remaining",
                    "date_created", "is_exhausted",
                ))
                path = self._write_or_report(
                    run_dir, "inventory_layers.json", layers,
                    dry_run=dry_run, label="InventoryLayer",
                )
                exported_paths.append(path)

                # ── 3. JournalEntry (inventory-related) ───────────────────
                from accounts.models import JournalEntry

                je_qs = JournalEntry.objects.filter(
                    source_type__in=["INVOICE", "BILL", "EXPENSE", "ADJUSTMENT",
                                     "OPENING", "TRANSFER", "ASSEMBLY",
                                     "SALES_RECEIPT", "CHEQUE"]
                )
                if company_id:
                    je_qs = je_qs.filter(company_id=company_id)

                journal_entries = list(je_qs.values(
                    "id", "company_id", "date", "description",
                    "source_type", "source_id", "created_at",
                ))
                path = self._write_or_report(
                    run_dir, "journal_entries.json", journal_entries,
                    dry_run=dry_run, label="JournalEntry (inventory)",
                )
                exported_paths.append(path)

                # ── 4. JournalLine (inventory GL) ─────────────────────────
                from accounts.models import JournalLine

                je_ids = [row["id"] for row in journal_entries]
                jl_qs = JournalLine.objects.filter(entry_id__in=je_ids)

                journal_lines = list(jl_qs.values(
                    "id", "entry_id", "account_id", "debit", "credit",
                    "supplier_id", "customer_id",
                ))
                path = self._write_or_report(
                    run_dir, "journal_lines.json", journal_lines,
                    dry_run=dry_run, label="JournalLine (inventory GL)",
                )
                exported_paths.append(path)

                # ── 5. Product stock/cost/valuation fields ────────────────
                prod_qs = Product.objects.filter(
                    type="Inventory",
                    track_inventory=True,
                )
                if company_id:
                    prod_qs = prod_qs.filter(company_id=company_id)

                products = list(prod_qs.values(
                    "id", "company_id", "name", "sku", "type",
                    "quantity", "avg_cost",
                    "opening_stock_value", "opening_stock_date",
                    "valuation_method", "cut_off_date",
                    "inventory_asset_account_id", "cogs_account_id",
                    "income_account_id", "expense_account_id",
                    "track_inventory",
                ))
                path = self._write_or_report(
                    run_dir, "products.json", products,
                    dry_run=dry_run, label="Product (stock/cost/valuation)",
                )
                exported_paths.append(path)

                if dry_run:
                    transaction.set_rollback(True)

        except Exception as exc:
            raise CommandError(
                f"Backup failed: {exc}"
            ) from exc

        # ── Summary ───────────────────────────────────────────────────────
        self.stdout.write(f"\n{'═' * 70}")
        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    f"  [DRY RUN] No files written. "
                    f"Would have created {len(exported_paths)} backup file(s) in:\n"
                    f"  {run_dir}"
                )
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    f"  Backup complete. {len(exported_paths)} file(s) written to:\n"
                    f"  {run_dir}"
                )
            )
            for p in exported_paths:
                self.stdout.write(f"    {p}")

        return str(run_dir)

    # ------------------------------------------------------------------
    def _write_or_report(self, run_dir: Path, filename: str, records: list,
                         *, dry_run: bool, label: str) -> str:
        """Write records to JSON or just report count (dry-run)."""
        count = len(records)
        file_path = run_dir / filename

        if dry_run:
            self.stdout.write(
                f"  [DRY] {label}: {count} record(s) → {filename}"
            )
        else:
            run_dir.mkdir(parents=True, exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as fh:
                json.dump(records, fh, default=_json_default, indent=2, ensure_ascii=False)
            self.stdout.write(
                self.style.SUCCESS(
                    f"  ✓ {label}: {count} record(s) → {file_path}"
                )
            )

        return str(file_path)
