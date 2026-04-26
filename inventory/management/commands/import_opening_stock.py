# inventory/management/commands/import_opening_stock.py
"""
Management command: import_opening_stock

Import opening stock balances from a CSV file.

CSV format (header row required):
    product_id,quantity,unit_cost[,date]

Columns:
    product_id  - Integer PK of the Product, OR the product SKU (string)
    quantity    - Decimal quantity on hand
    unit_cost   - Decimal unit cost
    date        - Optional date in YYYY-MM-DD format (defaults to --date or today)

Usage:
    python manage.py import_opening_stock opening_stock.csv
    python manage.py import_opening_stock opening_stock.csv --date 2026-04-26
    python manage.py import_opening_stock opening_stock.csv --date 2026-04-26 --dry-run
    python manage.py import_opening_stock opening_stock.csv --company 1
"""
import csv
from datetime import date as date_type
from decimal import Decimal, InvalidOperation

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone

ZERO = Decimal("0.00")


class Command(BaseCommand):
    help = (
        "Import opening stock balances from a CSV file.  "
        "Creates OPENING InventoryMovement records and rebuilds FIFO layers."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "csv_file",
            help="Path to the CSV file containing opening stock data.",
        )
        parser.add_argument(
            "--date",
            default=None,
            help="Default cut-off date in YYYY-MM-DD format (used when CSV row has no date column).",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Validate CSV without writing to the database.",
        )
        parser.add_argument(
            "--company",
            type=int,
            default=None,
            help="Limit product lookups to a specific company ID.",
        )

    def handle(self, *args, **options):
        from inventory.models import Product, InventoryMovement, InventoryLayer
        from inventory.fifo import rebuild_layers_from_movements
        from inventory.services import get_default_location

        dry_run = options["dry_run"]
        company_id = options["company"]
        csv_path = options["csv_file"]

        # --- Default date ---
        if options["date"]:
            try:
                default_date = date_type.fromisoformat(options["date"])
            except ValueError:
                raise CommandError(
                    f"Invalid date format: '{options['date']}'. Use YYYY-MM-DD."
                )
        else:
            default_date = timezone.localdate()

        prefix = "[DRY RUN] " if dry_run else ""
        self.stdout.write(f"{prefix}Importing opening stock from: {csv_path}")
        self.stdout.write(f"{prefix}Default date: {default_date}")

        # --- Read CSV ---
        try:
            with open(csv_path, newline="", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
        except FileNotFoundError:
            raise CommandError(f"File not found: {csv_path}")
        except Exception as exc:
            raise CommandError(f"Cannot read CSV file: {exc}")

        if not rows:
            self.stdout.write(self.style.WARNING("CSV file is empty — nothing to import."))
            return

        self.stdout.write(f"{prefix}Found {len(rows)} row(s) in CSV.")

        # --- Validate and import each row ---
        imported = 0
        errors = 0
        affected_products = set()

        for row_num, row in enumerate(rows, start=2):  # start=2 accounts for header
            try:
                product_id_raw = (row.get("product_id") or "").strip()
                qty_raw = (row.get("quantity") or "").strip()
                cost_raw = (row.get("unit_cost") or "").strip()
                date_raw = (row.get("date") or "").strip()

                if not product_id_raw:
                    raise ValueError("product_id is required.")
                if not qty_raw:
                    raise ValueError("quantity is required.")
                if not cost_raw:
                    raise ValueError("unit_cost is required.")

                # Parse quantity and cost
                try:
                    quantity = Decimal(qty_raw)
                except InvalidOperation:
                    raise ValueError(f"Invalid quantity: '{qty_raw}'.")

                try:
                    unit_cost = Decimal(cost_raw)
                except InvalidOperation:
                    raise ValueError(f"Invalid unit_cost: '{cost_raw}'.")

                if quantity <= ZERO:
                    raise ValueError(f"quantity must be greater than zero (got {quantity}).")
                if unit_cost <= ZERO:
                    raise ValueError(f"unit_cost must be greater than zero (got {unit_cost}).")

                # Parse date
                row_date = default_date
                if date_raw:
                    try:
                        row_date = date_type.fromisoformat(date_raw)
                    except ValueError:
                        raise ValueError(f"Invalid date: '{date_raw}'. Use YYYY-MM-DD.")

                # Resolve product (by PK or SKU)
                product_qs = Product.objects.all()
                if company_id:
                    product_qs = product_qs.filter(company_id=company_id)

                if product_id_raw.isdigit():
                    product = product_qs.filter(id=int(product_id_raw)).first()
                    if not product:
                        raise ValueError(
                            f"Product with id={product_id_raw} not found"
                            + (f" in company {company_id}" if company_id else "") + "."
                        )
                else:
                    # Try by SKU
                    product = product_qs.filter(sku=product_id_raw).first()
                    if not product:
                        raise ValueError(
                            f"Product with SKU='{product_id_raw}' not found"
                            + (f" in company {company_id}" if company_id else "") + "."
                        )

                if dry_run:
                    self.stdout.write(
                        f"  [DRY] Row {row_num}: {product.name} (id={product.id}) — "
                        f"qty={quantity}, unit_cost={unit_cost}, date={row_date}"
                    )
                    imported += 1
                    continue

                # --- Write to database ---
                company = getattr(product, "company", None)

                with transaction.atomic():
                    # Remove existing OPENING movement for this product+date
                    InventoryMovement.objects.filter(
                        product=product,
                        source_type="OPENING",
                        source_id=product.id,
                        date=row_date,
                    ).delete()

                    loc = get_default_location(company=company)
                    mv = InventoryMovement(
                        product=product,
                        location=loc,
                        date=row_date,
                        qty_in=quantity,
                        qty_out=ZERO,
                        unit_cost=unit_cost,
                        value=quantity * unit_cost,
                        source_type="OPENING",
                        source_id=product.id,
                        is_opening_balance=True,
                    )
                    if company is not None:
                        mv.company = company
                    mv.save()

                    affected_products.add(product.id)

                self.stdout.write(
                    self.style.SUCCESS(
                        f"  ✓ Row {row_num}: {product.name} (id={product.id}) — "
                        f"qty={quantity}, unit_cost={unit_cost}, date={row_date}"
                    )
                )
                imported += 1

            except Exception as exc:  # noqa: BLE001
                errors += 1
                self.stderr.write(
                    self.style.ERROR(f"  ✗ Row {row_num}: {exc}")
                )

        # --- Rebuild FIFO layers for affected products ---
        if not dry_run and affected_products:
            self.stdout.write("\nRebuilding FIFO layers for affected products…")
            for pid in affected_products:
                try:
                    product = Product.objects.get(id=pid)
                    company = getattr(product, "company", None)
                    cut_off = product.cut_off_date
                    rebuild_layers_from_movements(product, company=company, from_date=cut_off)

                    # Recalculate product.quantity
                    from django.db.models import Sum
                    base_qs = product.movements
                    if cut_off:
                        base_qs = base_qs.filter(date__gte=cut_off)
                    agg = base_qs.aggregate(tin=Sum("qty_in"), tout=Sum("qty_out"))
                    product.quantity = (agg["tin"] or ZERO) - (agg["tout"] or ZERO)
                    product.save(update_fields=["quantity"])

                    layer_count = InventoryLayer.objects.filter(product=product).count()
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"  ✓ {product.name} (id={product.id}): "
                            f"{layer_count} FIFO layer(s)"
                        )
                    )
                except Exception as exc:  # noqa: BLE001
                    self.stderr.write(
                        self.style.ERROR(f"  ✗ Product id={pid}: {exc}")
                    )

        summary_style = self.style.SUCCESS if errors == 0 else self.style.WARNING
        self.stdout.write(
            summary_style(
                f"\n{prefix}Done. Imported={imported}, Errors={errors}"
            )
        )
