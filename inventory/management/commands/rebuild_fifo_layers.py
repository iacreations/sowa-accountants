# inventory/management/commands/rebuild_fifo_layers.py
"""
Management command: rebuild_fifo_layers

Safely clears and rebuilds FIFO inventory layers from the InventoryMovement
ledger. Supports backup verification, dry-run mode, and post-rebuild validation.

Usage:
    python manage.py rebuild_fifo_layers
    python manage.py rebuild_fifo_layers --dry-run
    python manage.py rebuild_fifo_layers --company-id 1
    python manage.py rebuild_fifo_layers --product-id 5
    python manage.py rebuild_fifo_layers --from-date 2026-01-01
    python manage.py rebuild_fifo_layers --to-date 2026-12-31
    python manage.py rebuild_fifo_layers --force  (skip backup check)
"""
import time
from datetime import date as date_type
from decimal import Decimal
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models import Sum

ZERO = Decimal("0.00")
_Q2 = Decimal("0.01")


def _dec(v) -> Decimal:
    if v is None:
        return ZERO
    return Decimal(str(v))


class Command(BaseCommand):
    help = (
        "Rebuild FIFO inventory layers from InventoryMovement ledger. "
        "Validates backup, replays movements chronologically, and verifies "
        "post-rebuild integrity. Dry-run and company/product/date filters supported."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Simulate rebuild without writing to the database.",
        )
        parser.add_argument(
            "--company-id",
            dest="company_id",
            type=int,
            default=None,
            help="Limit rebuild to a specific company ID.",
        )
        parser.add_argument(
            "--product-id",
            dest="product_id",
            type=int,
            default=None,
            help="Limit rebuild to a specific product ID.",
        )
        parser.add_argument(
            "--from-date",
            dest="from_date",
            default=None,
            help="Only replay movements on or after this date (YYYY-MM-DD).",
        )
        parser.add_argument(
            "--to-date",
            dest="to_date",
            default=None,
            help="Only replay movements up to and including this date (YYYY-MM-DD).",
        )
        parser.add_argument(
            "--force",
            action="store_true",
            default=False,
            help="Skip backup-existence check and run without confirmation.",
        )

    # ------------------------------------------------------------------
    def handle(self, *args, **options):
        from inventory.models import Product, InventoryLayer, InventoryMovement
        from inventory.services import PURCHASE_SOURCE_TYPES

        dry_run = options["dry_run"]
        company_id = options["company_id"]
        product_id = options["product_id"]
        force = options["force"]

        from_date = self._parse_date(options.get("from_date"), "--from-date")
        to_date = self._parse_date(options.get("to_date"), "--to-date")

        if from_date and to_date and from_date > to_date:
            raise CommandError("--from-date must be before --to-date.")

        prefix = "[DRY RUN] " if dry_run else ""

        self.stdout.write(
            f"\n{'═' * 70}\n"
            f"  {prefix}Rebuild FIFO Layers\n"
            f"  Company   : {company_id or 'ALL'}\n"
            f"  Product   : {product_id or 'ALL'}\n"
            f"  From date : {from_date or 'beginning'}\n"
            f"  To date   : {to_date or 'today'}\n"
            f"{'═' * 70}\n"
        )

        # ── Backup check ──────────────────────────────────────────────────
        if not dry_run and not force:
            self._check_backup_exists()

        # ── Load products ─────────────────────────────────────────────────
        product_qs = Product.objects.filter(
            type="Inventory",
            track_inventory=True,
        )
        if company_id:
            product_qs = product_qs.filter(company_id=company_id)
        if product_id:
            product_qs = product_qs.filter(id=product_id)

        total_products = product_qs.count()
        self.stdout.write(f"  Products to process: {total_products}\n")

        # ── Pre-execution state ───────────────────────────────────────────
        layer_count_before = InventoryLayer.objects.filter(
            product__in=product_qs,
        ).count()
        self.stdout.write(f"  FIFO layers before  : {layer_count_before}\n")

        start_time = time.time()
        rebuilt = 0
        errors = 0
        mismatches = []
        total_layers_created = 0

        for product in product_qs.iterator():
            try:
                result = self._rebuild_product(
                    product,
                    from_date=from_date,
                    to_date=to_date,
                    dry_run=dry_run,
                    purchase_source_types=PURCHASE_SOURCE_TYPES,
                )
                rebuilt += 1
                total_layers_created += result["layers_created"]

                if result["qty_mismatch"]:
                    mismatches.append({
                        "product": product,
                        "fifo_qty": result["fifo_qty"],
                        "movement_qty": result["movement_qty"],
                    })
                    self.stdout.write(
                        self.style.WARNING(
                            f"  ⚠ {product.name} (id={product.id}): "
                            f"FIFO qty={result['fifo_qty']} ≠ "
                            f"movement qty={result['movement_qty']}"
                        )
                    )
                else:
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"  ✓ {product.name} (id={product.id}): "
                            f"{result['layers_created']} layer(s) created, "
                            f"qty={result['fifo_qty']}"
                        )
                    )

            except Exception as exc:
                errors += 1
                self.stderr.write(
                    self.style.ERROR(
                        f"  ✗ {product.name} (id={product.id}): {exc}"
                    )
                )

        elapsed = time.time() - start_time

        # ── Post-execution state ──────────────────────────────────────────
        layer_count_after = InventoryLayer.objects.filter(
            product__in=product_qs,
        ).count() if not dry_run else total_layers_created

        self.stdout.write(f"\n{'─' * 70}")
        self.stdout.write(f"  FIFO layers after   : {layer_count_after}")
        self.stdout.write(f"  Elapsed time        : {elapsed:.2f}s")

        # ── Summary ───────────────────────────────────────────────────────
        self.stdout.write(f"\n{'═' * 70}")
        status_style = self.style.SUCCESS if (errors == 0 and not mismatches) else self.style.WARNING
        self.stdout.write(
            status_style(
                f"  {prefix}Rebuild complete\n"
                f"  Products processed : {rebuilt}\n"
                f"  Errors             : {errors}\n"
                f"  Qty mismatches     : {len(mismatches)}\n"
                f"  Status             : "
                f"{'✓ OK' if errors == 0 and not mismatches else '✗ ISSUES FOUND'}"
            )
        )

        if mismatches:
            self.stdout.write(self.style.ERROR("\n  Qty mismatches (product | fifo_qty | movement_qty):"))
            for m in mismatches:
                self.stdout.write(
                    f"    {m['product'].name} (id={m['product'].id}): "
                    f"fifo={m['fifo_qty']}  movement={m['movement_qty']}"
                )
            self.stdout.write(
                self.style.WARNING(
                    "\n  Suggestion: Run 'recalculate_inventory_balances' to correct "
                    "Product.quantity fields."
                )
            )

    # ------------------------------------------------------------------
    def _rebuild_product(self, product, *, from_date, to_date, dry_run,
                         purchase_source_types):
        """
        Rebuild FIFO layers for a single product.
        Returns a dict with: layers_created, fifo_qty, movement_qty, qty_mismatch.
        """
        from inventory.models import InventoryLayer, InventoryMovement
        from inventory.fifo import rebuild_layers_from_movements

        company = getattr(product, "company", None)

        # Fall back to product.cut_off_date if no --from-date given
        effective_from_date = from_date
        if effective_from_date is None:
            effective_from_date = getattr(product, "cut_off_date", None)

        # Calculate expected qty from movements (for validation)
        mv_qs = InventoryMovement.objects.filter(product=product)
        if effective_from_date:
            mv_qs = mv_qs.filter(date__gte=effective_from_date)
        if to_date:
            mv_qs = mv_qs.filter(date__lte=to_date)

        agg = mv_qs.aggregate(
            total_in=Sum("qty_in"),
            total_out=Sum("qty_out"),
        )
        movement_qty = _dec(agg["total_in"]) - _dec(agg["total_out"])

        if dry_run:
            # Simulate in-memory without DB changes
            return self._simulate_rebuild(
                product, mv_qs, movement_qty,
                purchase_source_types=purchase_source_types,
            )

        with transaction.atomic():
            rebuild_layers_from_movements(
                product,
                company=company,
                from_date=effective_from_date,
            )

            # Count layers created
            layers_created = InventoryLayer.objects.filter(product=product).count()

            # Verify: FIFO total qty_remaining == movement_qty
            fifo_agg = InventoryLayer.objects.filter(
                product=product,
                is_exhausted=False,
            ).aggregate(total=Sum("qty_remaining"))
            fifo_qty = _dec(fifo_agg["total"])

        qty_mismatch = abs(fifo_qty - movement_qty) > _dec("0.02")

        return {
            "layers_created": layers_created,
            "fifo_qty": fifo_qty,
            "movement_qty": movement_qty,
            "qty_mismatch": qty_mismatch,
        }

    def _simulate_rebuild(self, product, mv_qs, movement_qty, *, purchase_source_types):
        """In-memory FIFO simulation for dry-run mode."""
        TRANSFER = "TRANSFER"
        pending = []

        for mv in mv_qs.order_by("date", "id"):
            qty_in = _dec(mv.qty_in)
            qty_out = _dec(mv.qty_out)
            unit_cost = _dec(mv.unit_cost)
            source_type = mv.source_type or ""

            is_purchase_in = qty_in > ZERO and source_type in purchase_source_types
            is_transfer_in = qty_in > ZERO and source_type == TRANSFER and unit_cost > ZERO

            if is_purchase_in or is_transfer_in:
                pending.append({
                    "unit_cost": unit_cost,
                    "qty_in": qty_in,
                    "qty_remaining": qty_in,
                    "date": mv.date,
                })
            elif qty_out > ZERO:
                remaining = qty_out
                for layer in pending:
                    if remaining <= ZERO:
                        break
                    available = _dec(layer["qty_remaining"])
                    if available <= ZERO:
                        continue
                    take = min(available, remaining)
                    layer["qty_remaining"] = available - take
                    remaining -= take

        layers_created = len(pending)
        fifo_qty = sum(_dec(l["qty_remaining"]) for l in pending if _dec(l["qty_remaining"]) > ZERO)
        qty_mismatch = abs(fifo_qty - movement_qty) > _dec("0.02")

        self.stdout.write(
            f"  [DRY] {product.name} (id={product.id}): "
            f"{layers_created} layer(s), fifo_qty={fifo_qty}, "
            f"movement_qty={movement_qty}"
        )

        return {
            "layers_created": layers_created,
            "fifo_qty": fifo_qty,
            "movement_qty": movement_qty,
            "qty_mismatch": qty_mismatch,
        }

    # ------------------------------------------------------------------
    def _check_backup_exists(self):
        """
        Warn the user if no recent backup directory is found.
        This is a soft check — it won't block execution, but will prompt.
        """
        from django.conf import settings

        backup_dir = Path(settings.BASE_DIR) / "inventory_backups"
        if not backup_dir.exists() or not any(backup_dir.iterdir()):
            self.stdout.write(
                self.style.WARNING(
                    "\n  ⚠ WARNING: No backup found in:\n"
                    f"    {backup_dir}\n\n"
                    "  It is strongly recommended to run backup_inventory_data first:\n"
                    "    python manage.py backup_inventory_data\n\n"
                    "  Continue anyway? [y/N] "
                ),
                ending="",
            )
            self.stdout.flush()
            answer = input().strip().lower()
            if answer not in ("y", "yes"):
                raise CommandError("Aborted by user (no backup found).")
        else:
            latest = max(backup_dir.iterdir(), key=lambda p: p.stat().st_mtime)
            self.stdout.write(
                self.style.SUCCESS(
                    f"  ✓ Backup found: {latest}\n"
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
