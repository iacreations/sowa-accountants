# inventory/assembly_engine.py
"""
Assembly Engine — production-grade service layer for the Assemblies module.

Implements the full lifecycle:
  - load_bom_into_build      : Populate BuildLines from a BOM
  - complete_assembly        : 2-step GL posting + stock movements
  - cancel_assembly          : Cancel a DRAFT/IN_PROGRESS build
  - reverse_assembly         : Reverse a COMPLETED build (undo GL + stock)

Accounting rules on completion:
  Step 1 — Consume Raw Materials:
    Dr Work In Progress (WIP)
    Cr Component Inventory Asset (per component, per FIFO layer)

  Step 2 — Complete Production:
    Dr Finished Goods Inventory
    Cr Work In Progress

Cost = SUM of all consumed components (FIFO valuation)
"""

from __future__ import annotations

import csv
import io
import logging
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

from django.db import transaction
from django.utils import timezone

logger = logging.getLogger(__name__)

ZERO = Decimal("0.00")
_Q2 = Decimal("0.01")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _dec(v) -> Decimal:
    if v is None:
        return ZERO
    if isinstance(v, Decimal):
        return v
    try:
        return Decimal(str(v))
    except Exception:
        return ZERO


def _q2(v) -> Decimal:
    return _dec(v).quantize(_Q2, rounding=ROUND_HALF_UP)


# ---------------------------------------------------------------------------
# WIP Account helpers
# ---------------------------------------------------------------------------

def get_or_create_wip_account(company):
    """
    Return the Work In Progress (WIP) inventory account for a company.

    Search order:
      1) Account named exactly "Work In Progress"
      2) Account name contains "Work In Progress" or "WIP"
      3) Any active CURRENT_ASSET account named "Inventory" (fallback)
      4) Auto-create a WIP account under CURRENT_ASSET
    """
    from accounts.models import Account

    company_id = getattr(company, "id", None)

    def _qs(term):
        qs = Account.objects.filter(is_active=True, account_name__icontains=term)
        if company_id:
            qs = qs.filter(company_id=company_id)
        return qs

    acc = _qs("Work In Progress").first() or _qs("WIP").first()
    if acc:
        return acc

    # Fallback: create it
    kwargs = dict(
        account_name="Work In Progress",
        account_number="1410",
        account_type="CURRENT_ASSET",
        detail_type="Work In Progress (WIP)",
        is_subaccount=False,
        parent=None,
        opening_balance=ZERO,
        as_of=timezone.localdate(),
        is_active=True,
    )
    if company_id:
        kwargs["company_id"] = company_id
    return Account.objects.create(**kwargs)


# ---------------------------------------------------------------------------
# BOM → Build
# ---------------------------------------------------------------------------

def load_bom_into_build(build, bom=None):
    """
    Populate (or replace) the BuildLines for *build* using the given *bom*.

    If *bom* is None, the build's own ``build.bom`` FK is used.
    Any existing BuildLines for this build are replaced.

    Args:
        build: Build instance (must be DRAFT/PENDING, not COMPLETED).
        bom: Optional BillOfMaterials instance. Defaults to build.bom.

    Returns:
        List of newly created BuildLine instances.
    """
    from inventory.models import BuildLine

    bom = bom or build.bom
    if not bom:
        raise ValueError("No BOM specified and build.bom is not set.")

    if build.status in ("COMPLETED", "CANCELLED"):
        raise ValueError(f"Cannot modify build {build.assembly_number}: status is {build.status}.")

    with transaction.atomic():
        build.lines.all().delete()

        new_lines = []
        for item in bom.items.select_related("component_item").all():
            new_lines.append(BuildLine(
                build=build,
                component=item.component_item,
                qty_per_unit=item.quantity_required,
            ))

        created = BuildLine.objects.bulk_create(new_lines)
        if bom.id and build.bom_id != bom.id:
            build.bom = bom
            build.save(update_fields=["bom"])

    return created


# ---------------------------------------------------------------------------
# Complete Assembly (2-step WIP accounting)
# ---------------------------------------------------------------------------

@transaction.atomic
def complete_assembly(build, completed_by=None):
    """
    Complete an assembly build with full double-entry accounting.

    Step 1: DR WIP  / CR Raw-Material Inventory Assets  (per FIFO layer per component)
    Step 2: DR Finished Goods  / CR WIP

    Stock Movements:
      - qty_out per component (source_type = ASSEMBLY)
      - qty_in  for finished product (source_type = ASSEMBLY)

    Args:
        build: Build instance in DRAFT, PENDING, or IN_PROGRESS status.
        completed_by: Optional user who is completing the build.

    Returns:
        The updated Build instance.
    """
    from inventory.models import BuildLine, InventoryMovement
    from inventory.fifo import simulate_fifo_consumption, rebuild_layers_from_movements
    from inventory.accounting import (
        _create_journal_entry, _add_line,
        _fallback_inventory_asset_account, _clear_inventory_movements,
        _delete_journal_entry_if_exists,
    )
    from accounts.models import JournalEntry
    from django.db.models import Sum

    if build.status == "COMPLETED":
        raise ValueError(f"Build {build.assembly_number} is already completed.")
    if build.status == "CANCELLED":
        raise ValueError(f"Build {build.assembly_number} is cancelled and cannot be completed.")

    company = build.company
    source_type = "ASSEMBLY"
    source_id = build.id
    post_date = build.build_date or timezone.localdate()
    location = build.location

    # Use default location if none specified
    if location is None:
        from inventory.services import get_default_location
        location = get_default_location(company=company)

    # Clear any previous movements and journal entries (idempotent)
    _clear_inventory_movements(source_type, source_id)
    _delete_journal_entry_if_exists(build)
    if build.wip_journal_entry_id:
        try:
            build.wip_journal_entry.delete()
        except Exception:
            pass
        build.wip_journal_entry = None

    wip_account = get_or_create_wip_account(company)

    build_qty = _q2(build.build_qty)
    total_component_cost = ZERO

    lines = list(BuildLine.objects.filter(build=build).select_related("component").all())
    if not lines:
        raise ValueError(f"Build {build.assembly_number} has no component lines.")

    # -----------------------------------------------------------------------
    # Step 1: Consume raw materials  → DR WIP / CR Component Inv Assets
    # -----------------------------------------------------------------------
    wip_je = _create_journal_entry(
        date=post_date,
        description=f"Assembly {build.assembly_number} – Step 1: Raw Materials → WIP",
        source_type=source_type,
        source_id=source_id,
    )
    if hasattr(wip_je, "company_id"):
        wip_je.company_id = getattr(company, "id", company)
        wip_je.save(update_fields=["company"])

    step1_total = ZERO

    for line in lines:
        component = line.component
        qty_consumed = _q2(_dec(line.qty_per_unit) * build_qty)
        if qty_consumed <= ZERO:
            continue

        # FIFO cost simulation (read-only — we create movements below)
        try:
            fifo_rows = simulate_fifo_consumption(component, qty_consumed)
        except ValueError as exc:
            raise ValueError(
                f"Insufficient stock for component '{component.name}': {exc}"
            ) from exc

        comp_inv_acc = _fallback_inventory_asset_account(component, company=company)
        if not comp_inv_acc:
            raise ValueError(
                f"Component '{component.name}' has no inventory asset account "
                "and no fallback account was found."
            )

        component_cost = ZERO
        for layer_cost, layer_qty in fifo_rows:
            layer_value = _q2(layer_qty * layer_cost)
            component_cost += layer_value

            # Stock OUT movement for component
            InventoryMovement.objects.create(
                product=component,
                company=company,
                date=post_date,
                qty_in=ZERO,
                qty_out=layer_qty,
                unit_cost=layer_cost,
                value=layer_value,
                location=location,
                source_type=source_type,
                source_id=source_id,
                gl_entry=wip_je,
                is_gl_posted=True,
            )

            # CR Component Inventory Asset
            _add_line(je=wip_je, account=comp_inv_acc, credit=layer_value)

        # Rebuild component stock after consuming
        agg = component.movements.aggregate(tin=Sum("qty_in"), tout=Sum("qty_out"))
        component.quantity = _dec(agg["tin"]) - _dec(agg["tout"])
        component.save(update_fields=["quantity"])
        rebuild_layers_from_movements(component, company=company)

        step1_total += component_cost

    if step1_total <= ZERO:
        # Zero-cost build — skip GL, just move stock
        wip_je.delete()
        finished = build.finished_product
        unit_cost_fg = ZERO

        InventoryMovement.objects.create(
            product=finished,
            company=company,
            date=post_date,
            qty_in=build_qty,
            qty_out=ZERO,
            unit_cost=unit_cost_fg,
            value=ZERO,
            location=location,
            source_type=source_type,
            source_id=source_id,
        )

        agg = finished.movements.aggregate(tin=Sum("qty_in"), tout=Sum("qty_out"))
        finished.quantity = _dec(agg["tin"]) - _dec(agg["tout"])
        finished.save(update_fields=["quantity"])
        rebuild_layers_from_movements(finished, company=company)

        build.status = "COMPLETED"
        build.total_cost = ZERO
        build.completed_at = timezone.now()
        if completed_by:
            build.completed_by = completed_by
        build._skip_inventory_signal = True
        build.save(update_fields=["status", "total_cost", "completed_at", "completed_by"])
        return build

    # DR WIP for total step-1 amount
    _add_line(je=wip_je, account=wip_account, debit=step1_total)

    # -----------------------------------------------------------------------
    # Step 2: Complete production  → DR Finished Goods / CR WIP
    # -----------------------------------------------------------------------
    finished = build.finished_product
    fin_inv_acc = _fallback_inventory_asset_account(finished, company=company)
    if not fin_inv_acc:
        raise ValueError(
            f"Finished product '{finished.name}' has no inventory asset account "
            "and no fallback account was found."
        )

    unit_cost_fg = _q2(step1_total / build_qty) if build_qty > ZERO else ZERO

    fg_je = _create_journal_entry(
        date=post_date,
        description=f"Assembly {build.assembly_number} – Step 2: WIP → Finished Goods",
        source_type=source_type,
        source_id=source_id,
    )
    if hasattr(fg_je, "company_id"):
        fg_je.company_id = getattr(company, "id", company)
        fg_je.save(update_fields=["company"])

    _add_line(je=fg_je, account=fin_inv_acc, debit=step1_total)
    _add_line(je=fg_je, account=wip_account, credit=step1_total)

    # Stock IN for finished product
    InventoryMovement.objects.create(
        product=finished,
        company=company,
        date=post_date,
        qty_in=build_qty,
        qty_out=ZERO,
        unit_cost=unit_cost_fg,
        value=step1_total,
        location=location,
        source_type=source_type,
        source_id=source_id,
        gl_entry=fg_je,
        is_gl_posted=True,
    )

    agg = finished.movements.aggregate(tin=Sum("qty_in"), tout=Sum("qty_out"))
    finished.quantity = _dec(agg["tin"]) - _dec(agg["tout"])
    finished.save(update_fields=["quantity"])
    rebuild_layers_from_movements(finished, company=company)

    # -----------------------------------------------------------------------
    # Finalise build record
    # -----------------------------------------------------------------------
    build.wip_journal_entry = wip_je
    build.journal_entry = fg_je
    build.status = "COMPLETED"
    build.total_cost = step1_total
    build.completed_at = timezone.now()
    if completed_by:
        build.completed_by = completed_by
    build._skip_inventory_signal = True
    build.save(update_fields=[
        "wip_journal_entry", "journal_entry", "status",
        "total_cost", "completed_at", "completed_by",
    ])
    return build


# ---------------------------------------------------------------------------
# Cancel Assembly
# ---------------------------------------------------------------------------

@transaction.atomic
def cancel_assembly(build):
    """
    Cancel a DRAFT or IN_PROGRESS assembly.
    No stock or GL impact to reverse (nothing was posted yet).

    Args:
        build: Build instance.

    Returns:
        The updated Build instance.
    """
    if build.status == "COMPLETED":
        raise ValueError(
            f"Assembly {build.assembly_number} is already completed. "
            "Use reverse_assembly() to undo a completed build."
        )
    if build.status == "CANCELLED":
        raise ValueError(f"Assembly {build.assembly_number} is already cancelled.")

    build.status = "CANCELLED"
    build._skip_inventory_signal = True
    build.save(update_fields=["status"])
    return build


# ---------------------------------------------------------------------------
# Reverse Assembly (undo a COMPLETED build)
# ---------------------------------------------------------------------------

@transaction.atomic
def reverse_assembly(build):
    """
    Reverse a COMPLETED assembly build.

    Actions:
      1. Delete Step 1 WIP journal entry (or create reversing entry)
      2. Delete Step 2 FG journal entry (or create reversing entry)
      3. Delete all ASSEMBLY inventory movements for this build
      4. Rebuild FIFO layers and product quantities for all affected products
      5. Set build.status = CANCELLED

    Args:
        build: Completed Build instance.

    Returns:
        The updated Build instance.
    """
    from inventory.models import InventoryMovement
    from inventory.fifo import rebuild_layers_from_movements
    from inventory.services import _delete_existing_source_movements
    from inventory.accounting import _create_journal_entry, _add_line

    if build.status != "COMPLETED":
        raise ValueError(
            f"Only COMPLETED assemblies can be reversed. "
            f"Build {build.assembly_number} has status {build.status}."
        )

    company = build.company
    post_date = timezone.localdate()  # reversal date is today
    source_type = "ASSEMBLY"
    source_id = build.id

    # -----------------------------------------------------------------------
    # Create reversing journal entries
    # -----------------------------------------------------------------------
    # Reverse Step 2 (FG → WIP) first to zero out FG
    if build.journal_entry_id:
        orig_fg_je = build.journal_entry
        rev_fg_je = _create_journal_entry(
            date=post_date,
            description=f"REVERSAL of Assembly {build.assembly_number} Step 2",
            source_type="ASSEMBLY_REVERSAL",
            source_id=source_id,
        )
        if hasattr(rev_fg_je, "company_id"):
            rev_fg_je.company_id = getattr(company, "id", company)
            rev_fg_je.save(update_fields=["company"])

        for line in orig_fg_je.lines.all():
            _add_line(
                je=rev_fg_je,
                account=line.account,
                debit=line.credit,   # swap
                credit=line.debit,   # swap
            )

    # Reverse Step 1 (WIP → Raw Materials) next
    if build.wip_journal_entry_id:
        orig_wip_je = build.wip_journal_entry
        rev_wip_je = _create_journal_entry(
            date=post_date,
            description=f"REVERSAL of Assembly {build.assembly_number} Step 1",
            source_type="ASSEMBLY_REVERSAL",
            source_id=source_id,
        )
        if hasattr(rev_wip_je, "company_id"):
            rev_wip_je.company_id = getattr(company, "id", company)
            rev_wip_je.save(update_fields=["company"])

        for line in orig_wip_je.lines.all():
            _add_line(
                je=rev_wip_je,
                account=line.account,
                debit=line.credit,   # swap
                credit=line.debit,   # swap
            )

    # -----------------------------------------------------------------------
    # Remove inventory movements and rebuild FIFO layers
    # -----------------------------------------------------------------------
    affected_product_ids = _delete_existing_source_movements(
        source_type, source_id, company=company
    )

    # -----------------------------------------------------------------------
    # Update build record
    # -----------------------------------------------------------------------
    build.status = "CANCELLED"
    build.journal_entry = None
    build.wip_journal_entry = None
    build.total_cost = ZERO
    build.completed_at = None
    build._skip_inventory_signal = True
    build.save(update_fields=[
        "status", "journal_entry", "wip_journal_entry",
        "total_cost", "completed_at",
    ])
    return build


# ---------------------------------------------------------------------------
# Report helpers
# ---------------------------------------------------------------------------

def assembly_report_data(company, date_from=None, date_to=None, status=None):
    """
    Return queryset data for the Assembly Report.

    Filters:
        date_from, date_to : date range on build_date
        status             : e.g. "COMPLETED", "DRAFT"
    """
    from inventory.models import Build

    qs = (
        Build.objects
        .for_company(company)
        .select_related("finished_product", "location", "created_by", "completed_by")
        .order_by("-build_date", "-id")
    )

    if date_from:
        qs = qs.filter(build_date__gte=date_from)
    if date_to:
        qs = qs.filter(build_date__lte=date_to)
    if status:
        qs = qs.filter(status=status)

    return qs


def component_consumption_report(company, date_from=None, date_to=None):
    """
    Return component consumption data grouped by component.

    Returns a list of dicts:
        {
            'component': Product,
            'total_qty_consumed': Decimal,
            'total_cost': Decimal,
            'builds': [Build, ...],
        }
    """
    from inventory.models import Build, BuildLine, InventoryMovement
    from django.db.models import Sum

    qs = Build.objects.for_company(company).filter(status="COMPLETED")
    if date_from:
        qs = qs.filter(build_date__gte=date_from)
    if date_to:
        qs = qs.filter(build_date__lte=date_to)

    build_ids = list(qs.values_list("id", flat=True))

    rows = {}
    for line in (
        BuildLine.objects
        .filter(build_id__in=build_ids)
        .select_related("component", "build")
    ):
        comp_id = line.component_id
        if comp_id not in rows:
            rows[comp_id] = {
                "component": line.component,
                "total_qty_consumed": ZERO,
                "total_cost": ZERO,
                "builds": [],
            }
        qty = _q2(_dec(line.qty_per_unit) * _dec(line.build.build_qty))
        rows[comp_id]["total_qty_consumed"] += qty
        rows[comp_id]["builds"].append(line.build)

    # Add cost from actual movements
    movements = (
        InventoryMovement.objects
        .filter(
            company=company,
            source_type="ASSEMBLY",
            source_id__in=build_ids,
            qty_out__gt=ZERO,
        )
        .values("product_id")
        .annotate(total_value=Sum("value"))
    )
    cost_map = {m["product_id"]: _q2(m["total_value"] or ZERO) for m in movements}
    for comp_id, row in rows.items():
        row["total_cost"] = cost_map.get(comp_id, ZERO)

    return list(rows.values())


def wip_report_data(company):
    """
    Return items currently In Progress (IN_PROGRESS status).
    """
    from inventory.models import Build

    return (
        Build.objects
        .for_company(company)
        .filter(status="IN_PROGRESS")
        .select_related("finished_product", "location")
        .order_by("build_date", "id")
    )


# ---------------------------------------------------------------------------
# CSV Import / Export
# ---------------------------------------------------------------------------

IMPORT_REQUIRED_FIELDS = {"finished_product_id", "build_qty", "build_date"}


def export_assemblies_csv(company, date_from=None, date_to=None) -> str:
    """
    Export assemblies to CSV string.
    """
    qs = assembly_report_data(company, date_from=date_from, date_to=date_to)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "assembly_number", "finished_product_id", "finished_product_name",
        "build_qty", "build_date", "status", "location", "total_cost", "memo",
    ])

    for build in qs:
        writer.writerow([
            build.assembly_number or "",
            build.finished_product_id or "",
            build.finished_product.name if build.finished_product_id else "",
            build.build_qty,
            build.build_date,
            build.status,
            build.location.name if build.location_id else "",
            build.total_cost,
            (build.memo or "").replace("\n", " "),
        ])

    return output.getvalue()


def import_assemblies_csv(csv_content: str, company, created_by=None) -> dict:
    """
    Import assemblies from CSV content string.

    Expected columns (all optional except marked):
        finished_product_id* : integer PK of finished product
        build_qty*           : decimal quantity
        build_date*          : YYYY-MM-DD
        memo                 : text
        status               : DRAFT (default) or IN_PROGRESS

    Returns:
        {"created": int, "errors": [str, ...]}
    """
    from inventory.models import Build, Product as Prod

    reader = csv.DictReader(io.StringIO(csv_content))
    created_count = 0
    errors = []

    for i, row in enumerate(reader, start=2):  # row 1 = header
        missing = IMPORT_REQUIRED_FIELDS - set(row.keys())
        if missing:
            errors.append(f"Row {i}: Missing required columns: {', '.join(sorted(missing))}")
            continue

        try:
            fp_id = int(row["finished_product_id"])
            finished = Prod.objects.for_company(company).get(id=fp_id)
        except (ValueError, Prod.DoesNotExist):
            errors.append(f"Row {i}: finished_product_id '{row.get('finished_product_id')}' not found.")
            continue

        try:
            build_qty = _q2(row["build_qty"])
            if build_qty <= ZERO:
                raise ValueError("build_qty must be > 0")
        except Exception:
            errors.append(f"Row {i}: Invalid build_qty '{row.get('build_qty')}'.")
            continue

        try:
            from datetime import date as _date
            build_date = _date.fromisoformat(row["build_date"].strip())
        except Exception:
            errors.append(f"Row {i}: Invalid build_date '{row.get('build_date')}' (expected YYYY-MM-DD).")
            continue

        status = row.get("status", "DRAFT").strip().upper()
        if status not in ("DRAFT", "IN_PROGRESS"):
            status = "DRAFT"

        try:
            Build.objects.create(
                company=company,
                finished_product=finished,
                build_qty=build_qty,
                build_date=build_date,
                status=status,
                memo=row.get("memo", "").strip(),
                created_by=created_by,
            )
            created_count += 1
        except Exception as exc:
            errors.append(f"Row {i}: Failed to create build — {exc}")

    return {"created": created_count, "errors": errors}
