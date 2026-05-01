#!/usr/bin/env python
"""
verify_deployment.py — Post-deployment verification script.

Runs a series of checks to confirm that the Phase 1 inventory accounting
deployment is correct.  Execute this script after running migrations:

    python verify_deployment.py

Exit codes:
    0 — all checks passed
    1 — one or more checks failed

Checks performed:
  1.  All products use FIFO (no WEIGHTED_AVERAGE)
  2.  Tracked products with qty > 0 have FIFO layers
  3.  GL Inventory Asset = FIFO layer value (per product)
  4.  No avg_cost references in COGS GL lines
  5.  All journal entries are balanced (DR = CR)
  6.  No TRANSFER source_type in GL journal entries
  7.  Product.VALUATION_METHODS contains only FIFO
  8.  reconcile_inventory_gl management command runs without error
  9.  All cached product quantities match movement sums
  10. Product model validation enforces FIFO
"""
import os
import sys
import django
from decimal import Decimal

# ── Django setup ──────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "sowafinance.settings")
django.setup()
# ─────────────────────────────────────────────────────────────────────────────

from django.db.models import Sum, Q, F, Abs, ExpressionWrapper, DecimalField

ZERO = Decimal("0.00")
TOLERANCE = Decimal("0.02")
PASS = "✓ PASS"
FAIL = "✗ FAIL"

issues = []


def check(name: str, ok: bool, detail: str = "") -> None:
    status = PASS if ok else FAIL
    msg = f"  {status}  {name}"
    if detail:
        msg += f"\n         {detail}"
    print(msg)
    if not ok:
        issues.append(name)


def _dec(v) -> Decimal:
    if v is None:
        return ZERO
    return Decimal(str(v))


# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "═" * 70)
print("  Sowa Accountants — Post-Deployment Verification")
print("═" * 70)

# ── Check 1: All products use FIFO ────────────────────────────────────────────
from inventory.models import Product, InventoryLayer
non_fifo = Product.objects.exclude(valuation_method="FIFO").count()
check(
    "All products use FIFO valuation",
    non_fifo == 0,
    f"Non-FIFO products: {non_fifo}" if non_fifo else "",
)

# ── Check 2: Tracked products with qty > 0 have layers ───────────────────────
from django.db.models import Count
tracked_no_layers = []
for p in Product.objects.filter(type="Inventory", track_inventory=True, quantity__gt=0):
    if not InventoryLayer.objects.filter(product=p, is_exhausted=False).exists():
        tracked_no_layers.append(f"{p} (company_id={p.company_id}, qty={p.quantity})")

check(
    "Tracked products with qty > 0 have FIFO layers",
    len(tracked_no_layers) == 0,
    "; ".join(tracked_no_layers[:5]) + ("..." if len(tracked_no_layers) > 5 else ""),
)

# ── Check 3: GL Inventory Asset = FIFO layer value ────────────────────────────
from accounts.models import Account, JournalLine
mismatched_products = []
for p in Product.objects.filter(type="Inventory", track_inventory=True).select_related(
    "inventory_asset_account"
):
    inv_acc = p.inventory_asset_account
    if not inv_acc:
        continue
    fifo_total = _dec(
        sum(
            _dec(l.qty_remaining) * _dec(l.unit_cost)
            for l in InventoryLayer.objects.filter(product=p, is_exhausted=False)
        )
    )
    agg = JournalLine.objects.filter(account=inv_acc).aggregate(
        dr=Sum("debit"), cr=Sum("credit")
    )
    gl_total = _dec(agg["dr"]) - _dec(agg["cr"])
    if abs(fifo_total - gl_total) > TOLERANCE:
        mismatched_products.append(
            f"{p}: FIFO={fifo_total:.2f}, GL={gl_total:.2f}"
        )

check(
    "GL Inventory Asset = FIFO layer value",
    len(mismatched_products) == 0,
    "; ".join(mismatched_products[:3]) + ("..." if len(mismatched_products) > 3 else ""),
)

# ── Check 4: No avg_cost in COGS GL lines ─────────────────────────────────────
avg_cost_refs = JournalLine.objects.filter(description__icontains="avg_cost").count()
check(
    "No avg_cost references in GL journal lines",
    avg_cost_refs == 0,
    f"{avg_cost_refs} lines reference avg_cost" if avg_cost_refs else "",
)

# ── Check 5: All journal entries are balanced ─────────────────────────────────
from accounts.models import JournalEntry
unbalanced = 0
for je in JournalEntry.objects.prefetch_related("lines"):
    lines = je.lines.all()
    total_dr = sum(_dec(ln.debit) for ln in lines)
    total_cr = sum(_dec(ln.credit) for ln in lines)
    if abs(total_dr - total_cr) > TOLERANCE and (total_dr > 0 or total_cr > 0):
        unbalanced += 1

check(
    "All journal entries balanced (DR = CR)",
    unbalanced == 0,
    f"{unbalanced} unbalanced entries found" if unbalanced else "",
)

# ── Check 6: No TRANSFER source_type in GL journal entries ────────────────────
transfer_je = JournalEntry.objects.filter(source_type="TRANSFER").count()
check(
    "No GL journal entries for stock transfers",
    transfer_je == 0,
    f"{transfer_je} TRANSFER journal entries found" if transfer_je else "",
)

# ── Check 7: Product.VALUATION_METHODS contains only FIFO ────────────────────
methods = [m[0] for m in Product.VALUATION_METHODS]
check(
    "Product.VALUATION_METHODS contains only FIFO",
    methods == ["FIFO"],
    f"Got: {methods}" if methods != ["FIFO"] else "",
)

# ── Check 8: reconcile_inventory_gl command runs cleanly ─────────────────────
from io import StringIO
from django.core.management import call_command
try:
    out = StringIO()
    call_command("reconcile_inventory_gl", stdout=out, stderr=StringIO())
    output = out.getvalue()
    reconcile_ok = "ALL FOUR REPORTS RECONCILE" in output or "Reconciliation Report" in output
    check("reconcile_inventory_gl command runs without error", True)
    check(
        "reconcile_inventory_gl reports reconciled",
        "MISMATCHES FOUND" not in output,
        "Run: python manage.py reconcile_inventory_gl" if "MISMATCHES FOUND" in output else "",
    )
except Exception as exc:
    check("reconcile_inventory_gl command runs without error", False, str(exc))

# ── Check 9: Cached product quantities match movement sums ────────────────────
from inventory.models import InventoryMovement
drifted = 0
for p in Product.objects.filter(type="Inventory"):
    agg = InventoryMovement.objects.filter(product=p).aggregate(
        tin=Sum("qty_in"), tout=Sum("qty_out")
    )
    expected = _dec(agg["tin"]) - _dec(agg["tout"])
    cached = _dec(p.quantity)
    if abs(expected - cached) > Decimal("0.01"):
        drifted += 1

check(
    "Cached product quantities match movement sums",
    drifted == 0,
    f"{drifted} products have quantity cache drift" if drifted else "",
)

# ── Check 10: Product model validation enforces FIFO ─────────────────────────
from django.core.exceptions import ValidationError
try:
    from tenancy.models import Company
    co = Company.objects.first()
    if co:
        test_product = Product(
            company=co,
            name="__verify_deploy_test__",
            type="Inventory",
            valuation_method="WEIGHTED_AVERAGE",
        )
        try:
            test_product.full_clean()
            validation_ok = False
        except ValidationError:
            validation_ok = True
    else:
        validation_ok = True  # no company to test with
    check("Product model rejects WEIGHTED_AVERAGE valuation", validation_ok)
except Exception as exc:
    check("Product model rejects WEIGHTED_AVERAGE valuation", False, str(exc))

# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "═" * 70)
total = 10
passed = total - len(issues)
if issues:
    print(f"\n  {FAIL}  {len(issues)} of {total} checks FAILED:\n")
    for issue in issues:
        print(f"    - {issue}")
    print()
    sys.exit(1)
else:
    print(f"\n  {PASS}  All {total} checks passed — deployment verified!\n")
    sys.exit(0)
