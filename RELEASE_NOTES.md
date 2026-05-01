# Inventory Accounting System — Release Notes

## Version: Phase 1 Production Release

**Date:** 2026-05-01
**Status:** Production Ready

---

## Summary

This release stabilises the inventory accounting engine by switching exclusively
to **FIFO (First In, First Out)** costing, unifying all GL posting through a
single canonical function, and adding a comprehensive regression test suite.

---

## Breaking Changes

### 1. Weighted Average Costing Removed

- `Product.VALUATION_METHODS` now contains **only** `"FIFO"`.
- Any product with `valuation_method = 'WEIGHTED_AVERAGE'` is automatically
  converted to `'FIFO'` by migration `0024_migrate_products_to_fifo`.
- **Action required:** After deployment, run the post-migration verification
  script to confirm all products are FIFO.

### 2. `avg_cost` field is Deprecated

- `Product.avg_cost` is preserved for historical/reporting compatibility but
  **must not** be used for new COGS posting logic.
- All COGS calculations now use FIFO layers (`InventoryLayer` model).
- Any custom code that reads `avg_cost` for pricing or COGS should be updated
  to call `Product.get_fifo_value()` or `simulate_fifo_consumption()` instead.

### 3. Single Posting Path for Invoices

- `post_invoice_inventory_and_gl()` in `inventory/accounting.py` is the **only**
  function that posts invoice GL entries (A/R, Revenue, VAT, COGS, Inventory).
- `_post_invoice_to_ledger()` in `sales/views.py` is now a thin delegation
  wrapper — do not add accounting logic there.

---

## New Features

### FIFO Cost Layers

- `InventoryLayer` model stores one cost layer per purchase/receipt.
- `record_purchase_layer()` creates layers on bill posting.
- `simulate_fifo_consumption()` reads layers (read-only) to preview COGS.
- `consume_fifo_layers()` deducts from layers on sale.
- `rebuild_layers_from_movements()` rebuilds all layers from raw movements.

### Transfer Handling

- **Transfer OUT** movements (`source_type='TRANSFER'`, `qty_out > 0`) consume
  FIFO layers without creating any GL entries.
- **Transfer IN** movements (`source_type='TRANSFER'`, `qty_in > 0`, `unit_cost > 0`)
  create new FIFO layers at the same unit cost, preserving company-wide inventory
  value.

### GL Reconciliation Commands

| Command | Purpose |
|---------|---------|
| `reconcile_inventory_gl` | Four-way reconciliation report |
| `reconcile_inventory_gl --daily-report` | One-line daily summary |
| `inventory_reconciliation_check` | Detailed mismatch analysis |
| `detect_inventory_variances` | FIFO vs GL variance detection |
| `detect_inventory_variances --weekly` | Weekly summary line |
| `audit_inventory_changes` | Monthly audit trail review |
| `audit_inventory_changes --monthly` | Monthly summary line |

### Product Model Validation (Phase 2)

- `Product.clean()` now enforces:
  - **Rule 1:** `valuation_method` must be `'FIFO'`.
  - **Rule 2:** `track_inventory=True` requires `inventory_asset_account`.
  - **Rule 3:** `track_inventory=True` requires `cogs_account`.

---

## Bug Fixes

- **Double-posting eliminated:** Signal handler now checks
  `_skip_inventory_signal` flag set by `post_invoice_inventory_and_gl()`.
- **Silent errors fixed:** Signal handlers now re-raise exceptions after
  logging (previously swallowed all errors silently).
- **COGS used avg_cost (wrong):** Replaced with FIFO layer cost throughout.
- **Transfer GL entries (phantom):** Transfers no longer create journal entries.
- **Multiple posting paths (legacy):** Legacy `_post_invoice_to_ledger()` code
  path now delegates to canonical function; old duplicate entries are deleted.

---

## Regression Tests

14 stabilisation tests added in `inventory/tests.py::InventoryStabilizationTests`:

1. Opening stock → sale at correct FIFO cost
2. Purchase → sale (FIFO cost)
3. Partial stock depletion
4. Sale across multiple FIFO layers (FIFO order preserved)
5. Transfer then sale (cost preserved through transfer)
6. Sale with insufficient FIFO stock (graceful skip, not crash)
7. Invoice creates matching GL + inventory movements
8. Stock transfer creates NO GL entry
9. FIFO layer value equals GL Inventory Asset balance
10. Journal entries are balanced (DR = CR)
11. Reconciliation management command runs cleanly
12. Signal errors propagate (no longer silenced)
13. Transfer IN creates FIFO layers
14. `_post_invoice_to_ledger` delegates to canonical function

Plus Phase 2 suites: `ProductValidationRuleTests`, `InventoryFullCycleSuite`,
`FIFOLayerIntegritySuite`, `TransferHandlingSuite`, `ReportReconciliationSuite`,
`EdgeCasesSuite`.

---

## Migration Path

1. **Back up** the production database.
2. Run `python manage.py migrate` — applies `0024_migrate_products_to_fifo`.
3. Run the deployment verification script: `python verify_deployment.py`.
4. Confirm all 14+ stabilisation tests pass.
5. Run `python manage.py reconcile_inventory_gl` to confirm GL reconciliation.

---

## Compatibility Notes

- `Product.avg_cost` field is retained (nullable) for backward compatibility with
  reports and UI that display historical average cost.
- `Product.stock_value` property (legacy) still works but returns `qty × avg_cost`.
  Use `Product.get_fifo_value()` for accurate FIFO-based stock value.
- All existing migration files are preserved; no destructive schema changes.
