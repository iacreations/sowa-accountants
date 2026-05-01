# Production Deployment Checklist

## Inventory Accounting System — Phase 1 Production Deployment

**Deployment Date:** _______________
**Deployed By:** _______________
**Environment:** Production

---

## Pre-Deployment

### 1. Database Backup

- [ ] Full database backup completed
- [ ] Backup file name: `_______________`
- [ ] Backup stored at: `_______________`
- [ ] Backup integrity verified (restore test or checksum)

### 2. Code Review

- [ ] All 14 stabilisation tests passing locally
- [ ] All Phase 2 test suites passing locally
- [ ] No uncommitted changes in production branch
- [ ] PR reviewed and approved

### 3. Staging Verification

- [ ] Migrations applied to staging without errors
- [ ] All tests pass in staging environment
- [ ] Reconciliation check passes in staging:
  ```
  python manage.py reconcile_inventory_gl
  ```
- [ ] Verify no WEIGHTED_AVERAGE products remain in staging:
  ```sql
  SELECT COUNT(*) FROM inventory_product WHERE valuation_method != 'FIFO';
  -- Expected: 0
  ```

---

## Deployment Steps

### 4. Apply Migrations

```bash
python manage.py migrate --list           # Review pending migrations
python manage.py migrate                  # Apply all pending migrations
```

Key migrations:
- `inventory 0024_migrate_products_to_fifo` — converts WEIGHTED_AVERAGE → FIFO

Expected output:
```
Applying inventory.0024_migrate_products_to_fifo... OK
```

- [ ] All migrations applied successfully

### 5. Convert WEIGHTED_AVERAGE Products to FIFO

Migration `0024` handles this automatically.  Verify manually:

```python
python manage.py shell -c "
from inventory.models import Product
wa_count = Product.objects.exclude(valuation_method='FIFO').count()
print(f'Non-FIFO products: {wa_count}')
# Expected: 0
"
```

- [ ] Non-FIFO product count = 0

### 6. Verify FIFO Layers Exist for All Tracked Products

```python
python manage.py shell -c "
from inventory.models import Product, InventoryLayer
from django.db.models import Count

tracked = Product.objects.filter(type='Inventory', track_inventory=True)
no_layers = [p for p in tracked if not InventoryLayer.objects.filter(product=p).exists()
             and p.quantity > 0]
print(f'Tracked products without FIFO layers: {len(no_layers)}')
for p in no_layers:
    print(f'  - {p} (company: {p.company}, qty: {p.quantity})')
"
```

- [ ] All tracked products with qty > 0 have FIFO layers (or rebuild if needed):
  ```bash
  python manage.py rebuild_fifo_layers
  ```

---

## Post-Deployment Verification

### 7. Run Stabilisation Test Suite

```bash
DATABASE_URL=$PROD_DATABASE_URL python manage.py test \
  inventory.tests.InventoryStabilizationTests \
  inventory.tests.FIFOEnforcementTests \
  inventory.tests.ProductValidationRuleTests \
  inventory.tests.InventoryFullCycleSuite \
  inventory.tests.FIFOLayerIntegritySuite \
  inventory.tests.TransferHandlingSuite \
  inventory.tests.ReportReconciliationSuite \
  inventory.tests.EdgeCasesSuite \
  -v 2
```

- [ ] All tests pass (0 failures, 0 errors)

### 8. GL Reconciliation Check

```bash
python manage.py reconcile_inventory_gl
```

Expected output ends with:
```
✓ ALL FOUR REPORTS RECONCILE
```

- [ ] Reconciliation passes for all companies
- [ ] No mismatches found

### 9. Run Deployment Verification Script

```bash
python verify_deployment.py
```

- [ ] All checks pass

### 10. Check GL Lines Do Not Use avg_cost

```python
python manage.py shell -c "
from accounts.models import JournalLine
avg_cost_lines = JournalLine.objects.filter(description__icontains='avg_cost')
print(f'JournalLines referencing avg_cost: {avg_cost_lines.count()}')
# Expected: 0
"
```

- [ ] No GL lines reference avg_cost

### 11. Smoke Test: Post a Test Invoice

- [ ] Create a test invoice in production (sandbox customer)
- [ ] Verify FIFO COGS is posted correctly
- [ ] Verify journal entry is balanced (DR = CR)
- [ ] Delete test invoice after verification

---

## Monitoring Setup

### 12. Configure Daily Reconciliation

Add to cron / task scheduler:
```bash
# Daily (e.g. 06:00)
python manage.py reconcile_inventory_gl --daily-report >> /var/log/inventory_daily.log 2>&1
```

- [ ] Daily reconciliation cron job configured

### 13. Configure Weekly Variance Detection

```bash
# Weekly (e.g. Monday 07:00)
python manage.py detect_inventory_variances --weekly >> /var/log/inventory_weekly.log 2>&1
```

- [ ] Weekly variance detection cron job configured

### 14. Configure Monthly Audit Review

```bash
# Monthly (1st of month, 08:00)
python manage.py audit_inventory_changes --monthly >> /var/log/inventory_monthly.log 2>&1
```

- [ ] Monthly audit cron job configured

---

## Roll-Back Procedure

If the deployment fails at any step:

1. **Stop application servers** immediately.
2. **Restore the database backup** taken in Step 1.
3. **Revert the code** to the previous tag/commit:
   ```bash
   git checkout <previous-release-tag>
   ```
4. **Restart application servers**.
5. **Verify roll-back** by checking that the old application version is running.
6. **Investigate the failure** before attempting re-deployment.

- [ ] Roll-back procedure understood by deployment team
- [ ] Previous release tag/commit recorded: `_______________`

---

## Sign-Off

| Role | Name | Signature | Date |
|------|------|-----------|------|
| Developer | | | |
| QA | | | |
| Operations | | | |
| Finance/Accounting | | | |
