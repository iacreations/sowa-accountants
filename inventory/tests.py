from django.test import TestCase
from decimal import Decimal
from django.utils import timezone

from inventory.fifo import (
    record_purchase_layer,
    simulate_fifo_consumption,
    consume_fifo_layers,
    compute_fifo_cogs,
    rebuild_layers_from_movements,
    get_available_layers,
)


class FIFOCostingTests(TestCase):
    """
    Unit tests for the FIFO costing engine.

    These tests use the in-memory Product / InventoryMovement / InventoryLayer
    models directly, so no full accounting stack is required.
    """

    def _make_product(self, name="Widget"):
        """Create a minimal Product instance for testing."""
        from inventory.models import Product
        from tenancy.models import Company

        company = Company.objects.create(name=f"Co-{name}", country="UG")
        return Product.objects.create(
            company=company,
            name=name,
            type="Inventory",
            quantity=Decimal("0.00"),
            avg_cost=Decimal("0.00"),
        )

    def _make_location(self, company):
        from inventory.models import MainStore, InventoryLocation
        store, _ = MainStore.objects.get_or_create(
            company=company, name="Main",
            defaults={"is_active": True},
        )
        loc, _ = InventoryLocation.objects.get_or_create(
            company=company, store=store, name="Default",
            defaults={"is_default": True, "is_active": True},
        )
        return loc

    def _make_movement_in(self, product, qty, unit_cost, date=None):
        from inventory.models import InventoryMovement
        loc = self._make_location(product.company)
        date = date or timezone.localdate()
        return InventoryMovement.objects.create(
            product=product,
            company=product.company,
            location=loc,
            date=date,
            qty_in=qty,
            qty_out=Decimal("0.00"),
            unit_cost=unit_cost,
            value=qty * unit_cost,
            source_type="BILL",
            source_id=1,
        )

    # ------------------------------------------------------------------
    # Scenario A: Buy 10@750, sell 1 → cost = 750
    # ------------------------------------------------------------------
    def test_scenario_a_single_sale_from_first_layer(self):
        p = self._make_product("Mineral Water A")
        mv = self._make_movement_in(p, Decimal("10"), Decimal("750"))
        record_purchase_layer(p, Decimal("750"), Decimal("10"), mv.date, mv, p.company)

        result = simulate_fifo_consumption(p, Decimal("1"))

        self.assertEqual(len(result), 1)
        cost, qty = result[0]
        self.assertEqual(qty, Decimal("1"))
        self.assertEqual(cost, Decimal("750"))

    # ------------------------------------------------------------------
    # Scenario B: Buy 10@750, buy 20@1000, sell 15 → 2 movements
    # ------------------------------------------------------------------
    def test_scenario_b_sale_spans_two_layers(self):
        p = self._make_product("Mineral Water B")
        from datetime import date

        mv1 = self._make_movement_in(p, Decimal("10"), Decimal("750"), date(2024, 1, 1))
        mv2 = self._make_movement_in(p, Decimal("20"), Decimal("1000"), date(2024, 1, 2))

        record_purchase_layer(p, Decimal("750"), Decimal("10"), date(2024, 1, 1), mv1, p.company)
        record_purchase_layer(p, Decimal("1000"), Decimal("20"), date(2024, 1, 2), mv2, p.company)

        result = simulate_fifo_consumption(p, Decimal("15"))

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], (Decimal("750"), Decimal("10")))
        self.assertEqual(result[1], (Decimal("1000"), Decimal("5")))

    # ------------------------------------------------------------------
    # Scenario C: After Layer 1 exhausted, next sale uses Layer 2 cost
    # ------------------------------------------------------------------
    def test_scenario_c_after_first_layer_exhausted(self):
        p = self._make_product("Mineral Water C")
        from datetime import date

        mv1 = self._make_movement_in(p, Decimal("10"), Decimal("750"), date(2024, 1, 1))
        mv2 = self._make_movement_in(p, Decimal("20"), Decimal("1000"), date(2024, 1, 2))

        record_purchase_layer(p, Decimal("750"), Decimal("10"), date(2024, 1, 1), mv1, p.company)
        record_purchase_layer(p, Decimal("1000"), Decimal("20"), date(2024, 1, 2), mv2, p.company)

        # Consume all of Layer 1
        consume_fifo_layers(p, Decimal("10"))

        # Layer 1 should now be exhausted
        layers = get_available_layers(p)
        self.assertEqual(len(layers), 1)
        self.assertEqual(layers[0].unit_cost, Decimal("1000"))

        # Next sale should come from Layer 2
        result = simulate_fifo_consumption(p, Decimal("1"))
        self.assertEqual(len(result), 1)
        cost, qty = result[0]
        self.assertEqual(cost, Decimal("1000"))
        self.assertEqual(qty, Decimal("1"))

    # ------------------------------------------------------------------
    # COGS computation
    # ------------------------------------------------------------------
    def test_compute_fifo_cogs_single_layer(self):
        p = self._make_product("COGS Test")
        mv = self._make_movement_in(p, Decimal("10"), Decimal("500"))
        record_purchase_layer(p, Decimal("500"), Decimal("10"), mv.date, mv, p.company)

        cogs = compute_fifo_cogs(p, Decimal("3"))
        self.assertEqual(cogs, Decimal("1500.00"))

    def test_compute_fifo_cogs_two_layers(self):
        p = self._make_product("COGS Two Layers")
        from datetime import date

        mv1 = self._make_movement_in(p, Decimal("10"), Decimal("750"), date(2024, 1, 1))
        mv2 = self._make_movement_in(p, Decimal("20"), Decimal("1000"), date(2024, 1, 2))

        record_purchase_layer(p, Decimal("750"), Decimal("10"), date(2024, 1, 1), mv1, p.company)
        record_purchase_layer(p, Decimal("1000"), Decimal("20"), date(2024, 1, 2), mv2, p.company)

        # Sell 15: 10@750 + 5@1000 = 7500 + 5000 = 12500
        cogs = compute_fifo_cogs(p, Decimal("15"))
        self.assertEqual(cogs, Decimal("12500.00"))

    # ------------------------------------------------------------------
    # Rebuild from movements
    # ------------------------------------------------------------------
    def test_rebuild_layers_from_movements(self):
        p = self._make_product("Rebuild Test")
        from datetime import date
        from inventory.models import InventoryMovement

        loc = self._make_location(p.company)

        InventoryMovement.objects.create(
            product=p, company=p.company, location=loc,
            date=date(2024, 1, 1),
            qty_in=Decimal("10"), qty_out=Decimal("0"),
            unit_cost=Decimal("750"), value=Decimal("7500"),
            source_type="BILL", source_id=1,
        )
        InventoryMovement.objects.create(
            product=p, company=p.company, location=loc,
            date=date(2024, 1, 2),
            qty_in=Decimal("20"), qty_out=Decimal("0"),
            unit_cost=Decimal("1000"), value=Decimal("20000"),
            source_type="BILL", source_id=2,
        )
        # Sale of 5
        InventoryMovement.objects.create(
            product=p, company=p.company, location=loc,
            date=date(2024, 1, 3),
            qty_in=Decimal("0"), qty_out=Decimal("5"),
            unit_cost=Decimal("750"), value=Decimal("3750"),
            source_type="INVOICE", source_id=1,
        )

        rebuild_layers_from_movements(p, p.company)

        layers = list(p.fifo_layers.all())
        self.assertEqual(len(layers), 2)
        # Layer 1: 10 in, 5 consumed → 5 remaining
        self.assertEqual(layers[0].qty_in, Decimal("10"))
        self.assertEqual(layers[0].qty_remaining, Decimal("5"))
        self.assertFalse(layers[0].is_exhausted)
        # Layer 2: 20 in, 0 consumed → 20 remaining
        self.assertEqual(layers[1].qty_in, Decimal("20"))
        self.assertEqual(layers[1].qty_remaining, Decimal("20"))
        self.assertFalse(layers[1].is_exhausted)

    # ------------------------------------------------------------------
    # Scenario D: Transfers don't create new layers
    # ------------------------------------------------------------------
    def test_scenario_d_transfer_does_not_affect_layers(self):
        p = self._make_product("Transfer Test")
        mv = self._make_movement_in(p, Decimal("10"), Decimal("750"))
        record_purchase_layer(p, Decimal("750"), Decimal("10"), mv.date, mv, p.company)

        # Simulate a transfer OUT movement (not a BILL/EXPENSE etc.)
        from inventory.models import InventoryMovement
        loc = self._make_location(p.company)
        InventoryMovement.objects.create(
            product=p, company=p.company, location=loc,
            date=timezone.localdate(),
            qty_in=Decimal("0"), qty_out=Decimal("3"),
            unit_cost=Decimal("750"), value=Decimal("2250"),
            source_type="TRANSFER", source_id=1,
        )

        # Rebuild from movements — transfer should reduce qty_remaining
        rebuild_layers_from_movements(p, p.company)

        layers = get_available_layers(p)
        self.assertEqual(len(layers), 1)
        # Transfer consumed from Layer 1 (rebuild treats OUT as consumption)
        self.assertEqual(layers[0].qty_remaining, Decimal("7"))


class FIFOFromDateTests(TestCase):
    """
    Tests for the from_date parameter of rebuild_layers_from_movements
    and the FIFO reset mechanism.
    """

    def _make_product(self, name="Widget"):
        from inventory.models import Product
        from tenancy.models import Company
        company = Company.objects.create(name=f"Co-{name}", country="UG")
        return Product.objects.create(
            company=company,
            name=name,
            type="Inventory",
            quantity=Decimal("0.00"),
            avg_cost=Decimal("0.00"),
        )

    def _make_location(self, company):
        from inventory.models import MainStore, InventoryLocation
        store, _ = MainStore.objects.get_or_create(
            company=company, name="Main",
            defaults={"is_active": True},
        )
        loc, _ = InventoryLocation.objects.get_or_create(
            company=company, store=store, name="Default",
            defaults={"is_default": True, "is_active": True},
        )
        return loc

    def _make_movement(self, product, qty_in, qty_out, unit_cost, source_type, date):
        from inventory.models import InventoryMovement
        loc = self._make_location(product.company)
        return InventoryMovement.objects.create(
            product=product,
            company=product.company,
            location=loc,
            date=date,
            qty_in=qty_in,
            qty_out=qty_out,
            unit_cost=unit_cost,
            value=(qty_in or qty_out) * unit_cost,
            source_type=source_type,
            source_id=1,
        )

    # ------------------------------------------------------------------
    # rebuild_layers_from_movements with from_date ignores old movements
    # ------------------------------------------------------------------
    def test_rebuild_with_from_date_ignores_pre_cutoff_movements(self):
        """Movements before from_date should not be replayed."""
        from datetime import date
        p = self._make_product("FromDate Test")

        # Old purchase (before cut-off) — should be ignored
        self._make_movement(
            p, Decimal("10"), Decimal("0"), Decimal("500"), "BILL", date(2025, 1, 1)
        )
        # Old sale (before cut-off) — should be ignored
        self._make_movement(
            p, Decimal("0"), Decimal("3"), Decimal("500"), "INVOICE", date(2025, 6, 1)
        )
        # Cut-off opening balance
        self._make_movement(
            p, Decimal("7"), Decimal("0"), Decimal("500"), "OPENING", date(2026, 4, 26)
        )
        # Post-cutoff sale — should consume from opening layer
        self._make_movement(
            p, Decimal("0"), Decimal("2"), Decimal("500"), "INVOICE", date(2026, 5, 1)
        )

        rebuild_layers_from_movements(p, p.company, from_date=date(2026, 4, 26))

        layers = list(p.fifo_layers.all())
        # Only OPENING movement creates a layer (pre-cutoff BILL is ignored)
        self.assertEqual(len(layers), 1)
        self.assertEqual(layers[0].qty_in, Decimal("7"))
        # Post-cutoff sale consumed 2 → 5 remaining
        self.assertEqual(layers[0].qty_remaining, Decimal("5"))

    def test_rebuild_without_from_date_uses_all_movements(self):
        """Without from_date, all movements are replayed as before."""
        from datetime import date
        p = self._make_product("AllMovements Test")

        self._make_movement(
            p, Decimal("10"), Decimal("0"), Decimal("750"), "BILL", date(2024, 1, 1)
        )
        self._make_movement(
            p, Decimal("0"), Decimal("4"), Decimal("750"), "INVOICE", date(2024, 6, 1)
        )

        rebuild_layers_from_movements(p, p.company)

        layers = list(p.fifo_layers.all())
        self.assertEqual(len(layers), 1)
        self.assertEqual(layers[0].qty_in, Decimal("10"))
        self.assertEqual(layers[0].qty_remaining, Decimal("6"))

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------
    def test_validate_fifo_stock_available_passes(self):
        """Validation passes when enough FIFO stock exists."""
        from datetime import date
        from inventory.validators import validate_fifo_stock_available
        p = self._make_product("Validator OK")
        self._make_movement(
            p, Decimal("10"), Decimal("0"), Decimal("500"), "BILL", date(2026, 1, 1)
        )
        rebuild_layers_from_movements(p, p.company)
        # Should not raise
        validate_fifo_stock_available(p, Decimal("5"))

    def test_validate_fifo_stock_available_fails(self):
        """Validation fails when requesting more than available FIFO stock."""
        from datetime import date
        from inventory.validators import validate_fifo_stock_available
        p = self._make_product("Validator Fail")
        self._make_movement(
            p, Decimal("3"), Decimal("0"), Decimal("500"), "BILL", date(2026, 1, 1)
        )
        rebuild_layers_from_movements(p, p.company)
        with self.assertRaises(ValueError):
            validate_fifo_stock_available(p, Decimal("5"))

    def test_check_negative_inventory_balances(self):
        """check_negative_inventory_balances detects products with negative qty."""
        from datetime import date
        from inventory.validators import check_negative_inventory_balances
        p = self._make_product("Negative Inv")
        # Sale before purchase (bad historical data)
        self._make_movement(
            p, Decimal("0"), Decimal("5"), Decimal("500"), "INVOICE", date(2025, 1, 1)
        )
        problems = check_negative_inventory_balances(company=p.company)
        self.assertEqual(len(problems), 1)
        self.assertEqual(problems[0][0].id, p.id)
        self.assertEqual(problems[0][1], Decimal("-5"))

    # ------------------------------------------------------------------
    # InventoryMovement.is_opening_balance field
    # ------------------------------------------------------------------
    def test_is_opening_balance_field_default_false(self):
        """New movements have is_opening_balance=False by default."""
        from datetime import date
        p = self._make_product("IsOpening Default")
        mv = self._make_movement(
            p, Decimal("10"), Decimal("0"), Decimal("500"), "BILL", date(2026, 1, 1)
        )
        self.assertFalse(mv.is_opening_balance)

    def test_is_opening_balance_field_can_be_set_true(self):
        """is_opening_balance can be set to True for opening balance movements."""
        from datetime import date
        from inventory.models import InventoryMovement
        p = self._make_product("IsOpening Set")
        loc = self._make_location(p.company)
        mv = InventoryMovement.objects.create(
            product=p,
            company=p.company,
            location=loc,
            date=date(2026, 4, 26),
            qty_in=Decimal("10"),
            qty_out=Decimal("0"),
            unit_cost=Decimal("500"),
            value=Decimal("5000"),
            source_type="OPENING",
            source_id=p.id,
            is_opening_balance=True,
        )
        self.assertTrue(mv.is_opening_balance)

    # ------------------------------------------------------------------
    # Product.cut_off_date field
    # ------------------------------------------------------------------
    def test_product_cut_off_date_field(self):
        """Product.cut_off_date can be set and retrieved."""
        from datetime import date
        p = self._make_product("CutOff Field")
        p.cut_off_date = date(2026, 4, 26)
        p.save(update_fields=["cut_off_date"])
        p.refresh_from_db()
        self.assertEqual(p.cut_off_date, date(2026, 4, 26))




# ==========================================================
# PHASE 3 - New Models Tests
# ==========================================================

class PhaseThreeModelsTests(TestCase):
    """Tests for new Phase 3 models: StockAdjustment, Batch, InventoryAlert, etc."""

    def _make_company(self, name="TestCo"):
        from tenancy.models import Company
        return Company.objects.create(name=name, country="UG")

    def _make_product(self, company, name="Widget"):
        from inventory.models import Product
        return Product.objects.create(
            company=company, name=name, type="Inventory",
            quantity=Decimal("0.00"), avg_cost=Decimal("0.00"),
        )

    def _make_location(self, company):
        from inventory.models import MainStore, InventoryLocation
        store, _ = MainStore.objects.get_or_create(
            company=company, name="Main", defaults={"is_active": True},
        )
        loc, _ = InventoryLocation.objects.get_or_create(
            company=company, store=store, name="Default",
            defaults={"is_default": True, "is_active": True},
        )
        return loc

    def test_stock_adjustment_creation(self):
        from inventory.models import StockAdjustment, StockAdjustmentLine
        company = self._make_company("AdjCo")
        product = self._make_product(company)
        adj = StockAdjustment.objects.create(
            company=company, reason="damage", status="draft"
        )
        line = StockAdjustmentLine.objects.create(
            adjustment=adj, product=product,
            qty_decrease=Decimal("5.00"), unit_cost=Decimal("100.00"),
        )
        self.assertEqual(adj.status, "draft")
        self.assertEqual(line.qty_decrease, Decimal("5.00"))

    def test_stock_count_worksheet_variance(self):
        from inventory.models import StockCountWorksheet, StockCountLine
        company = self._make_company("CountCo")
        product = self._make_product(company)
        loc = self._make_location(company)
        ws = StockCountWorksheet.objects.create(company=company, location=loc, status="draft")
        line = StockCountLine.objects.create(
            worksheet=ws, product=product,
            expected_qty=Decimal("10.00"), counted_qty=Decimal("8.00"),
        )
        self.assertEqual(line.variance, Decimal("-2.00"))

    def test_batch_creation(self):
        from inventory.models import Batch
        from datetime import date
        company = self._make_company("BatchCo")
        product = self._make_product(company)
        batch = Batch.objects.create(
            company=company, product=product,
            batch_number="BATCH-001",
            expiry_date=date(2027, 12, 31),
            quantity_purchased=Decimal("100.00"),
            quantity_available=Decimal("100.00"),
            status="active",
        )
        self.assertEqual(batch.batch_number, "BATCH-001")
        self.assertEqual(batch.status, "active")

    def test_inventory_alert_creation(self):
        from inventory.models import InventoryAlert
        company = self._make_company("AlertCo")
        product = self._make_product(company)
        alert = InventoryAlert.objects.create(
            company=company, product=product,
            alert_type="low_stock", severity="warning",
            message="Stock is low",
        )
        self.assertFalse(alert.is_resolved)
        self.assertEqual(alert.alert_type, "low_stock")

    def test_inventory_alert_threshold(self):
        from inventory.models import InventoryAlertThreshold
        company = self._make_company("ThreshCo")
        product = self._make_product(company)
        threshold = InventoryAlertThreshold.objects.create(
            company=company, product=product,
            low_stock_threshold=Decimal("5.00"),
            expiry_warning_days=14,
        )
        self.assertEqual(threshold.low_stock_threshold, Decimal("5.00"))

    def test_supplier_price_history(self):
        from inventory.models import SupplierPriceHistory
        from sowaf.models import Newsupplier
        company = self._make_company("SupplierCo")
        product = self._make_product(company)
        supplier = Newsupplier.objects.create(company=company, company_name="TestSupplier")
        price_record = SupplierPriceHistory.objects.create(
            company=company, product=product, supplier=supplier,
            unit_price=Decimal("500.00"), currency="UGX",
            purchase_qty=Decimal("50.00"),
        )
        self.assertEqual(price_record.unit_price, Decimal("500.00"))


# ==========================================================
# PHASE 4 - FIFO Engine Enhancements Tests
# ==========================================================

class PhaseFourFIFOTests(TestCase):
    """Tests for Phase 4 FIFO engine enhancements."""

    def _make_product(self):
        from inventory.models import Product
        from tenancy.models import Company
        company = Company.objects.create(name="FIFOCo4", country="UG")
        return Product.objects.create(
            company=company, name="Widget4", type="Inventory",
            quantity=Decimal("0.00"), avg_cost=Decimal("0.00"),
        )

    def _make_movement_in(self, product, qty, cost, date=None):
        from inventory.models import InventoryMovement, MainStore, InventoryLocation
        from django.utils import timezone
        company = product.company
        store, _ = MainStore.objects.get_or_create(company=company, name="Main", defaults={"is_active": True})
        loc, _ = InventoryLocation.objects.get_or_create(
            company=company, store=store, name="Default",
            defaults={"is_default": True, "is_active": True},
        )
        return InventoryMovement.objects.create(
            product=product, company=company, location=loc,
            date=date or timezone.localdate(),
            qty_in=qty, qty_out=Decimal("0.00"), unit_cost=cost,
            value=qty * cost, source_type="BILL", source_id=1,
        )

    def test_record_stock_in(self):
        from inventory.fifo import record_stock_in
        p = self._make_product()
        mv = self._make_movement_in(p, Decimal("10"), Decimal("100"))
        layer = record_stock_in(p, Decimal("100"), Decimal("10"), mv.date, mv, p.company)
        self.assertEqual(layer.qty_in, Decimal("10.00"))
        self.assertEqual(layer.unit_cost, Decimal("100.00"))

    def test_validate_available_stock_passes(self):
        from inventory.fifo import record_purchase_layer, validate_available_stock
        p = self._make_product()
        mv = self._make_movement_in(p, Decimal("10"), Decimal("100"))
        record_purchase_layer(p, Decimal("100"), Decimal("10"), mv.date, mv, p.company)
        validate_available_stock(p, Decimal("5"))

    def test_validate_available_stock_fails(self):
        from inventory.fifo import record_purchase_layer, validate_available_stock
        p = self._make_product()
        mv = self._make_movement_in(p, Decimal("3"), Decimal("100"))
        record_purchase_layer(p, Decimal("100"), Decimal("3"), mv.date, mv, p.company)
        with self.assertRaises(ValueError):
            validate_available_stock(p, Decimal("10"))

    def test_calculate_inventory_value_fifo(self):
        from inventory.fifo import record_purchase_layer, calculate_inventory_value_fifo
        p = self._make_product()
        mv = self._make_movement_in(p, Decimal("10"), Decimal("500"))
        record_purchase_layer(p, Decimal("500"), Decimal("10"), mv.date, mv, p.company)
        value = calculate_inventory_value_fifo(p)
        self.assertEqual(value, Decimal("5000.00"))

    def test_calculate_cogs_fifo(self):
        from inventory.fifo import record_purchase_layer, calculate_cogs_fifo
        p = self._make_product()
        mv = self._make_movement_in(p, Decimal("10"), Decimal("750"))
        record_purchase_layer(p, Decimal("750"), Decimal("10"), mv.date, mv, p.company)
        cogs = calculate_cogs_fifo(p, Decimal("3"))
        self.assertEqual(cogs, Decimal("2250.00"))


# ==========================================================
# PHASE 8 - Alert Generation Tests
# ==========================================================

class PhaseEightAlertsTests(TestCase):
    """Tests for Phase 8 alert generation."""

    def _make_product(self, name, qty=0):
        from inventory.models import Product, InventoryMovement, MainStore, InventoryLocation
        from inventory.fifo import record_purchase_layer
        from tenancy.models import Company
        from django.utils import timezone

        company = Company.objects.create(name=f"AlertCo-{name}", country="UG")
        p = Product.objects.create(
            company=company, name=name, type="Inventory",
            quantity=Decimal(str(qty)), avg_cost=Decimal("100.00"),
        )
        if qty > 0:
            store, _ = MainStore.objects.get_or_create(company=company, name="Main", defaults={"is_active": True})
            loc, _ = InventoryLocation.objects.get_or_create(
                company=company, store=store, name="Default",
                defaults={"is_default": True, "is_active": True},
            )
            mv = InventoryMovement.objects.create(
                product=p, company=company, location=loc,
                date=timezone.localdate(),
                qty_in=Decimal(str(qty)), qty_out=Decimal("0.00"),
                unit_cost=Decimal("100.00"), value=Decimal(str(qty)) * Decimal("100.00"),
                source_type="BILL", source_id=1,
            )
            record_purchase_layer(p, Decimal("100.00"), Decimal(str(qty)), mv.date, mv, company)
        return p

    def test_out_of_stock_alert(self):
        from inventory.services import generate_inventory_alerts
        from inventory.models import InventoryAlert
        p = self._make_product("OutOfStock", qty=0)
        generate_inventory_alerts(company=p.company)
        alert = InventoryAlert.objects.filter(product=p, alert_type="out_of_stock").first()
        self.assertIsNotNone(alert)
        self.assertEqual(alert.severity, "critical")

    def test_low_stock_alert(self):
        from inventory.services import generate_inventory_alerts
        from inventory.models import InventoryAlert, InventoryAlertThreshold
        p = self._make_product("LowStock", qty=3)
        InventoryAlertThreshold.objects.create(
            company=p.company, product=p, low_stock_threshold=Decimal("10.00")
        )
        generate_inventory_alerts(company=p.company)
        alert = InventoryAlert.objects.filter(product=p, alert_type="low_stock").first()
        self.assertIsNotNone(alert)

    def test_no_sku_alert(self):
        from inventory.services import generate_inventory_alerts
        from inventory.models import InventoryAlert
        p = self._make_product("NoSKU", qty=5)
        p.sku = None
        p.save(update_fields=["sku"])
        generate_inventory_alerts(company=p.company)
        alert = InventoryAlert.objects.filter(product=p, alert_type="no_sku").first()
        self.assertIsNotNone(alert)


# ==========================================================
# PHASE 10 - Validator Tests
# ==========================================================

class PhaseTenValidatorTests(TestCase):
    """Tests for Phase 10 validators."""

    def test_validate_non_zero_purchase_cost_fails(self):
        from inventory.validators import validate_non_zero_purchase_cost
        from inventory.models import Product
        from tenancy.models import Company
        company = Company.objects.create(name="ValidatorCo", country="UG")
        p = Product.objects.create(company=company, name="Widget", type="Inventory")
        with self.assertRaises(ValueError):
            validate_non_zero_purchase_cost(p, Decimal("0.00"))

    def test_validate_non_zero_purchase_cost_passes_free(self):
        from inventory.validators import validate_non_zero_purchase_cost
        from inventory.models import Product
        from tenancy.models import Company
        company = Company.objects.create(name="FreeCo", country="UG")
        p = Product.objects.create(company=company, name="FreeWidget", type="Inventory")
        validate_non_zero_purchase_cost(p, Decimal("0.00"), is_free=True)

    def test_validate_not_service_item_fails(self):
        from inventory.validators import validate_not_service_item
        from inventory.models import Product
        from tenancy.models import Company
        company = Company.objects.create(name="ServiceCo", country="UG")
        p = Product.objects.create(company=company, name="Consulting", type="Service")
        with self.assertRaises(ValueError):
            validate_not_service_item(p)

    def test_validate_movement_qty_both_zero_fails(self):
        from inventory.validators import validate_movement_qty
        with self.assertRaises(ValueError):
            validate_movement_qty(Decimal("0.00"), Decimal("0.00"))

    def test_validate_movement_qty_both_positive_fails(self):
        from inventory.validators import validate_movement_qty
        with self.assertRaises(ValueError):
            validate_movement_qty(Decimal("5.00"), Decimal("3.00"))

    def test_validate_fifo_layer_qty_fails(self):
        from inventory.validators import validate_fifo_layer_qty
        with self.assertRaises(ValueError):
            validate_fifo_layer_qty(Decimal("0.00"))

    def test_validate_fifo_layer_cost_fails(self):
        from inventory.validators import validate_fifo_layer_cost
        with self.assertRaises(ValueError):
            validate_fifo_layer_cost(Decimal("0.00"))
