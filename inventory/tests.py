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


# ==========================================================
# GL Posting / Accounting Tests
# ==========================================================

class InventoryAccountingGLTests(TestCase):
    """
    Tests for the GL posting engine in inventory/accounting.py.

    Verifies that purchases, sales, stock adjustments, and opening stock
    all produce correct double-entry journal entries with proper FIFO costing.
    """

    def setUp(self):
        from tenancy.models import Company
        from accounts.models import Account
        from inventory.models import Product, MainStore, InventoryLocation
        from sowaf.models import Newsupplier, Newcustomer

        self.company = Company.objects.create(name="GLTestCo", country="UG")

        # Chart of Accounts
        self.inv_asset_acc = Account.objects.create(
            company=self.company,
            account_name="Inventory Asset",
            account_number="1200",
            account_type="CURRENT_ASSET",
            is_active=True,
            as_of=timezone.localdate(),
        )
        self.cogs_acc = Account.objects.create(
            company=self.company,
            account_name="Cost of Goods Sold",
            account_number="5000",
            account_type="OPERATING_EXPENSE",
            is_active=True,
            as_of=timezone.localdate(),
        )
        self.ap_acc = Account.objects.create(
            company=self.company,
            account_name="Accounts Payable",
            account_number="2000",
            account_type="CURRENT_LIABILITY",
            detail_type="Accounts Payable (A/P)",
            is_active=True,
            as_of=timezone.localdate(),
        )
        self.ar_acc = Account.objects.create(
            company=self.company,
            account_name="Accounts Receivable",
            account_number="1100",
            account_type="CURRENT_ASSET",
            detail_type="Accounts Receivable (A/R)",
            is_active=True,
            as_of=timezone.localdate(),
        )
        self.income_acc = Account.objects.create(
            company=self.company,
            account_name="Sales Revenue",
            account_number="4000",
            account_type="OPERATING_INCOME",
            is_active=True,
            as_of=timezone.localdate(),
        )
        self.adj_acc = Account.objects.create(
            company=self.company,
            account_name="Inventory Adjustment",
            account_number="5100",
            account_type="OPERATING_EXPENSE",
            is_active=True,
            as_of=timezone.localdate(),
        )
        self.equity_acc = Account.objects.create(
            company=self.company,
            account_name="Opening Balance Equity",
            account_number="3000",
            account_type="OWNER_EQUITY",
            is_active=True,
            as_of=timezone.localdate(),
        )

        # Product
        self.product = Product.objects.create(
            company=self.company,
            name="Widget",
            type="Inventory",
            track_inventory=True,
            quantity=Decimal("0.00"),
            avg_cost=Decimal("0.00"),
            inventory_asset_account=self.inv_asset_acc,
            cogs_account=self.cogs_acc,
            income_account=self.income_acc,
        )

        # Supplier and customer
        self.supplier = Newsupplier.objects.create(
            company=self.company,
            company_name="Test Supplier",
            ap_account=self.ap_acc,
        )
        self.customer = Newcustomer.objects.create(
            company=self.company,
            customer_name="Test Customer",
            ar_account=self.ar_acc,
        )

        # Inventory location
        store, _ = MainStore.objects.get_or_create(
            company=self.company, name="Main",
            defaults={"is_active": True},
        )
        self.location, _ = InventoryLocation.objects.get_or_create(
            company=self.company, store=store, name="Default",
            defaults={"is_default": True, "is_active": True},
        )

    def _make_bill(self, qty, unit_cost, bill_no="BILL-001"):
        from expenses.models import Bill, BillItemLine
        bill = Bill.objects.create(
            company=self.company,
            supplier=self.supplier,
            bill_no=bill_no,
            bill_date=timezone.localdate(),
            total_amount=qty * unit_cost,
            location=self.location,
        )
        BillItemLine.objects.create(
            bill=bill,
            product=self.product,
            qty=qty,
            rate=unit_cost,
            amount=qty * unit_cost,
        )
        return bill

    def _make_invoice(self, qty, unit_price, inv_num=1):
        from sales.models import Newinvoice, InvoiceItem
        invoice = Newinvoice.objects.create(
            company=self.company,
            customer=self.customer,
            date_created=timezone.now(),
            location=self.location,
        )
        InvoiceItem.objects.create(
            invoice=invoice,
            product=self.product,
            qty=qty,
            unit_price=unit_price,
        )
        return invoice

    # ------------------------------------------------------------------
    # Test 1: Purchase creates Inventory Asset debit
    # ------------------------------------------------------------------
    def test_purchase_creates_inventory_asset_debit(self):
        from inventory.accounting import post_bill_to_gl
        from accounts.models import JournalLine

        bill = self._make_bill(Decimal("10"), Decimal("750"))
        post_bill_to_gl(bill)

        bill.refresh_from_db()
        self.assertTrue(bill.is_posted)
        self.assertIsNotNone(bill.journal_entry)

        # Check Inventory Asset was debited
        je = bill.journal_entry
        inv_lines = JournalLine.objects.filter(entry=je, account=self.inv_asset_acc)
        self.assertTrue(inv_lines.exists(), "No inventory asset debit line found")
        total_debit = sum(ln.debit for ln in inv_lines)
        self.assertEqual(total_debit, Decimal("7500.00"))

        # Check AP (supplier subledger) was credited
        ap_lines = JournalLine.objects.filter(entry=je, account__detail_type="Supplier Subledger (A/P)")
        self.assertTrue(ap_lines.exists(), "No AP/supplier subledger credit line found")

    # ------------------------------------------------------------------
    # Test 2: Purchase links movements to GL entry
    # ------------------------------------------------------------------
    def test_purchase_links_movements_to_gl_entry(self):
        from inventory.accounting import post_bill_to_gl
        from inventory.models import InventoryMovement

        bill = self._make_bill(Decimal("10"), Decimal("750"))
        post_bill_to_gl(bill)
        bill.refresh_from_db()

        movements = InventoryMovement.objects.filter(source_type="BILL", source_id=bill.id)
        self.assertTrue(movements.exists())
        for mv in movements:
            self.assertEqual(mv.gl_entry_id, bill.journal_entry_id,
                             "Movement not linked to its journal entry")
            self.assertTrue(mv.is_gl_posted, "Movement is_gl_posted not True")

    # ------------------------------------------------------------------
    # Test 3: Sale creates COGS debit + Inventory Asset credit at FIFO cost
    # ------------------------------------------------------------------
    def test_sale_creates_cogs_debit_and_inventory_credit(self):
        from inventory.accounting import post_bill_to_gl, post_invoice_inventory_and_gl
        from accounts.models import JournalLine

        # First, purchase stock at 750/unit
        bill = self._make_bill(Decimal("10"), Decimal("750"))
        post_bill_to_gl(bill)

        # Then sell 3 units at 1200/unit (selling price should NOT affect COGS)
        invoice = self._make_invoice(Decimal("3"), Decimal("1200"))
        post_invoice_inventory_and_gl(invoice)

        invoice.refresh_from_db()
        self.assertTrue(invoice.is_posted)
        je = invoice.journal_entry

        # COGS debit = 3 * 750 = 2250 (FIFO cost, not selling price)
        cogs_lines = JournalLine.objects.filter(entry=je, account=self.cogs_acc)
        self.assertTrue(cogs_lines.exists(), "No COGS debit line found")
        total_cogs = sum(ln.debit for ln in cogs_lines)
        self.assertEqual(total_cogs, Decimal("2250.00"),
                         "COGS should use FIFO cost (750), not selling price (1200)")

        # Inventory Asset credit = 2250
        inv_lines = JournalLine.objects.filter(entry=je, account=self.inv_asset_acc)
        self.assertTrue(inv_lines.exists(), "No inventory asset credit line found")
        total_inv_credit = sum(ln.credit for ln in inv_lines)
        self.assertEqual(total_inv_credit, Decimal("2250.00"))

    # ------------------------------------------------------------------
    # Test 4: COGS is at FIFO cost, not selling price
    # ------------------------------------------------------------------
    def test_cogs_at_fifo_cost_not_selling_price(self):
        from inventory.accounting import post_bill_to_gl, post_invoice_inventory_and_gl
        from accounts.models import JournalLine

        # Purchase at cost 500
        bill = self._make_bill(Decimal("5"), Decimal("500"), bill_no="BILL-FIFO")
        post_bill_to_gl(bill)

        # Sell at price 2000 (4x markup)
        invoice = self._make_invoice(Decimal("5"), Decimal("2000"))
        post_invoice_inventory_and_gl(invoice)

        je = invoice.journal_entry
        cogs_lines = JournalLine.objects.filter(entry=je, account=self.cogs_acc)
        total_cogs = sum(ln.debit for ln in cogs_lines)

        # COGS must be 5 * 500 = 2500, NOT 5 * 2000 = 10000
        self.assertEqual(total_cogs, Decimal("2500.00"),
                         "COGS must use purchase cost (500), not selling price (2000)")

    # ------------------------------------------------------------------
    # Test 5: Partial stock depletion calculates correct COGS
    # ------------------------------------------------------------------
    def test_partial_stock_depletion(self):
        from inventory.accounting import post_bill_to_gl, post_invoice_inventory_and_gl
        from accounts.models import JournalLine

        # Purchase 10 units at 750 each
        bill = self._make_bill(Decimal("10"), Decimal("750"), bill_no="BILL-PARTIAL")
        post_bill_to_gl(bill)

        # Sell only 4 units
        invoice = self._make_invoice(Decimal("4"), Decimal("1000"))
        post_invoice_inventory_and_gl(invoice)

        je = invoice.journal_entry
        cogs_lines = JournalLine.objects.filter(entry=je, account=self.cogs_acc)
        total_cogs = sum(ln.debit for ln in cogs_lines)
        self.assertEqual(total_cogs, Decimal("3000.00"),
                         "COGS for 4 units at 750 each = 3000")

    # ------------------------------------------------------------------
    # Test 6: Multiple FIFO layers consume oldest first
    # ------------------------------------------------------------------
    def test_multiple_batches_fifo_order(self):
        from datetime import date
        from inventory.accounting import post_bill_to_gl, post_invoice_inventory_and_gl
        from inventory.fifo import rebuild_layers_from_movements
        from expenses.models import Bill, BillItemLine
        from accounts.models import JournalLine

        # Batch 1: Buy 10 at 500 (older)
        bill1 = Bill.objects.create(
            company=self.company, supplier=self.supplier,
            bill_no="BILL-B1", bill_date=date(2026, 1, 1),
            total_amount=Decimal("5000"), location=self.location,
        )
        BillItemLine.objects.create(
            bill=bill1, product=self.product,
            qty=Decimal("10"), rate=Decimal("500"), amount=Decimal("5000"),
        )
        post_bill_to_gl(bill1)

        # Batch 2: Buy 10 at 800 (newer)
        bill2 = Bill.objects.create(
            company=self.company, supplier=self.supplier,
            bill_no="BILL-B2", bill_date=date(2026, 2, 1),
            total_amount=Decimal("8000"), location=self.location,
        )
        BillItemLine.objects.create(
            bill=bill2, product=self.product,
            qty=Decimal("10"), rate=Decimal("800"), amount=Decimal("8000"),
        )
        post_bill_to_gl(bill2)

        # Sell 15 units — should use 10@500 + 5@800 = 5000 + 4000 = 9000
        invoice = self._make_invoice(Decimal("15"), Decimal("2000"))
        post_invoice_inventory_and_gl(invoice)

        je = invoice.journal_entry
        cogs_lines = JournalLine.objects.filter(entry=je, account=self.cogs_acc)
        total_cogs = sum(ln.debit for ln in cogs_lines)
        self.assertEqual(total_cogs, Decimal("9000.00"),
                         "FIFO: 10@500 + 5@800 = 9000")

    # ------------------------------------------------------------------
    # Test 7: GL journal entry totals are balanced (debits == credits)
    # ------------------------------------------------------------------
    def test_gl_entry_is_balanced(self):
        from inventory.accounting import post_bill_to_gl
        from accounts.models import JournalLine

        bill = self._make_bill(Decimal("10"), Decimal("750"), bill_no="BILL-BAL")
        post_bill_to_gl(bill)

        bill.refresh_from_db()
        je = bill.journal_entry
        lines = JournalLine.objects.filter(entry=je)
        total_debit = sum(ln.debit for ln in lines)
        total_credit = sum(ln.credit for ln in lines)
        self.assertEqual(total_debit, total_credit,
                         f"Journal entry not balanced: DR={total_debit} CR={total_credit}")

    # ------------------------------------------------------------------
    # Test 8: Invoice GL entry is balanced
    # ------------------------------------------------------------------
    def test_invoice_gl_entry_is_balanced(self):
        from inventory.accounting import post_bill_to_gl, post_invoice_inventory_and_gl
        from accounts.models import JournalLine

        bill = self._make_bill(Decimal("10"), Decimal("750"), bill_no="BILL-INVBAL")
        post_bill_to_gl(bill)

        invoice = self._make_invoice(Decimal("5"), Decimal("1500"))
        post_invoice_inventory_and_gl(invoice)

        invoice.refresh_from_db()
        je = invoice.journal_entry
        lines = JournalLine.objects.filter(entry=je)
        total_debit = sum(ln.debit for ln in lines)
        total_credit = sum(ln.credit for ln in lines)
        self.assertEqual(total_debit, total_credit,
                         f"Invoice GL entry not balanced: DR={total_debit} CR={total_credit}")

    # ------------------------------------------------------------------
    # Test 9: Opening stock posts to Opening Balance Equity
    # ------------------------------------------------------------------
    def test_opening_stock_posts_to_opening_equity(self):
        from inventory.accounting import post_opening_stock_to_gl
        from accounts.models import JournalLine

        je = post_opening_stock_to_gl(
            product=self.product,
            qty=Decimal("20"),
            unit_cost=Decimal("600"),
            date=timezone.localdate(),
            company=self.company,
        )

        self.assertIsNotNone(je, "post_opening_stock_to_gl returned None")

        lines = JournalLine.objects.filter(entry=je)
        total_debit = sum(ln.debit for ln in lines)
        total_credit = sum(ln.credit for ln in lines)
        self.assertEqual(total_debit, Decimal("12000.00"))
        self.assertEqual(total_credit, Decimal("12000.00"))

        # Inventory Asset debited
        inv_lines = JournalLine.objects.filter(entry=je, account=self.inv_asset_acc)
        self.assertTrue(inv_lines.exists(), "No Inventory Asset debit for opening stock")

        # Opening Balance Equity credited
        eq_lines = JournalLine.objects.filter(entry=je, account=self.equity_acc)
        self.assertTrue(eq_lines.exists(), "No Opening Balance Equity credit for opening stock")

    # ------------------------------------------------------------------
    # Test 10: Opening stock updates product.opening_stock_value
    # ------------------------------------------------------------------
    def test_opening_stock_updates_product_fields(self):
        from inventory.accounting import post_opening_stock_to_gl
        from datetime import date

        cutoff = date(2026, 1, 1)
        post_opening_stock_to_gl(
            product=self.product,
            qty=Decimal("15"),
            unit_cost=Decimal("400"),
            date=cutoff,
            company=self.company,
        )

        self.product.refresh_from_db()
        self.assertEqual(self.product.opening_stock_value, Decimal("6000.00"))
        self.assertEqual(self.product.opening_stock_date, cutoff)

    # ------------------------------------------------------------------
    # Test 11: Stock adjustment (increase) posts correct GL
    # ------------------------------------------------------------------
    def test_stock_adjustment_increase_gl(self):
        from inventory.accounting import post_stock_adjustment_to_gl
        from inventory.models import StockAdjustment, StockAdjustmentLine
        from accounts.models import JournalLine

        adj = StockAdjustment.objects.create(
            company=self.company,
            reason="other",
            status="posted",
        )
        StockAdjustmentLine.objects.create(
            adjustment=adj,
            product=self.product,
            qty_increase=Decimal("5"),
            qty_decrease=Decimal("0"),
            unit_cost=Decimal("600"),
        )

        post_stock_adjustment_to_gl(adj)

        adj.refresh_from_db()
        self.assertIsNotNone(adj.journal_entry)

        je = adj.journal_entry
        lines = JournalLine.objects.filter(entry=je)
        total_debit = sum(ln.debit for ln in lines)
        total_credit = sum(ln.credit for ln in lines)
        self.assertEqual(total_debit, total_credit, "Adjustment GL not balanced")

        # Inventory Asset debited
        inv_lines = JournalLine.objects.filter(entry=je, account=self.inv_asset_acc)
        self.assertTrue(inv_lines.exists(), "No Inventory Asset debit on adjustment increase")
        total_inv_dr = sum(ln.debit for ln in inv_lines)
        self.assertEqual(total_inv_dr, Decimal("3000.00"))

    # ------------------------------------------------------------------
    # Test 12: Stock adjustment (decrease) posts COGS/expense debit at FIFO cost
    # ------------------------------------------------------------------
    def test_stock_adjustment_decrease_gl_at_fifo_cost(self):
        from inventory.accounting import post_bill_to_gl, post_stock_adjustment_to_gl
        from inventory.models import StockAdjustment, StockAdjustmentLine
        from accounts.models import JournalLine

        # First, build inventory at 750/unit
        bill = self._make_bill(Decimal("10"), Decimal("750"), bill_no="BILL-ADJ-DEC")
        post_bill_to_gl(bill)

        # Write off 3 units (damage)
        adj = StockAdjustment.objects.create(
            company=self.company,
            reason="damage",
            status="posted",
        )
        StockAdjustmentLine.objects.create(
            adjustment=adj,
            product=self.product,
            qty_increase=Decimal("0"),
            qty_decrease=Decimal("3"),
            unit_cost=Decimal("0"),  # FIFO cost used for decreases
        )

        post_stock_adjustment_to_gl(adj)

        adj.refresh_from_db()
        je = adj.journal_entry
        lines = JournalLine.objects.filter(entry=je)
        total_debit = sum(ln.debit for ln in lines)
        total_credit = sum(ln.credit for ln in lines)
        self.assertEqual(total_debit, total_credit, "Adjustment decrease GL not balanced")

        # Adjustment expense account debited at FIFO cost (3 * 750 = 2250)
        adj_lines = JournalLine.objects.filter(entry=je, account=self.adj_acc)
        self.assertTrue(adj_lines.exists(), "No adjustment expense debit found")
        total_adj_dr = sum(ln.debit for ln in adj_lines)
        self.assertEqual(total_adj_dr, Decimal("2250.00"),
                         "Adjustment decrease should debit at FIFO cost (3 * 750 = 2250)")

    # ------------------------------------------------------------------
    # Test 13: Inventory movements are linked to GL entries (audit trail)
    # ------------------------------------------------------------------
    def test_sale_movements_linked_to_gl_entry(self):
        from inventory.accounting import post_bill_to_gl, post_invoice_inventory_and_gl
        from inventory.models import InventoryMovement

        bill = self._make_bill(Decimal("10"), Decimal("750"), bill_no="BILL-AUDIT")
        post_bill_to_gl(bill)

        invoice = self._make_invoice(Decimal("5"), Decimal("1500"))
        post_invoice_inventory_and_gl(invoice)

        invoice.refresh_from_db()
        movements = InventoryMovement.objects.filter(source_type="INVOICE", source_id=invoice.id)
        self.assertTrue(movements.exists())
        for mv in movements:
            self.assertEqual(mv.gl_entry_id, invoice.journal_entry_id,
                             "Sale movement not linked to journal entry")
            self.assertTrue(mv.is_gl_posted)

    # ------------------------------------------------------------------
    # Test 14: Idempotent re-posting (re-posting replaces previous GL entry)
    # ------------------------------------------------------------------
    def test_repost_replaces_previous_gl_entry(self):
        from inventory.accounting import post_bill_to_gl
        from accounts.models import JournalEntry

        bill = self._make_bill(Decimal("10"), Decimal("750"), bill_no="BILL-IDEM")
        post_bill_to_gl(bill)
        bill.refresh_from_db()
        first_je_id = bill.journal_entry_id

        # Re-post the same bill
        post_bill_to_gl(bill)
        bill.refresh_from_db()
        second_je_id = bill.journal_entry_id

        self.assertNotEqual(first_je_id, second_je_id,
                            "Re-posting should create a new JE (old one deleted)")
        # Old JE should no longer exist
        self.assertFalse(JournalEntry.objects.filter(id=first_je_id).exists())

    # ------------------------------------------------------------------
    # Test 15: Product valuation_method and opening_stock_date fields exist
    # ------------------------------------------------------------------
    def test_product_new_fields_exist(self):
        from inventory.models import Product
        self.product.refresh_from_db()
        self.assertEqual(self.product.valuation_method, "FIFO")
        self.assertIsNone(self.product.opening_stock_value)
        self.assertIsNone(self.product.opening_stock_date)

    # ------------------------------------------------------------------
    # Test 16: InventoryMovement new fields default values
    # ------------------------------------------------------------------
    def test_movement_new_fields_defaults(self):
        from inventory.models import InventoryMovement
        mv = InventoryMovement.objects.create(
            product=self.product,
            company=self.company,
            location=self.location,
            date=timezone.localdate(),
            qty_in=Decimal("5"),
            qty_out=Decimal("0"),
            unit_cost=Decimal("100"),
            value=Decimal("500"),
            source_type="BILL",
            source_id=999,
        )
        self.assertIsNone(mv.gl_entry_id)
        self.assertFalse(mv.is_gl_posted)


# ===========================================================================
# Assembly Engine Tests
# ===========================================================================

class AssemblyEngineTests(TestCase):
    """
    Tests for the assembly module:
      - BOM creation and loading
      - Draft vs Completed behaviour
      - 2-step WIP GL posting
      - Cost accuracy (FIFO)
      - Assembly cancellation
      - Assembly reversal
      - Multi-location assembly
      - Component consumption report
    """

    def setUp(self):
        from tenancy.models import Company
        from inventory.models import Product, MainStore, InventoryLocation, InventoryMovement
        from inventory.fifo import record_purchase_layer
        from accounts.models import Account
        from datetime import date

        self.company = Company.objects.create(name="AssemblyTestCo", country="UG")

        store, _ = MainStore.objects.get_or_create(
            company=self.company, name="Main",
            defaults={"is_active": True},
        )
        self.location, _ = InventoryLocation.objects.get_or_create(
            company=self.company, store=store, name="Default",
            defaults={"is_default": True, "is_active": True},
        )

        # Create inventory asset account
        self.inv_account = Account.objects.create(
            company=self.company,
            account_name="Inventory Asset",
            account_number="1300",
            account_type="CURRENT_ASSET",
            detail_type="Inventory Asset",
            is_active=True,
        )

        # Create WIP account
        self.wip_account = Account.objects.create(
            company=self.company,
            account_name="Work In Progress",
            account_number="1410",
            account_type="CURRENT_ASSET",
            detail_type="Work In Progress (WIP)",
            is_active=True,
        )

        # Finished product
        self.finished = Product.objects.create(
            company=self.company,
            name="Assembled Widget",
            type="Inventory",
            track_inventory=True,
            inventory_asset_account=self.inv_account,
            quantity=Decimal("0.00"),
        )

        # Component A: 10 units @ 100 each
        self.comp_a = Product.objects.create(
            company=self.company,
            name="Component A",
            type="Inventory",
            track_inventory=True,
            inventory_asset_account=self.inv_account,
            quantity=Decimal("0.00"),
        )
        mv_a = InventoryMovement.objects.create(
            product=self.comp_a, company=self.company, location=self.location,
            date=date(2025, 1, 1), qty_in=Decimal("10"), qty_out=Decimal("0"),
            unit_cost=Decimal("100"), value=Decimal("1000"),
            source_type="BILL", source_id=1,
        )
        record_purchase_layer(self.comp_a, Decimal("100"), Decimal("10"), date(2025, 1, 1), mv_a, self.company)
        self.comp_a.quantity = Decimal("10")
        self.comp_a.save(update_fields=["quantity"])

        # Component B: 5 units @ 200 each
        self.comp_b = Product.objects.create(
            company=self.company,
            name="Component B",
            type="Inventory",
            track_inventory=True,
            inventory_asset_account=self.inv_account,
            quantity=Decimal("0.00"),
        )
        mv_b = InventoryMovement.objects.create(
            product=self.comp_b, company=self.company, location=self.location,
            date=date(2025, 1, 2), qty_in=Decimal("5"), qty_out=Decimal("0"),
            unit_cost=Decimal("200"), value=Decimal("1000"),
            source_type="BILL", source_id=2,
        )
        record_purchase_layer(self.comp_b, Decimal("200"), Decimal("5"), date(2025, 1, 2), mv_b, self.company)
        self.comp_b.quantity = Decimal("5")
        self.comp_b.save(update_fields=["quantity"])

    # -----------------------------------------------------------------------
    # 1. BOM creation and loading into build
    # -----------------------------------------------------------------------
    def test_bom_creation_and_load_into_build(self):
        from inventory.models import BillOfMaterials, BOMItem, Build
        from inventory.assembly_engine import load_bom_into_build

        bom = BillOfMaterials.objects.create(
            company=self.company, finished_product=self.finished, version=1, is_active=True,
        )
        BOMItem.objects.create(bom=bom, component_item=self.comp_a, quantity_required=Decimal("2"))
        BOMItem.objects.create(bom=bom, component_item=self.comp_b, quantity_required=Decimal("1"))

        build = Build.objects.create(
            company=self.company, finished_product=self.finished,
            build_qty=Decimal("1"), location=self.location, status="DRAFT",
        )
        load_bom_into_build(build, bom)

        self.assertEqual(build.lines.count(), 2)
        self.assertEqual(build.bom, bom)

    # -----------------------------------------------------------------------
    # 2. Draft build has no stock or GL impact
    # -----------------------------------------------------------------------
    def test_draft_build_no_stock_impact(self):
        from inventory.models import Build, BuildLine, InventoryMovement

        build = Build.objects.create(
            company=self.company, finished_product=self.finished,
            build_qty=Decimal("1"), location=self.location, status="DRAFT",
        )
        BuildLine.objects.create(build=build, component=self.comp_a, qty_per_unit=Decimal("2"))

        self.comp_a.refresh_from_db()
        # Draft — no movements yet
        self.assertEqual(self.comp_a.quantity, Decimal("10"))
        self.assertFalse(
            InventoryMovement.objects.filter(source_type="ASSEMBLY", source_id=build.id).exists()
        )

    # -----------------------------------------------------------------------
    # 3. Complete assembly — correct cost (FIFO)
    # -----------------------------------------------------------------------
    def test_complete_assembly_cost_accuracy(self):
        from inventory.models import Build, BuildLine
        from inventory.assembly_engine import complete_assembly

        # Build 2 units of finished, each needing 2×CompA + 1×CompB
        # Cost = 2 units × (2×100 + 1×200) = 2 × 400 = 800
        build = Build.objects.create(
            company=self.company, finished_product=self.finished,
            build_qty=Decimal("2"), location=self.location, status="DRAFT",
        )
        BuildLine.objects.create(build=build, component=self.comp_a, qty_per_unit=Decimal("2"))
        BuildLine.objects.create(build=build, component=self.comp_b, qty_per_unit=Decimal("1"))

        complete_assembly(build)

        build.refresh_from_db()
        self.assertEqual(build.status, "COMPLETED")
        self.assertEqual(build.total_cost, Decimal("800.00"))

        # Finished goods stock should increase by 2
        self.finished.refresh_from_db()
        self.assertEqual(self.finished.quantity, Decimal("2.00"))

        # Component A consumed 4, component B consumed 2
        self.comp_a.refresh_from_db()
        self.assertEqual(self.comp_a.quantity, Decimal("6.00"))  # 10 - 4

        self.comp_b.refresh_from_db()
        self.assertEqual(self.comp_b.quantity, Decimal("3.00"))  # 5 - 2

    # -----------------------------------------------------------------------
    # 4. Complete assembly — 2-step GL (WIP + FG journal entries)
    # -----------------------------------------------------------------------
    def test_complete_assembly_two_step_gl(self):
        from inventory.models import Build, BuildLine
        from inventory.assembly_engine import complete_assembly

        build = Build.objects.create(
            company=self.company, finished_product=self.finished,
            build_qty=Decimal("1"), location=self.location, status="DRAFT",
        )
        BuildLine.objects.create(build=build, component=self.comp_a, qty_per_unit=Decimal("2"))
        BuildLine.objects.create(build=build, component=self.comp_b, qty_per_unit=Decimal("1"))
        complete_assembly(build)

        build.refresh_from_db()
        # Both journal entries must exist
        self.assertIsNotNone(build.wip_journal_entry_id, "Step 1 (WIP) JE missing")
        self.assertIsNotNone(build.journal_entry_id, "Step 2 (FG) JE missing")

        # Step 1: WIP entry must have DR WIP and CR component accounts
        wip_je = build.wip_journal_entry
        wip_debits = sum(line.debit for line in wip_je.lines.all())
        wip_credits = sum(line.credit for line in wip_je.lines.all())
        self.assertEqual(wip_debits, wip_credits, "Step 1 JE is not balanced")

        # Step 2: FG entry must have DR FG and CR WIP
        fg_je = build.journal_entry
        fg_debits = sum(line.debit for line in fg_je.lines.all())
        fg_credits = sum(line.credit for line in fg_je.lines.all())
        self.assertEqual(fg_debits, fg_credits, "Step 2 JE is not balanced")

        # Both entries equal total cost
        total_cost = build.total_cost
        self.assertEqual(wip_debits, total_cost)
        self.assertEqual(fg_debits, total_cost)

    # -----------------------------------------------------------------------
    # 5. Cancel draft assembly
    # -----------------------------------------------------------------------
    def test_cancel_draft_assembly(self):
        from inventory.models import Build, BuildLine
        from inventory.assembly_engine import cancel_assembly

        build = Build.objects.create(
            company=self.company, finished_product=self.finished,
            build_qty=Decimal("1"), location=self.location, status="DRAFT",
        )
        BuildLine.objects.create(build=build, component=self.comp_a, qty_per_unit=Decimal("1"))

        cancel_assembly(build)

        build.refresh_from_db()
        self.assertEqual(build.status, "CANCELLED")

    def test_cancel_completed_assembly_raises(self):
        from inventory.models import Build, BuildLine
        from inventory.assembly_engine import cancel_assembly, complete_assembly

        build = Build.objects.create(
            company=self.company, finished_product=self.finished,
            build_qty=Decimal("1"), location=self.location, status="DRAFT",
        )
        BuildLine.objects.create(build=build, component=self.comp_a, qty_per_unit=Decimal("1"))
        complete_assembly(build)

        with self.assertRaises(ValueError):
            cancel_assembly(build)

    # -----------------------------------------------------------------------
    # 6. Reverse completed assembly — stock restored, GL reversed
    # -----------------------------------------------------------------------
    def test_reverse_assembly_restores_stock(self):
        from inventory.models import Build, BuildLine, InventoryMovement
        from inventory.assembly_engine import complete_assembly, reverse_assembly

        build = Build.objects.create(
            company=self.company, finished_product=self.finished,
            build_qty=Decimal("1"), location=self.location, status="DRAFT",
        )
        BuildLine.objects.create(build=build, component=self.comp_a, qty_per_unit=Decimal("2"))
        BuildLine.objects.create(build=build, component=self.comp_b, qty_per_unit=Decimal("1"))
        complete_assembly(build)

        # Capture stock before reversal
        self.comp_a.refresh_from_db()
        qty_a_before_reversal = self.comp_a.quantity  # should be 8

        reverse_assembly(build)

        build.refresh_from_db()
        self.assertEqual(build.status, "CANCELLED")
        self.assertIsNone(build.journal_entry_id)
        self.assertIsNone(build.wip_journal_entry_id)
        self.assertEqual(build.total_cost, Decimal("0.00"))

        # Stock movements for this assembly should be gone
        self.assertFalse(
            InventoryMovement.objects.filter(source_type="ASSEMBLY", source_id=build.id).exists()
        )

        # Component A stock restored to 10
        self.comp_a.refresh_from_db()
        self.assertEqual(self.comp_a.quantity, Decimal("10.00"))

        # Finished goods stock back to 0
        self.finished.refresh_from_db()
        self.assertEqual(self.finished.quantity, Decimal("0.00"))

    def test_reverse_non_completed_raises(self):
        from inventory.models import Build
        from inventory.assembly_engine import reverse_assembly

        build = Build.objects.create(
            company=self.company, finished_product=self.finished,
            build_qty=Decimal("1"), location=self.location, status="DRAFT",
        )
        with self.assertRaises(ValueError):
            reverse_assembly(build)

    # -----------------------------------------------------------------------
    # 7. Insufficient stock raises error
    # -----------------------------------------------------------------------
    def test_insufficient_stock_raises(self):
        from inventory.models import Build, BuildLine
        from inventory.assembly_engine import complete_assembly

        build = Build.objects.create(
            company=self.company, finished_product=self.finished,
            build_qty=Decimal("1"), location=self.location, status="DRAFT",
        )
        # Need 20 of CompA but only 10 available
        BuildLine.objects.create(build=build, component=self.comp_a, qty_per_unit=Decimal("20"))

        with self.assertRaises(ValueError):
            complete_assembly(build)

    # -----------------------------------------------------------------------
    # 8. Assembly number auto-generation
    # -----------------------------------------------------------------------
    def test_assembly_number_auto_generated(self):
        from inventory.models import Build

        build = Build.objects.create(
            company=self.company, finished_product=self.finished,
            build_qty=Decimal("1"), location=self.location, status="DRAFT",
        )
        self.assertIsNotNone(build.assembly_number)
        self.assertTrue(build.assembly_number.startswith("ASM-"))

    # -----------------------------------------------------------------------
    # 9. FIFO layer consumption correctness
    # -----------------------------------------------------------------------
    def test_fifo_layers_consumed_correctly(self):
        from inventory.models import Build, BuildLine
        from inventory.assembly_engine import complete_assembly
        from inventory.fifo import get_available_layers, record_purchase_layer
        from inventory.models import InventoryMovement
        from datetime import date

        # Add second batch of CompA at different cost: 5 units @ 150
        mv_a2 = InventoryMovement.objects.create(
            product=self.comp_a, company=self.company, location=self.location,
            date=date(2025, 3, 1), qty_in=Decimal("5"), qty_out=Decimal("0"),
            unit_cost=Decimal("150"), value=Decimal("750"),
            source_type="BILL", source_id=3,
        )
        record_purchase_layer(self.comp_a, Decimal("150"), Decimal("5"), date(2025, 3, 1), mv_a2, self.company)
        self.comp_a.quantity = Decimal("15")
        self.comp_a.save(update_fields=["quantity"])

        # Build consuming 12 of CompA: FIFO → 10@100 + 2@150 = 1300
        build = Build.objects.create(
            company=self.company, finished_product=self.finished,
            build_qty=Decimal("1"), location=self.location, status="DRAFT",
        )
        BuildLine.objects.create(build=build, component=self.comp_a, qty_per_unit=Decimal("12"))
        complete_assembly(build)

        build.refresh_from_db()
        # Cost = 10×100 + 2×150 = 1300
        self.assertEqual(build.total_cost, Decimal("1300.00"))

    # -----------------------------------------------------------------------
    # 10. Component consumption report
    # -----------------------------------------------------------------------
    def test_component_consumption_report(self):
        from inventory.models import Build, BuildLine
        from inventory.assembly_engine import complete_assembly, component_consumption_report

        build = Build.objects.create(
            company=self.company, finished_product=self.finished,
            build_qty=Decimal("1"), location=self.location, status="DRAFT",
        )
        BuildLine.objects.create(build=build, component=self.comp_a, qty_per_unit=Decimal("2"))
        complete_assembly(build)

        report = component_consumption_report(self.company)
        self.assertGreater(len(report), 0)
        comp_ids = {row["component"].id for row in report}
        self.assertIn(self.comp_a.id, comp_ids)

    # -----------------------------------------------------------------------
    # 11. BOM unique version per product constraint
    # -----------------------------------------------------------------------
    def test_bom_duplicate_version_raises(self):
        from inventory.models import BillOfMaterials
        from django.core.exceptions import ValidationError

        BillOfMaterials.objects.create(
            company=self.company, finished_product=self.finished, version=1, is_active=True,
        )
        with self.assertRaises(Exception):
            # Second BOM with same version should fail constraint
            BillOfMaterials.objects.create(
                company=self.company, finished_product=self.finished, version=1, is_active=False,
            )

    # -----------------------------------------------------------------------
    # 12. Load BOM replaces existing lines
    # -----------------------------------------------------------------------
    def test_load_bom_replaces_existing_lines(self):
        from inventory.models import BillOfMaterials, BOMItem, Build, BuildLine
        from inventory.assembly_engine import load_bom_into_build

        bom = BillOfMaterials.objects.create(
            company=self.company, finished_product=self.finished, version=1, is_active=True,
        )
        BOMItem.objects.create(bom=bom, component_item=self.comp_a, quantity_required=Decimal("3"))

        build = Build.objects.create(
            company=self.company, finished_product=self.finished,
            build_qty=Decimal("1"), location=self.location, status="DRAFT",
        )
        # Add existing manual line
        BuildLine.objects.create(build=build, component=self.comp_b, qty_per_unit=Decimal("2"))
        self.assertEqual(build.lines.count(), 1)

        # Load BOM — replaces existing
        load_bom_into_build(build, bom)
        self.assertEqual(build.lines.count(), 1)
        self.assertEqual(build.lines.first().component, self.comp_a)
        self.assertEqual(build.lines.first().qty_per_unit, Decimal("3"))
