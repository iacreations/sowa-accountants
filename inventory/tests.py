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


