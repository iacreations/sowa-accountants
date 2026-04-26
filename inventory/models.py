from decimal import Decimal

from django.db import models
from django.utils import timezone
from django.core.exceptions import ValidationError

from tenancy.base import TenantModel

from accounts.models import Account
from sowaf.models import Newsupplier


class Category(TenantModel):
    category_type = models.CharField(max_length=255, null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["company", "category_type"]),
        ]

    def __str__(self):
        return self.category_type or "Category"


class Pclass(TenantModel):
    class_name = models.CharField(max_length=255, null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["company", "class_name"]),
        ]

    def __str__(self):
        return self.class_name or "Class"


class Product(TenantModel):
    PRODUCT_TYPES = [
        ("Inventory", "Inventory"),
        ("Non-Inventory", "Non-Inventory"),
        ("Service", "Service"),
        ("Bundle", "Bundle"),
    ]

    type = models.CharField(max_length=20, choices=PRODUCT_TYPES, blank=True, null=True)
    name = models.CharField(max_length=255, blank=True, null=True)
    sku = models.CharField(max_length=100, blank=True, null=True)

    category = models.ForeignKey(Category, on_delete=models.SET_NULL, blank=True, null=True)
    class_field = models.ForeignKey(Pclass, on_delete=models.SET_NULL, blank=True, null=True)

    sales_description = models.TextField(blank=True, null=True)
    purchase_description = models.TextField(blank=True, null=True)

    sell_checkbox = models.BooleanField(default=False, blank=True, null=True)
    purchase_checkbox = models.BooleanField(default=False, blank=True, null=True)

    sales_price = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    purchase_price = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)

    # cached values (truth is ledger)
    quantity = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    avg_cost = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))

    purchase_date = models.DateField(null=True, blank=True)
    taxable = models.BooleanField(default=False)

    supplier = models.ForeignKey(Newsupplier, on_delete=models.SET_NULL, null=True, blank=True)

    track_inventory = models.BooleanField(
        default=False,
        help_text="When checked, an Inventory Asset account is created in the COA and stock is tracked.",
    )

    is_bundle = models.BooleanField(default=False, blank=True, null=True)
    display_bundle_contents = models.BooleanField(default=False, blank=True, null=True)

    income_account = models.ForeignKey(
        Account,
        on_delete=models.SET_NULL,
        null=True, blank=True,
        related_name="income_products",
        limit_choices_to={"account_type__in": ["OPERATING_INCOME", "INVESTING_INCOME"]},
    )

    expense_account = models.ForeignKey(
        Account,
        on_delete=models.SET_NULL,
        null=True, blank=True,
        related_name="expense_products",
        limit_choices_to={
            "account_type__in": [
                "OPERATING_EXPENSE",
                "INVESTING_EXPENSE",
                "FINANCING_EXPENSE",
                "INCOME_TAX_EXPENSE",
            ]
        },
    )

    inventory_asset_account = models.ForeignKey(
        Account,
        on_delete=models.SET_NULL,
        null=True, blank=True,
        related_name="inventory_products",
        help_text="Where inventory value sits (Inventory Asset). Only needed for Inventory items.",
    )

    cogs_account = models.ForeignKey(
        Account,
        on_delete=models.SET_NULL,
        null=True, blank=True,
        related_name="cogs_products",
        limit_choices_to={
            "account_type__in": [
                "OPERATING_EXPENSE",
                "INVESTING_EXPENSE",
                "FINANCING_EXPENSE",
                "INCOME_TAX_EXPENSE",
            ]
        },
        help_text="Cost of Goods Sold account for Inventory items.",
    )

    class Meta:
        indexes = [
            models.Index(fields=["company", "name"]),
            models.Index(fields=["company", "type"]),
        ]
        constraints = [
            # sku unique per company (only when sku is not null/empty)
            models.UniqueConstraint(
                fields=["company", "sku"],
                name="uniq_product_sku_per_company",
                condition=models.Q(sku__isnull=False) & ~models.Q(sku=""),
            ),
        ]

    def clean(self):
        errors = {}

        if self.category_id and self.category.company_id != self.company_id:
            errors["category"] = "Category must belong to the same company."

        if self.class_field_id and self.class_field.company_id != self.company_id:
            errors["class_field"] = "Class must belong to the same company."

        if self.supplier_id and self.supplier.company_id != self.company_id:
            errors["supplier"] = "Supplier must belong to the same company."

        if self.income_account_id and getattr(self.income_account, "company_id", None) not in (None, self.company_id):
            errors["income_account"] = "Income account must belong to the same company."

        if self.expense_account_id and getattr(self.expense_account, "company_id", None) not in (None, self.company_id):
            errors["expense_account"] = "Expense account must belong to the same company."

        if self.inventory_asset_account_id and getattr(self.inventory_asset_account, "company_id", None) not in (None, self.company_id):
            errors["inventory_asset_account"] = "Inventory asset account must belong to the same company."

        if self.cogs_account_id and getattr(self.cogs_account, "company_id", None) not in (None, self.company_id):
            errors["cogs_account"] = "COGS account must belong to the same company."

        if errors:
            raise ValidationError(errors)

    def save(self, *args, **kwargs):
        self.full_clean(validate_constraints=False)
        super().save(*args, **kwargs)

    @property
    def stock_value(self):
        """Legacy: quantity × avg_cost (kept for backward compatibility)."""
        return (self.quantity or Decimal("0.00")) * (self.avg_cost or Decimal("0.00"))

    def get_fifo_value(self):
        """Return the value of on-hand stock using FIFO cost layers (oldest layer cost × qty)."""
        oldest = self.fifo_layers.filter(is_exhausted=False).order_by("date_created", "id").first()
        if oldest:
            return (self.quantity or Decimal("0.00")) * (oldest.unit_cost or Decimal("0.00"))
        return Decimal("0.00")

    def __str__(self):
        return self.name or "Product"


class BundleItem(models.Model):
    bundle = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="bundle_items")
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="used_in_bundle")
    quantity = models.PositiveIntegerField(default=1)

    class Meta:
        indexes = [
            models.Index(fields=["bundle", "product"]),
        ]

    def clean(self):
        errors = {}

        if self.bundle_id and self.product_id:
            if self.bundle.company_id != self.product.company_id:
                errors["product"] = "Bundle items must be within the same company."

            if not self.bundle.is_bundle:
                errors["bundle"] = "Selected bundle product must be marked as a bundle."

            if self.bundle_id == self.product_id:
                errors["product"] = "A bundle cannot include itself."

        if errors:
            raise ValidationError(errors)

    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)

    def __str__(self):
        b = self.bundle.name if self.bundle_id else "Bundle"
        p = self.product.name if self.product_id else "Product"
        return f"{b} -> {p} x{self.quantity}"


# ==========================================================
# MAIN STORE (per company)
# ==========================================================

class MainStore(TenantModel):
    """
    Represents a company's inventory store context.
    Usually one active record per company.
    """
    name = models.CharField(max_length=120, default="Main Store")
    is_active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-is_active", "name"]
        constraints = [
            models.UniqueConstraint(fields=["company", "name"], name="uniq_store_name_per_company"),
        ]
        indexes = [
            models.Index(fields=["company", "is_active"]),
        ]

    def __str__(self):
        return f"{self.name}"


class InventoryLocation(TenantModel):
    store = models.ForeignKey(
        MainStore,
        on_delete=models.PROTECT,
        related_name="locations",
        blank=True, null=True,
    )

    name = models.CharField(max_length=120, blank=True, null=True)
    is_default = models.BooleanField(default=False, blank=True, null=True)
    is_active = models.BooleanField(default=True, blank=True, null=True)

    class Meta:
        ordering = ["name"]
        constraints = [
            models.UniqueConstraint(fields=["company", "store", "name"], name="uniq_location_per_company_store"),
        ]
        indexes = [
            models.Index(fields=["company", "is_active"]),
            models.Index(fields=["company", "store"]),
        ]

    def clean(self):
        errors = {}

        if self.store_id and self.store.company_id != self.company_id:
            errors["store"] = "Store must belong to the same company."

        if errors:
            raise ValidationError(errors)

    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)

        # only one default location per store (inside company)
        if self.is_default and self.store_id:
            InventoryLocation.objects.filter(
                company=self.company,
                store=self.store
            ).exclude(id=self.id).update(is_default=False)

    def __str__(self):
        return self.name or "Location"


# -----------------------
# Ledger: Stock movements
# -----------------------

class InventoryMovement(TenantModel):
    SOURCE_TYPES = [
        ("INVOICE", "Invoice"),
        ("BILL", "Bill"),
        ("EXPENSE", "Expense"),
        ("CHEQUE", "Cheque"),
        ("ADJUSTMENT", "Adjustment"),
        ("TRANSFER", "Transfer"),
        ("OPENING", "Opening Stock"),
        ("SALES_RECEIPT", "Sales Receipt"),
        ("ASSEMBLY", "Assembly Build"),
    ]

    product = models.ForeignKey(
        Product,
        on_delete=models.CASCADE,
        related_name="movements",
        null=True, blank=True,
    )

    location = models.ForeignKey(
        InventoryLocation,
        on_delete=models.PROTECT,
        related_name="movements",
        null=True, blank=True,
    )

    date = models.DateField(default=timezone.localdate, null=True, blank=True)

    qty_in = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"), null=True, blank=True)
    qty_out = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"), null=True, blank=True)

    unit_cost = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"), null=True, blank=True)
    value = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"), null=True, blank=True)

    source_type = models.CharField(max_length=30, choices=SOURCE_TYPES, null=True, blank=True)
    source_id = models.IntegerField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True, null=True, blank=True)

    class Meta:
        ordering = ["date", "id"]
        indexes = [
            models.Index(fields=["company", "product", "location", "date"]),
            models.Index(fields=["company", "source_type", "source_id"]),
        ]

    def clean(self):
        errors = {}

        if self.product_id and self.product.company_id != self.company_id:
            errors["product"] = "Movement company must match product company."

        if self.location_id and self.location.company_id != self.company_id:
            errors["location"] = "Movement company must match location company."

        if self.qty_in and self.qty_in < 0:
            errors["qty_in"] = "qty_in cannot be negative."

        if self.qty_out and self.qty_out < 0:
            errors["qty_out"] = "qty_out cannot be negative."

        if self.qty_in and self.qty_out and self.qty_in > 0 and self.qty_out > 0:
            errors["qty_out"] = "A movement cannot have both qty_in and qty_out greater than zero."

        if errors:
            raise ValidationError(errors)

    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)

    def __str__(self):
        pname = self.product.name if self.product_id else "—"
        lname = self.location.name if self.location_id else "—"
        return f"{pname} @ {lname} +{self.qty_in} -{self.qty_out} ({self.source_type}#{self.source_id})"


class InventoryLayer(TenantModel):
    """
    FIFO cost layer.  Each purchase (or opening stock / assembly build) creates
    one layer.  Sales consume from the oldest layer first.
    """
    product = models.ForeignKey(
        Product,
        on_delete=models.CASCADE,
        related_name="fifo_layers",
    )
    unit_cost = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    qty_in = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"),
                                  help_text="Original purchased / received quantity")
    qty_remaining = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"),
                                         help_text="Units still available to be issued")
    source_movement = models.ForeignKey(
        InventoryMovement,
        on_delete=models.SET_NULL,
        null=True, blank=True,
        related_name="fifo_layer",
    )
    date_created = models.DateField(default=timezone.localdate,
                                     help_text="Date the layer was created (purchase / receipt date)")
    is_exhausted = models.BooleanField(default=False,
                                        help_text="True when qty_remaining reaches zero")

    class Meta:
        ordering = ["date_created", "id"]  # FIFO: oldest first
        indexes = [
            models.Index(fields=["company", "product", "date_created"]),
            models.Index(fields=["company", "product", "is_exhausted"]),
        ]

    def __str__(self):
        pname = self.product.name if self.product_id else "—"
        return f"Layer {self.id}: {pname} {self.qty_remaining}/{self.qty_in} @ {self.unit_cost}"


class StockTransfer(TenantModel):
    """
    Stock transfer header.
    Does NOT affect GL. Only inventory movement ledger.
    """
    from_location = models.ForeignKey(
        InventoryLocation,
        on_delete=models.PROTECT,
        related_name="stock_transfers_out",
        blank=True, null=True,
    )
    to_location = models.ForeignKey(
        InventoryLocation,
        on_delete=models.PROTECT,
        related_name="stock_transfers_in",
        blank=True, null=True,
    )

    transfer_date = models.DateField(default=timezone.localdate)
    memo = models.TextField(blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-transfer_date", "-id"]
        indexes = [
            models.Index(fields=["company", "transfer_date"]),
        ]

    def clean(self):
        errors = {}

        if self.from_location_id and self.from_location.company_id != self.company_id:
            errors["from_location"] = "from_location must belong to the same company."

        if self.to_location_id and self.to_location.company_id != self.company_id:
            errors["to_location"] = "to_location must belong to the same company."

        if self.from_location_id and self.to_location_id and self.from_location_id == self.to_location_id:
            errors["to_location"] = "Destination location must be different from source location."

        if errors:
            raise ValidationError(errors)

    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)

    def __str__(self):
        return f"Transfer #{self.id} {self.from_location} -> {self.to_location} ({self.transfer_date})"


class StockTransferLine(models.Model):
    transfer = models.ForeignKey(
        StockTransfer,
        on_delete=models.CASCADE,
        related_name="lines",
        blank=True, null=True,
    )
    product = models.ForeignKey(
        Product,
        on_delete=models.PROTECT,
        related_name="transfer_lines",
        blank=True, null=True,
    )
    qty = models.DecimalField(max_digits=12, null=True, blank=True, decimal_places=2, default=Decimal("0.00"))

    def clean(self):
        errors = {}

        if self.transfer_id and self.product_id:
            if self.transfer.company_id != self.product.company_id:
                errors["product"] = "StockTransferLine product must belong to same company as transfer."

        if self.qty is not None and self.qty <= 0:
            errors["qty"] = "Transfer quantity must be greater than zero."

        if errors:
            raise ValidationError(errors)

    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.product} x {self.qty} (Transfer {self.transfer_id})"


# ==========================================================
# ASSEMBLY / BUILD
# ==========================================================

class Build(TenantModel):
    """
    Represents an assembly build that converts raw materials (components)
    into a finished product. When completed:
      - Component quantities decrease
      - Finished product quantity increases at accumulated cost
      - GL: DR Finished Goods Inventory Asset, CR Component Inventory Assets
    """
    STATUS_CHOICES = [
        ("PENDING", "Pending"),
        ("COMPLETED", "Completed"),
    ]

    finished_product = models.ForeignKey(
        Product,
        on_delete=models.PROTECT,
        related_name="builds_as_finished",
        help_text="The product being assembled.",
    )
    build_qty = models.DecimalField(
        max_digits=12, decimal_places=2,
        default=Decimal("1.00"),
        help_text="How many units of the finished product to build.",
    )

    build_date = models.DateField(default=timezone.localdate)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="PENDING")
    completed_at = models.DateTimeField(null=True, blank=True)

    memo = models.TextField(blank=True, null=True)

    journal_entry = models.OneToOneField(
        "accounts.JournalEntry",
        on_delete=models.SET_NULL,
        null=True, blank=True,
        related_name="build_source",
    )

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-build_date", "-id"]
        indexes = [
            models.Index(fields=["company", "build_date"]),
            models.Index(fields=["company", "status"]),
        ]

    def clean(self):
        errors = {}
        if self.finished_product_id and self.finished_product.company_id != self.company_id:
            errors["finished_product"] = "Finished product must belong to the same company."
        if self.build_qty is not None and self.build_qty <= 0:
            errors["build_qty"] = "Build quantity must be greater than zero."
        if errors:
            raise ValidationError(errors)

    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)

    @property
    def total_component_cost(self):
        """Sum of (FIFO cost * qty_per_unit * build_qty) for all component lines."""
        total = Decimal("0.00")
        for line in self.lines.select_related("component").all():
            from inventory.fifo import compute_fifo_cogs
            qty = (line.qty_per_unit or Decimal("0.00")) * (self.build_qty or Decimal("1.00"))
            total += compute_fifo_cogs(line.component, qty)
        return total

    def __str__(self):
        name = self.finished_product.name if self.finished_product_id else "?"
        return f"Build #{self.id} – {name} x{self.build_qty} ({self.status})"


class BuildLine(models.Model):
    """
    A component (raw material) used in a build.
    qty_per_unit is the quantity of this component needed per ONE unit of the finished product.
    """
    build = models.ForeignKey(Build, on_delete=models.CASCADE, related_name="lines")
    component = models.ForeignKey(
        Product,
        on_delete=models.PROTECT,
        related_name="used_in_builds",
    )
    qty_per_unit = models.DecimalField(
        max_digits=12, decimal_places=2,
        default=Decimal("1.00"),
        help_text="Quantity of this component needed per unit of the finished product.",
    )

    class Meta:
        indexes = [
            models.Index(fields=["build", "component"]),
        ]

    def clean(self):
        errors = {}
        if self.build_id and self.component_id:
            if self.build.company_id != self.component.company_id:
                errors["component"] = "Component must belong to the same company."
            if self.build.finished_product_id == self.component_id:
                errors["component"] = "A build cannot use the finished product as a component."
        if self.qty_per_unit is not None and self.qty_per_unit <= 0:
            errors["qty_per_unit"] = "Quantity must be greater than zero."
        if errors:
            raise ValidationError(errors)

    def save(self, *args, **kwargs):
        self.full_clean()
        super().save(*args, **kwargs)

    @property
    def total_qty(self):
        """Total quantity consumed = qty_per_unit * build.build_qty."""
        return (self.qty_per_unit or Decimal("0.00")) * (self.build.build_qty or Decimal("1.00"))

    @property
    def total_cost(self):
        """Total FIFO cost for this component line."""
        from inventory.fifo import compute_fifo_cogs
        return compute_fifo_cogs(self.component, self.total_qty)

    @property
    def fifo_unit_cost(self):
        """FIFO unit cost = total_cost / total_qty (cost of oldest available layer)."""
        from inventory.fifo import get_available_layers
        layers = get_available_layers(self.component)
        if layers:
            return layers[0].unit_cost
        return Decimal("0.00")

    def __str__(self):
        c = self.component.name if self.component_id else "?"
        return f"{c} x {self.qty_per_unit}/unit (Build #{self.build_id})"