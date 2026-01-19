from decimal import Decimal
from django.db import models
from django.utils import timezone

from accounts.models import Account
from sowaf.models import Newsupplier


class Category(models.Model):
    category_type = models.CharField(max_length=255)

    def __str__(self):
        return self.category_type


class Pclass(models.Model):
    class_name = models.CharField(max_length=255)

    def __str__(self):
        return self.class_name


class Product(models.Model):
    PRODUCT_TYPES = [
        ("Inventory", "Inventory"),
        ("Non-Inventory", "Non-Inventory"),
        ("Service", "Service"),
        ("Bundle", "Bundle"),
    ]

    type = models.CharField(max_length=20, choices=PRODUCT_TYPES,blank=True, null=True)
    name = models.CharField(max_length=255,blank=True, null=True)

    sku = models.CharField(max_length=100, blank=True, null=True)

    category = models.ForeignKey(Category, on_delete=models.CASCADE,blank=True, null=True)
    class_field = models.ForeignKey(Pclass, on_delete=models.CASCADE,blank=True, null=True)

    sales_description = models.TextField(blank=True, null=True)
    purchase_description = models.TextField(blank=True, null=True)

    sell_checkbox = models.BooleanField(default=False,blank=True, null=True)
    purchase_checkbox = models.BooleanField(default=False,blank=True, null=True)

    sales_price = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    purchase_price = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)

    quantity = models.IntegerField(null=True, blank=True, default=0)

    purchase_date = models.DateField(null=True, blank=True)
    taxable = models.BooleanField(default=False)

    supplier = models.ForeignKey(Newsupplier, on_delete=models.SET_NULL, null=True, blank=True)

    is_bundle = models.BooleanField(default=False,blank=True, null=True)
    display_bundle_contents = models.BooleanField(default=False,blank=True, null=True)

    #Link to CoA using YOUR account_type codes (not "Income"/"Cost of Sales")
    income_account = models.ForeignKey(
        Account,
        on_delete=models.SET_NULL,
        null=True, blank=True,
        related_name="income_products",
        limit_choices_to={"account_type__in": ["OPERATING_INCOME", "INVESTING_INCOME"]},
    )

    # This can be used for non-inventory expenses or as fallback
    expense_account = models.ForeignKey(
        Account,
        on_delete=models.SET_NULL,
        null=True, blank=True,
        related_name="expense_products",
        limit_choices_to={"account_type__in": ["OPERATING_EXPENSE", "INVESTING_EXPENSE", "FINANCING_EXPENSE", "INCOME_TAX_EXPENSE"]},
    )

    # Inventory accounting fields (for real stock + GL)
    inventory_asset_account = models.ForeignKey(
        Account,
        on_delete=models.SET_NULL,
        null=True, blank=True,
        related_name="inventory_products",
        limit_choices_to={"account_type": "CURRENT_ASSET"},
        help_text="Where inventory value sits (Inventory Asset). Only needed for Inventory items.",
    )

    cogs_account = models.ForeignKey(
        Account,
        on_delete=models.SET_NULL,
        null=True, blank=True,
        related_name="cogs_products",
        limit_choices_to={"account_type__in": ["OPERATING_EXPENSE", "INVESTING_EXPENSE", "FINANCING_EXPENSE", "INCOME_TAX_EXPENSE"]},
        help_text="Cost of Goods Sold account for Inventory items.",
    )

    def __str__(self):
        return self.name


class BundleItem(models.Model):
    bundle = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="bundle_items")
    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="used_in_bundle")
    quantity = models.PositiveIntegerField(default=1)

    def __str__(self):
        return f"{self.bundle.name} -> {self.product.name} x{self.quantity}"


# ✅ NEW: stock movement audit table
class InventoryMovement(models.Model):
    SOURCE_TYPES = [
        ("SALES_RECEIPT", "Sales Receipt"),
        ("INVOICE", "Invoice"),
        ("BILL", "Bill"),
        ("CHEQUE", "Cheque"),
        ("EXPENSE", "Expense"),
        ("ADJUSTMENT", "Adjustment"),
    ]

    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="movements")
    date = models.DateField(default=timezone.now)

    qty_in = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    qty_out = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))

    unit_cost = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))
    value = models.DecimalField(max_digits=12, decimal_places=2, default=Decimal("0.00"))

    source_type = models.CharField(max_length=30, choices=SOURCE_TYPES, blank=True, null=True)
    source_id = models.IntegerField(blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-date", "-id"]

    def __str__(self):
        return f"{self.product.name} in:{self.qty_in} out:{self.qty_out} ({self.source_type} #{self.source_id})"
