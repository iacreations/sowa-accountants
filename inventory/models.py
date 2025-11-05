from django.db import models
from accounts.models import Account
from sowaf.models import Newsupplier
# Create your models here.
# category table
class Category(models.Model):
    category_type = models.CharField(max_length=255)
    def __str__(self):
        return self.category_type
    
# class table
class Pclass(models.Model):
    class_name = models.CharField(max_length=255)

    def __str__(self):
        return self.class_name
    
# product table
class Product(models.Model):
    PRODUCT_TYPES = [
        ('Inventory', 'Inventory'),
        ('Non-Inventory', 'Non-Inventory'),
        ('Service', 'Service'),
        ('Bundle', 'Bundle'),
    ]

    type = models.CharField(max_length=20, choices=PRODUCT_TYPES)
    name = models.CharField(max_length=255)
    quantity = models.CharField(max_length=255,blank=True, null=True)
    sku = models.CharField(max_length=100, blank=True, null=True)
    category = models.ForeignKey(Category, on_delete=models.CASCADE)
    class_field = models.ForeignKey(Pclass, on_delete=models.CASCADE)
    sales_description = models.TextField(blank=True, null=True)
    purchase_description = models.TextField(blank=True, null=True)
    sell_checkbox = models.BooleanField(default=False)
    sales_price = models.DecimalField(max_digits=100, decimal_places=2, 
    blank=True, null=True)
    purchase_price = models.DecimalField(max_digits=100, decimal_places=2, 
    blank=True, null=True)
    quantity = models.IntegerField(null=True, blank=True)
    purchase_date = models.DateField(null=True, blank=True)
    taxable = models.BooleanField(default=False)
    income_account = models.CharField(max_length=200,blank=True, null=True)
    expense_account = models.CharField(max_length=200,blank=True, null=True)
    supplier = models.ForeignKey(Newsupplier, on_delete=models.CASCADE)
    purchase_checkbox = models.BooleanField(default=False)
    is_bundle = models.BooleanField(default=False)
    display_bundle_contents = models.BooleanField(default=False)
    #  linking to the CoA
    income_account = models.ForeignKey(
        Account,
        on_delete=models.CASCADE,
        limit_choices_to={'account_type': 'Income'},  # only Income accounts can be picked
        related_name="income_products"
    )
    expense_account = models.ForeignKey(
        Account,
        on_delete=models.CASCADE,
        limit_choices_to={'account_type': 'Cost of Sales'},# only cost of sales accounts can be picked
        related_name="expense_products"
    )
    def __str__(self):
        return self.name

class BundleItem(models.Model):
    bundle = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="bundle_items")
    product = models.ForeignKey(Product, on_delete=models.CASCADE,related_name="used_in_bundle")
    quantity = models.PositiveIntegerField()

    def __str__(self):

        return self.bundle


