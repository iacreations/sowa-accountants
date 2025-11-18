from django.db import models
from django.conf import settings


class CompanySettings(models.Model):
    """
    Global company settings for this company file.
    """
    company_name = models.CharField(max_length=255, blank=True, null=True)
    address = models.TextField(blank=True, null=True)
    email = models.EmailField(blank=True, null=True)
    phone = models.CharField(max_length=50, blank=True, null=True)

    # Home / reporting currency (e.g. UGX, USD)
    reporting_currency = models.CharField(max_length=10, blank=True, null=True)

    # Once True, reporting_currency cannot be changed via UI
    currency_locked = models.BooleanField(default=False)

    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="company_settings_created",
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        name = self.company_name or "Company Settings"
        cur = self.reporting_currency or "No currency"
        return f"{name} ({cur})"


class Currency(models.Model):
    """
    Currencies table (QuickBooks-style currency centre).
    rate_to_home = 1 unit of this currency in terms of the home currency.
    Example with home = UGX:
        USD rate_to_home = 3575.65 means 1 USD = 3575.65 UGX
    """
    code = models.CharField(max_length=10)             # USD, EUR, UGX…
    name = models.CharField(max_length=100)           # United States Dollar
    is_home = models.BooleanField(default=False)      # True only for the home currency
    rate_to_home = models.DecimalField(
        max_digits=18,
        decimal_places=6,
        default=1
    )
    is_active = models.BooleanField(default=True)
    last_updated = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("code", "is_home")

    def __str__(self):
        return f"{self.code} - {self.name}"
