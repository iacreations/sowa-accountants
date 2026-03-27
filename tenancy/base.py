# tenancy/base.py
from django.db import models
from tenancy.managers import TenantManager


class TenantModel(models.Model):
    company = models.ForeignKey(
        "tenancy.Company",
        on_delete=models.CASCADE,
        related_name="+",
        db_index=True,
        null=True,
        blank=True,
    )

    objects = TenantManager()

    class Meta:
        abstract = True