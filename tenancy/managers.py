# tenancy/managers.py
from django.db import models


class TenantQuerySet(models.QuerySet):
    def for_company(self, company):
        """
        Always scope queries to the active company.
        """
        if not company:
            return self.none()
        return self.filter(company=company)


class TenantManager(models.Manager):
    def get_queryset(self):
        return TenantQuerySet(self.model, using=self._db)

    def for_company(self, company):
        return self.get_queryset().for_company(company)