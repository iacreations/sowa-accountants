# tenancy/managers.py
from django.db import models


class TenantQuerySet(models.QuerySet):
    def for_company(self, company):
        """
        Always scope queries to the active company.
        Accepts either a Company instance or a company id.
        """
        if not company:
            return self.none()

        company_id = getattr(company, "id", company)
        return self.filter(company_id=company_id)


class TenantManager(models.Manager):
    def get_queryset(self):
        return TenantQuerySet(self.model, using=self._db)

    def for_company(self, company):
        return self.get_queryset().for_company(company)