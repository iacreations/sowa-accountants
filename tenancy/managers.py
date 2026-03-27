from django.db import models



class TenantQuerySet(models.QuerySet):
    def for_company(self, company=None, user=None):
        """
        Always scope queries to the active company, unless user is superuser.
        Accepts either a Company instance or a company id.
        """
        if user is not None and getattr(user, "is_superuser", False):
            return self.all()
        if not company:
            return self.none()
        company_id = getattr(company, "id", company)
        return self.filter(company_id=company_id)


class TenantManager(models.Manager):
    def get_queryset(self):
        return TenantQuerySet(self.model, using=self._db)

    def for_company(self, company=None, user=None):
        return self.get_queryset().for_company(company=company, user=user)