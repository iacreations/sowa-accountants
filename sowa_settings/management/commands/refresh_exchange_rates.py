"""
Daily exchange-rate refresh for every company whose currency is locked.

Usage:
    python manage.py refresh_exchange_rates

Schedule with cron / Windows Task Scheduler / Celery Beat to run once daily.
"""
from django.core.management.base import BaseCommand

from tenancy.models import Company
from sowa_settings.services import refresh_company_rates


class Command(BaseCommand):
    help = "Fetch latest exchange rates for all companies with a locked currency."

    def handle(self, *args, **options):
        companies = Company.objects.filter(currency_locked=True, is_active=True)
        total = 0
        for comp in companies:
            count = refresh_company_rates(comp)
            if count:
                self.stdout.write(
                    self.style.SUCCESS(f"  {comp.name}: {count} rates refreshed")
                )
                total += count
            else:
                self.stdout.write(
                    self.style.WARNING(f"  {comp.name}: no rates (API may be down)")
                )
        self.stdout.write(self.style.SUCCESS(f"Done — {total} rates across {companies.count()} companies."))
