import django, os
os.environ['DJANGO_SETTINGS_MODULE'] = 'sowafinance.settings'
django.setup()

from tenancy.models import Company
from accounts.models import JournalLine, JournalEntry
from sowa_settings.models import Currency

for c in Company.objects.all():
    print(f"id={c.id} name={c.name} currency={c.currency} locked={c.currency_locked}")

for c in Company.objects.filter(currency_locked=True):
    jl_count = JournalLine.objects.filter(entry__company=c).count()
    cur_count = Currency.objects.filter(company=c).count()
    print(f"  Company {c.id}: {jl_count} journal lines, {cur_count} currency rows")
    # Show a few currency rates
    for cur in Currency.objects.filter(company=c, code__in=["UGX", "USD", "EUR"]):
        print(f"    {cur.code}: rate_to_home={cur.rate_to_home} is_home={cur.is_home}")
