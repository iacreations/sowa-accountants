from django.contrib import admin
from . models import CompanySettings,Currency
# Register your models here.
admin.site.register(CompanySettings)
admin.site.register(Currency)