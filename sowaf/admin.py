from django.contrib import admin
from . models import Newcustomer,Newsupplier,Newemployee,Newasset
# Register your models here
admin.site.register(Newcustomer)
admin.site.register(Newsupplier)
admin.site.register(Newemployee)
admin.site.register(Newasset)

