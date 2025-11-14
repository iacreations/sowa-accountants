from django.contrib import admin
from .models import Statement, StatementLine
from . models import Newinvoice,InvoiceItem,Product,Payment,PaymentInvoice,SalesReceipt,SalesReceiptLine
# Register your models here
admin.site.register(Newinvoice),
admin.site.register(InvoiceItem),
admin.site.register(Product),
admin.site.register(Payment),
admin.site.register(PaymentInvoice)
admin.site.register(SalesReceipt)
admin.site.register(SalesReceiptLine)


class StatementLineInline(admin.TabularInline):
    model = StatementLine
    extra = 0

@admin.register(Statement)
class StatementAdmin(admin.ModelAdmin):
    list_display = ("id", "customer", "statement_type", "statement_date", "opening_balance", "closing_balance")
    inlines = [StatementLineInline]