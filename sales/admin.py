from django.contrib import admin
from . models import Newinvoice,InvoiceItem,Product,Payment,PaymentInvoice,SalesReceipt,SalesReceiptLine
# Register your models here
admin.site.register(Newinvoice),
admin.site.register(InvoiceItem),
admin.site.register(Product),
admin.site.register(Payment),
admin.site.register(PaymentInvoice)
admin.site.register(SalesReceipt)
admin.site.register(SalesReceiptLine)