from django.contrib import admin
from .models import Expense,ExpenseCategoryLine,ExpenseItemLine,Bill,BillCategoryLine,BillItemLine
# Register your models here.
admin.site.register(Expense)
admin.site.register(ExpenseCategoryLine)
admin.site.register(ExpenseItemLine)
admin.site.register(Bill)
admin.site.register(BillCategoryLine)
admin.site.register(BillItemLine)