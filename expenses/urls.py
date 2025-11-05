from django.urls import path
from . import views
app_name='expenses'
# my urls
urlpatterns = [  
# expenses urls
# all expenses
    path('expenses/', views.expenses, name='expenses'),
# expenses alone
    path("save-prefs/", views.save_column_prefs, name="save_column_prefs"),
    path("add-expences/", views.add_expense, name="add-expense"),  
    path("expences-list/", views.expense_list, name="expense-list"),                   
    path("<int:pk>/", views.expense_detail, name="expense-detail"),    
    path("<int:pk>/edit/", views.expense_edit, name="expense-edit"), 

    # time activity  
    path('expenses/add/time-activity', views.add_time_activity, name='time-activity'),

    # bill urls
    path('bills/add-bill', views.add_bill, name='add-bill'),
    path("bills/<int:pk>/edit/", views.edit_bill, name="bill-edit"),
    path("bills/", views.bills_list, name="bills-list"),
    path("bills/<int:pk>/", views.bill_detail, name="bill-detail"),

# cheque url

    path('expenses/add/cheque', views.add_cheque, name="add-cheque"),
# end
    path('expenses/supplier-credit', views.supplier_credit, name='supplier-credit'),
    path('expenses/add/purchase_order', views.purchase_order, name='purchase_order'),
    path('expenses/pay_down_credit', views.pay_down_credit, name='pay-down-credit'),
    path('expenses/import_bills', views.import_bills, name='import-bills'),
    path('expenses/credit_card', views.credit_card, name='credit-card'),
    
    path('expenses/add/expenses', views.add_expense, name='add-expenses'),
]
