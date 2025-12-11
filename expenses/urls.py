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
    path("add-expences/", views.add_expense, name="add-expenses"),  
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
    path("cheques/", views.cheque_list, name="cheque-list"),
    path("cheques/<int:pk>/", views.cheque_detail, name="cheque-detail"),
    path("cheques/<int:pk>/edit/", views.cheque_edit, name="cheque-edit"),

    # Purchase Orders
    path("purchase-order", views.purchase_order, name="purchase_order"),
    path("purchase-orders/", views.purchase_order_list, name="purchase-order-list"),
    path("purchase-orders/<int:pk>/", views.purchase_order_detail, name="purchase-order-detail"),
    path("purchase-orders/<int:pk>/edit/", views.purchase_order_edit, name="purchase-order-edit"),

    
    # Supplier Credit
    path("supplier-credit/add/", views.add_supplier_credit, name="supplier-credit"),
    path("supplier-credits/", views.supplier_credit_list, name="supplier-credit-list"),
    path("supplier-credits/<int:pk>/", views.supplier_credit_detail, name="supplier-credit-detail"),
    path("supplier-credits/<int:pk>/edit/", views.supplier_credit_edit, name="supplier-credit-edit"),

# pay down credit
    path("pay-down-credit/add/", views.add_paydown_credit, name="pay-down-credit"),
    path("pay-down-credits/", views.paydown_credit_list, name="paydown-credit-list"),
    path("pay-down-credit/<int:pk>/", views.paydown_credit_detail, name="paydown-credit-detail"),
    path("pay-down-credit/<int:pk>/edit/", views.paydown_credit_edit, name="paydown-credit-edit"),

# Credit Card Credit
    path("credit-card-credit/add/", views.add_credit_card_credit, name="credit-card"),
    path("credit-card-credits/", views.credit_card_credit_list, name="credit-card-credit-list"),
    path("credit-card-credit/<int:pk>/", views.credit_card_credit_detail, name="credit-card-credit-detail"),
    path("credit-card-credit/<int:pk>/edit/", views.credit_card_credit_edit, name="credit-card-credit-edit"),
# end
    path('expenses/import_bills', views.import_bills, name='import-bills'),

    
]
