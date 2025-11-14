from django.urls import path
from . import views


app_name='sales'
# my urls
urlpatterns = [  
# sales urls
    path('sales/', views.sales, name='sales'),
    path('sales/add/invoice', views.add_invoice, name='add-invoice'),
    path('sales/invoices/', views.invoice_list, name='invoices'),
 # invoice edit and view
    path("invoices/<int:pk>/", views.invoice_detail, name="invoice-detail"),
    path("invoices/<int:pk>/edit/", views.edit_invoice, name="edit-invoice"), 
    # invoice printout
    path("invoices/<int:pk>/print/", views.invoice_print, name="invoice-print"),
    
    # adding receipt urls
    path("add-receipt/", views.sales_receipt_new, name="add-receipt"),       
    path("sales-receipts/<int:pk>/", views.sales_receipt_detail, name="receipt-detail"),
    path("sales-receipts/<int:pk>/edit/", views.sales_receipt_edit, name="receipt-edit"),
    path("sales-receipts/", views.sales_receipt_list, name="sales-receipt-list"),
    path("sales-receipts/<int:pk>/print/", views.receipt_print, name="receipt-print"),
    
    # payment links 
    path("payments/<int:pk>/", views.payment_detail, name="payment-detail"),
   path("payments/<int:pk>/edit/", views.payment_edit, name="payment-edit"),
    path("sales/payments/", views.payments_list, name="payments_list"),
    path('sales/receive/payment', views.receive_payment_view, name='receive-payment'),
    path("add-class-ajax/", views.add_class_ajax, name="add_class_ajax"),
    path("receive-payment/outstanding.json", views.outstanding_invoices_api, name="outstanding_invoices_api"),
    # payment print
    path("payments/<int:pk>/print/", views.payment_print, name="payment-print"),
    
    # statements
    path("statements/new/", views.statement_new, name="statement-new"),
    path("statements/<int:pk>/", views.statement_detail, name="statement-detail"),
]
