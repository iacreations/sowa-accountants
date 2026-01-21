from django.urls import path
from . import views
from accounts.views import (
    aging_report,
    aging_report_detail,
    open_invoices_report,
    customer_balances_report,
    invoice_list_report,
    collections_report,
    aging_report_customer,
)

app_name='sales'
# my urls
urlpatterns = [  
# sales urls
    path('sales/', views.sales, name='sales'),
    path('sales/add/invoice', views.add_invoice, name='add-invoice'),
    path('sales/invoices/', views.invoice_list, name='invoices'),
    path("product-details/<int:pk>/", views.get_product_details, name="product-details"),
 # invoice edit and view
    path("invoices/<int:pk>/", views.invoice_detail, name="invoice-detail"),
    path("invoices/<int:pk>/edit/", views.edit_invoice, name="edit-invoice"), 
    # invoice printout
    path("invoices/<int:pk>/print/", views.invoice_print, name="invoice-print"),
    path("customer-credits/", views.customer_credits_list, name="customer-refunds-list"),
    path("customer-credits/<int:customer_id>/refund/new/", views.customer_refund_new, name="customer-refund-new"),
# Recurring invoices
    path("sales/recurring-invoices/", views.recurring_invoice_list, name="recurring-invoices"),
    path("sales/recurring-invoices/new/", views.recurring_invoice_new, name="recurring-invoice-new"),
    path("recurring-invoices/", views.recurring_invoice_list, name="recurring-invoices"),
    path("recurring-invoices/new/", views.recurring_invoice_new, name="recurring-invoice-new"),
    path("sales/recurring-invoices/run-today/", views.recurring_run_today, name="recurring-run-today"),

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
    path("statements/<int:pk>/export.xlsx", views.statement_export_excel, name="statement-export-excel"),
    path("statements/<int:pk>/export.pdf",   views.statement_export_pdf,   name="statement-export-pdf"),
    # aging reports
    path("reports/ar-aging/", aging_report, name="aging-report"),
    
    path("reports/ar-aging/detail/", aging_report_detail, name="aging-report-detail"),
    path("reports/open-invoices/", open_invoices_report, name="open-invoices-report"),
    path("reports/ar-aging/customer/<int:customer_id>/", aging_report_customer, name="aging-report-customer"),

    path("reports/customer-balances/", customer_balances_report, name="customer-balances-report"),
    path("reports/invoice-list/", invoice_list_report, name="invoice-list-report"),
    path("reports/collections/", collections_report, name="collections-report"),
    # Sales & Customers Reports
    # -----------------------------
    path("reports/sales-by-customer/", views.sales_by_customer_report, name="sales-by-customer-report"),
    
    path("reports/sales-by-customer/export/<str:fmt>/", views.sales_by_customer_export, name="sales-by-customer-export"),
    path("reports/sales-by-product/", views.sales_by_product_report, name="sales-by-product-report"),
    path("reports/sales-summary/", views.sales_summary_report, name="sales-summary-report"),
    path("reports/invoice-payments/", views.invoice_payments_report, name="invoice-payments-report"),

    path("reports/customer-statements/", views.customer_statements_report, name="customer-statements-report"),
    path("reports/sales-receipts/", views.sales_receipt_list_report, name="sales-receipt-list-report"),

    # exports for the two we implement now
    path("reports/customer-statements/export/<str:fmt>/", views.customer_statements_export, name="customer-statements-export"),
    path("reports/sales-receipts/export/<str:fmt>/", views.sales_receipt_list_export, name="sales-receipt-list-export"),
]
