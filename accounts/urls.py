from django.urls import path
from . import views


app_name='accounts'
# my urls
urlpatterns = [  
    path('accounts/', views.accounts, name='accounts'),
    path('accounts/add/account', views.add_account, name='add-account'),
    path("accounts/<int:pk>/edit/", views.edit_account, name="edit-account"),
    path("accounts/<int:pk>/deactivate/", views.deactivate_account, name="deactivate-account"),
    path("accounts/<int:pk>/activate/", views.activate_account, name="activate-account"),
    # general ledger

    path("general-ledger/", views.general_ledger, name="general-ledger"),
    path("journal-entries/", views.journal_entries, name="journal-entries"),
    path("journal-entries/<int:pk>/", views.journal_entry_detail, name="journal-entry-detail"),
    path("general-ledger/print/", views.general_ledger_print, name="general-ledger-print"),
    # reports
    path(
        "reports/trial-balance/",views.report_trial_balance,      name="report-trial-balance",
    ),
    path("reports/pnl/", views.report_pnl, name="report-pnl"),

    path("reports/balance-sheet/", views.report_balance_sheet, name="report-balance-sheet"),

    path("reports/cashflow/", views.report_cashflow, name="report-cashflow"),
    # to save the customized columns
    path("save-prefs/", views.save_column_prefs, name="save_column_prefs"),

    path("audit-trail/", views.audit_trail, name="audit-trail"),

]