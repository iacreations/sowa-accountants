from django.urls import path
from . import views


app_name='accounts'
# my urls
urlpatterns = [  
    path('accounts/', views.accounts, name='accounts'),
    path('accounts/add/account', views.add_account, name='add-account'),
    path("accounts/<int:pk>/deactivate/", views.deactivate_account, name="deactivate-account"),
    path("accounts/<int:pk>/activate/", views.activate_account, name="activate-account"),
    # to save the customized columns
    path("save-prefs/", views.save_column_prefs, name="save_column_prefs"),
    # journal entries
    path("accounts/journal/", views.journal_list, name="journal_entries"),
    # trial balance
    path("reports/trial-balance/", views.trial_balance, name="report-trial-balance"),
    # proffits and loss
    path("accounts/reports/pnl/", views.report_pnl, name="report-pnl"),
#    balance sheet
    path("reports/balance-sheet/", views.report_bs, name="report-bs"),
#  cashflow
    path("reports/cashflow/", views.report_cashflow, name="cashflow"),
]