# urls.py
from django.urls import path
from . import views

app_name = "tenancy"

urlpatterns = [
    path("choose/", views.choose_company, name="choose_company"),
    path("switch/<int:company_id>/", views.switch_company, name="switch_company"),
    path("exit/", views.exit_company, name="exit_company"),

    # Staff only
    path("clients/", views.company_list, name="company_list"),
    path("clients/add/", views.company_create, name="company_create"),

    # Invite + OTP
    path("invite/<str:token>/", views.client_invite, name="client_invite"),
    path("client/login/", views.client_otp_login, name="client_otp_login"),
    path("client/verify/", views.client_otp_verify, name="client_otp_verify"),

    # Client settings (users)
    path("settings/users/", views.client_settings_users, name="client_settings_users"),
    path("settings/users/invite/", views.client_invite_user, name="client_invite_user"),
    path("settings/users/<int:member_id>/role/", views.client_update_member_role, name="client_update_member_role"),
    path("settings/users/<int:member_id>/deactivate/", views.client_deactivate_member, name="client_deactivate_member"),
    path("settings/users/<int:member_id>/reactivate/", views.client_reactivate_member, name="client_reactivate_member"),
]