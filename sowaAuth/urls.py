from django.urls import path
from . import views

app_name = "sowaAuth"

urlpatterns = [
    path("", views.auth_page, name="login"),
    path("do-login/", views.login_user, name="do_login"),
    path("staff/verify-otp/", views.staff_otp_verify, name="staff_otp_verify"),
    path("staff/invite/<str:token>/", views.accept_staff_invite, name="accept_invite"),
    path("staff/users/", views.staff_users, name="staff_users"),
    path("staff/users/invite/", views.staff_invite_create, name="staff_invite_create"),
    path("staff/users/<int:user_id>/toggle-active/", views.staff_toggle_active, name="staff_toggle_active"),
    path("staff/users/<int:user_id>/toggle-manager/", views.staff_toggle_manager, name="staff_toggle_manager"),
    path("logout/", views.logout_user, name="logout"),
]