# sowaAuth/urls.py
from django.urls import path
from . import views

app_name = "sowaAuth"

urlpatterns = [
    path("", views.auth_page, name="login"),         # single page
    path("register/", views.register_user, name="register"),
    path("do-login/", views.login_user, name="do_login"),
    path("logout/", views.logout_user, name="logout"),
]
