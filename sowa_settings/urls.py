from django.urls import path
from . import views


app_name='sowa_settings'
# my urls
urlpatterns = [
    path("settings/", views.company_settings_view, name="settings"),
]
