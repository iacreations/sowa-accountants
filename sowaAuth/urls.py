from django.urls import path
from django.contrib.auth import views as auth_views
from . import views

# app name
app_name='sowaAuth'
urlpatterns = [    
    path('', views.login_user, name='login'),
    path('register/', views.register_user, name='register'),
    path('logout/', views.logout_user, name='logout'),
    path('otp/', views.verify_otp, name='otp'),
    # path('password_reset/', views.password_reset, name='password_reset'),
    path("password-reset/", views.CustomPasswordResetView.as_view(), name="password_reset"),
    path("sowaAuth/password-reset/done/", views.CustomPasswordResetDoneView.as_view(), name="password_reset_done"),
    path("sowaAuth/reset/<uidb64>/<token>/", views.CustomPasswordResetConfirmView.as_view(), name="password_reset_confirm"),
    path("sowaAuth/reset/done/", views.CustomPasswordResetCompleteView.as_view(), name="password_reset_complete"),
]


