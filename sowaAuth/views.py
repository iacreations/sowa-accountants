from django.shortcuts import render, redirect
from django.contrib.auth.models import User
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.contrib.auth.hashers import make_password
from django.contrib import messages
from django.conf import settings
from .models import Newuser
from django.contrib.auth.hashers import make_password
from django.core.mail import EmailMessage
from django.utils import timezone
from django.urls import reverse
# Create your views here.
def register_user(request):
    if request.method=='POST':
        username=request.POST.get('username')
        email=request.POST.get('email')
        contact=request.POST.get('contact')
        password=request.POST.get('password')
        confirm_password=request.POST.get('confirm_password')
        
        # validating our information ie email and password
        user_data_has_error = False
        
        if Newuser.objects.filter(username=username).exists():
            user_data_has_error = True
            # creating an error message
            messages.error(request, "Username already exists")
        
        if Newuser.objects.filter(email=email).exists():
            user_data_has_error = True
            # creating the error message
            messages.error(request, "Account with this email already exists")
        # working on the length of a password
        if len(password) < 8:
            user_data_has_error = True
            messages.error(request, "Password must be atleast 8 characters")
        # validating the password
        if confirm_password != password:
            user_data_has_error = True
            messages.error(request, "Password does not match")
        
        # if datahas errors,, what should be done
        if user_data_has_error:
            return redirect('sowaAuth:register')
        # else what
        else:
            Newuser.objects.create(username=username,email=email,contact=contact,password=make_password(password))
            messages.success(request, "User created successfully")
            return redirect('login')
    return render(request, 'registration/register.html')
# login view

def login_user(request):
    if request.method == 'POST':
        username = request.POST.get('username')
        password = request.POST.get('password')

        user = authenticate(request, username=username, password=password)
        if user is not None:
            login(request, user)
            messages.success(request, "Login successful")
            return redirect('sowaf:home')
        else:
            messages.error(request, "Invalid credentials")

    return render(request, 'registration/login.html')
# logout view
def logout_user(request):
    logout(request)
    return redirect('sowaAuth:login')

# getting the otp
def verify_otp(request):
    return render(request, 'registration/otp.html')



# def login_user(request):
#     if request.method == 'POST':
#         username = request.POST.get('username')
#         password = request.POST.get('password')

#         user = authenticate(request, username=username, password=password)
#         if user is not None:
#             # ✅ Don't log in yet — generate OTP and store info
#             otp = str(random.randint(100000, 999999))
#             request.session['pending_user_id'] = user.id
#             request.session['otp'] = otp

#             # ✅ Send OTP to user's email (adjust to send SMS if needed)
#             send_mail(
#                 'Your Login OTP',
#                 f'Your OTP is: {otp}',
#                 'no-reply@sowa.com',
#                 [user.email],
#                 fail_silently=False
#             )

#             return redirect('sowaAuth:otp')
#         else:
#             messages.error(request, "Invalid credentials")

#     return render(request, 'registration/login.html')
# # getting the otp

# User = get_user_model()

# def verify_otp(request):
#     if request.method == 'POST':
#         entered_otp = request.POST.get('otp')
#         actual_otp = request.session.get('otp')
#         user_id = request.session.get('pending_user_id')

#         if entered_otp == actual_otp and user_id:
#             user = User.objects.get(id=user_id)
#             login(request, user)

#             # ✅ Clean up session
#             del request.session['otp']
#             del request.session['pending_user_id']

#             messages.success(request, "OTP verified. You are now logged in.")
#             return redirect('sowaf:home')
#         else:
#             messages.error(request, "Invalid OTP")

#     return render(request, 'registration/otp.html')


# def password_reset(request):
#     return render(request, 'registration/password_reset.html')

# sowaAuth/views.py
from django.contrib.auth import views as auth_views

class CustomPasswordResetView(auth_views.PasswordResetView):
    template_name = "sowaAuth/password_reset.html"
    email_template_name = "sowaAuth/password_reset_email.html"
    subject_template_name = "sowaAuth/password_reset_subject.txt"

class CustomPasswordResetDoneView(auth_views.PasswordResetDoneView):
    template_name = "sowaAuth/password_reset_done.html"

class CustomPasswordResetConfirmView(auth_views.PasswordResetConfirmView):
    template_name = "sowaAuth/password_reset_confirm.html"

class CustomPasswordResetCompleteView(auth_views.PasswordResetCompleteView):
    template_name = "sowaAuth/password_reset_complete.html"
