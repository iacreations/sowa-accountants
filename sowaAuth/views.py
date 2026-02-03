# sowaAuth/views.py
from django.contrib import messages
from django.contrib.auth import authenticate, login, logout
from django.shortcuts import render, redirect
from django.contrib.auth import get_user_model
from django.db.models import Q

User = get_user_model()

def auth_page(request):
    """
    Single page that contains both Login + Signup tabs.
    """
    return render(request, "registration/auth.html")

def register_user(request):
    if request.method == "POST":
        username = (request.POST.get("username") or "").strip()
        email = (request.POST.get("email") or "").strip()
        contact = (request.POST.get("contact") or "").strip()
        password = request.POST.get("password") or ""
        confirm_password = request.POST.get("confirm_password") or ""

        user_data_has_error = False

        if not username:
            user_data_has_error = True
            messages.error(request, "Username is required")

        if User.objects.filter(username__iexact=username).exists():
            user_data_has_error = True
            messages.error(request, "Username already exists")

        if email and User.objects.filter(email__iexact=email).exists():
            user_data_has_error = True
            messages.error(request, "Account with this email already exists")

        if contact and User.objects.filter(contact__iexact=contact).exists():
            user_data_has_error = True
            messages.error(request, "Account with this phone number already exists")

        if len(password) < 8:
            user_data_has_error = True
            messages.error(request, "Password must be at least 8 characters")

        if confirm_password != password:
            user_data_has_error = True
            messages.error(request, "Password does not match")

        if user_data_has_error:
            # we return to the same auth page; JS will open signup tab
            return redirect("sowaAuth:login")

        user = User.objects.create_user(
            username=username,
            email=email if email else None,
            password=password
        )
        user.contact = contact if contact else None
        user.save()

        messages.success(request, "Account created successfully. Please login.")
        return redirect("sowaAuth:login")

    return redirect("sowaAuth:login")

def login_user(request):
    if request.method == "POST":
        username = (request.POST.get("username") or "").strip()  # can be username/email/contact
        password = request.POST.get("password") or ""

        user = authenticate(request, username=username, password=password)
        if user is not None:
            login(request, user)
            messages.success(request, "Login successful")
            return redirect("sowaf:home")
        else:
            messages.error(request, "Invalid credentials")
            return redirect("sowaAuth:login")

    return redirect("sowaAuth:login")

def logout_user(request):
    logout(request)
    return redirect("sowaAuth:login")
