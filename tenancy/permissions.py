from functools import wraps
from django.shortcuts import redirect
from django.contrib import messages
from .models import CompanyMember


def is_sowa_user(user) -> bool:
    """
    SOWA (your internal admin users) = staff or superuser.
    They can access everything across companies.
    """
    if not user or not getattr(user, "is_authenticated", False):
        return False
    return bool(user.is_staff or user.is_superuser)


def get_membership(request):
    """
    Returns the active membership for the currently selected company.
    Assumes CompanyMiddleware sets request.company from session["company_id"].
    """
    company = getattr(request, "company", None)
    if not company or not request.user.is_authenticated:
        return None

    return (
        CompanyMember.objects
        .filter(company=company, user=request.user, is_active=True)
        .first()
    )


def company_required(view_func):
    @wraps(view_func)
    def _wrapped(request, *args, **kwargs):
        if not getattr(request, "company", None):
            messages.error(request, "Select a company first.")
            return redirect("tenancy:choose_company")
        return view_func(request, *args, **kwargs)
    return _wrapped


def module_required(module_key: str):
    module_key = (module_key or "").strip().lower()

    def decorator(view_func):
        @wraps(view_func)
        def _wrapped(request, *args, **kwargs):
            if not getattr(request, "company", None):
                messages.error(request, "Select a company first.")
                return redirect("tenancy:choose_company")

            # SOWA can access everything
            if is_sowa_user(request.user):
                return view_func(request, *args, **kwargs)

            membership = get_membership(request)
            if not membership:
                messages.error(request, "No active company access.")
                return redirect("tenancy:choose_company")

            if not membership.can_access(module_key):
                messages.error(request, "You are not allowed to access that section.")
                return redirect("sowaf:home")

            request.membership = membership
            return view_func(request, *args, **kwargs)
        return _wrapped
    return decorator


def role_required(roles):
    roles_set = {str(r).upper().strip() for r in (roles or []) if str(r).strip()}

    def decorator(view_func):
        @wraps(view_func)
        def _wrapped(request, *args, **kwargs):
            if not getattr(request, "company", None):
                messages.error(request, "Select a company first.")
                return redirect("tenancy:choose_company")

            # SOWA can access everything
            if is_sowa_user(request.user):
                return view_func(request, *args, **kwargs)

            membership = get_membership(request)
            if not membership:
                messages.error(request, "No active company access.")
                return redirect("tenancy:choose_company")

            if (membership.role or "").upper().strip() not in roles_set:
                messages.error(request, "Not allowed.")
                return redirect("sowaf:home")

            request.membership = membership
            return view_func(request, *args, **kwargs)
        return _wrapped
    return decorator