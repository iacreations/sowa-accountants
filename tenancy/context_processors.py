def current_company(request):
    user = getattr(request, "user", None)

    can_manage_sowa_staff = False
    if user and user.is_authenticated:
        can_manage_sowa_staff = bool(user.is_superuser or getattr(user, "can_manage_staff", False))

    return {
        "current_company": getattr(request, "company", None),
        "workspace_mode": getattr(request, "workspace_mode", "sowa"),
        "in_client_workspace": getattr(request, "in_client_workspace", False),
        "in_sowa_workspace": getattr(request, "in_sowa_workspace", False),
        "can_manage_sowa_staff": can_manage_sowa_staff,
    }