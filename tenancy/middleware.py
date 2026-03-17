from .models import Company, CompanyMember


class CompanyMiddleware:
    """
    Attaches:
      - request.company
      - request.in_client_workspace
      - request.in_sowa_workspace
      - request.workspace_mode
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def _get_user_default_company(self, user):
        owner_membership = (
            CompanyMember.objects
            .filter(user=user, is_active=True, role="OWNER", company__is_active=True)
            .select_related("company")
            .order_by("-created_at")
            .first()
        )
        if owner_membership:
            return owner_membership.company

        membership = (
            CompanyMember.objects
            .filter(user=user, is_active=True, company__is_active=True)
            .select_related("company")
            .order_by("-created_at")
            .first()
        )
        if membership:
            return membership.company

        return None

    def __call__(self, request):
        request.company = None
        request.in_client_workspace = False
        request.in_sowa_workspace = False
        request.workspace_mode = "sowa"

        if not request.user.is_authenticated:
            return self.get_response(request)

        is_staff_user = bool(request.user.is_staff or request.user.is_superuser)

        # ---------------- STAFF USERS ----------------
        if is_staff_user:
            active_company_id = request.session.get("active_company_id")

            if active_company_id:
                company = Company.objects.filter(id=active_company_id, is_active=True).first()
                if company:
                    request.company = company
                    request.in_client_workspace = True
                    request.in_sowa_workspace = False
                    request.workspace_mode = "client"
                    return self.get_response(request)

            request.session["active_company_id"] = None
            request.company = None
            request.in_client_workspace = False
            request.in_sowa_workspace = True
            request.workspace_mode = "sowa"
            return self.get_response(request)

        # ---------------- CLIENT USERS ----------------
        company = self._get_user_default_company(request.user)

        if not company:
            request.company = None
            request.in_client_workspace = False
            request.in_sowa_workspace = False
            request.workspace_mode = "client"
            return self.get_response(request)

        request.company = company
        request.in_client_workspace = True
        request.in_sowa_workspace = False
        request.workspace_mode = "client"

        return self.get_response(request)