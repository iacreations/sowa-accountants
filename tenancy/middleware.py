from .models import Company, CompanyMember


class CompanyMiddleware:
    """
    Attaches:
      - request.company
      - request.workspace_mode
      - request.in_client_workspace
      - request.in_sowa_workspace

    Session keys:
      - company_id
      - workspace_mode  -> "sowa" or "client"
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def _get_sowa_company(self):
        return Company.objects.filter(company_kind="SOWA", is_active=True).order_by("id").first()

    def _get_user_company(self, user):
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
        request.workspace_mode = request.session.get("workspace_mode", "client")
        request.in_client_workspace = False
        request.in_sowa_workspace = False

        if not request.user.is_authenticated:
            return self.get_response(request)

        is_staff_user = bool(request.user.is_staff or request.user.is_superuser)
        company_id = request.session.get("company_id")
        workspace_mode = request.session.get("workspace_mode")
        sowa_company = self._get_sowa_company()

        if is_staff_user:
            if not workspace_mode:
                request.session["workspace_mode"] = "sowa"
                request.workspace_mode = "sowa"
            else:
                request.workspace_mode = workspace_mode

            if not company_id and sowa_company:
                request.session["company_id"] = sowa_company.id
                company_id = sowa_company.id
                if request.workspace_mode != "client":
                    request.session["workspace_mode"] = "sowa"
                    request.workspace_mode = "sowa"

            if company_id:
                company = Company.objects.filter(id=company_id, is_active=True).first()
                if not company and sowa_company:
                    request.session["company_id"] = sowa_company.id
                    request.session["workspace_mode"] = "sowa"
                    request.workspace_mode = "sowa"
                    company = sowa_company

                request.company = company

            request.in_sowa_workspace = request.workspace_mode == "sowa"
            request.in_client_workspace = request.workspace_mode == "client"
            return self.get_response(request)

        company = self._get_user_company(request.user)
        if not company:
            request.session.pop("company_id", None)
            request.session["workspace_mode"] = "client"
            request.workspace_mode = "client"
            request.company = None
            request.in_client_workspace = True
            request.in_sowa_workspace = False
            return self.get_response(request)

        if company_id != company.id:
            request.session["company_id"] = company.id

        request.session["workspace_mode"] = "client"
        request.workspace_mode = "client"

        request.company = company
        request.in_client_workspace = True
        request.in_sowa_workspace = False

        return self.get_response(request)