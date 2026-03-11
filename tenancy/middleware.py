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

    SOWA_COMPANY_ID = 12

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        request.company = None
        request.workspace_mode = request.session.get("workspace_mode", "client")
        request.in_client_workspace = False
        request.in_sowa_workspace = False

        if not request.user.is_authenticated:
            return self.get_response(request)

        company_id = request.session.get("company_id")
        workspace_mode = request.session.get("workspace_mode")

        # if no mode set, infer it
        if not workspace_mode:
            if request.user.is_staff or request.user.is_superuser:
                request.session["workspace_mode"] = "sowa"
                request.workspace_mode = "sowa"

                if not company_id:
                    request.session["company_id"] = self.SOWA_COMPANY_ID
                    company_id = self.SOWA_COMPANY_ID
            else:
                request.session["workspace_mode"] = "client"
                request.workspace_mode = "client"
        else:
            request.workspace_mode = workspace_mode

        # if staff has no company selected, default to Sowa company
        if (request.user.is_staff or request.user.is_superuser) and not company_id:
            request.session["company_id"] = self.SOWA_COMPANY_ID
            company_id = self.SOWA_COMPANY_ID
            if request.workspace_mode != "client":
                request.session["workspace_mode"] = "sowa"
                request.workspace_mode = "sowa"

        if not company_id:
            return self.get_response(request)

        company = Company.objects.filter(id=company_id, is_active=True).first()
        if not company:
            request.session.pop("company_id", None)

            if request.user.is_staff or request.user.is_superuser:
                request.session["company_id"] = self.SOWA_COMPANY_ID
                request.session["workspace_mode"] = "sowa"
                request.workspace_mode = "sowa"
                company = Company.objects.filter(id=self.SOWA_COMPANY_ID, is_active=True).first()
                request.company = company
                request.in_sowa_workspace = True
                request.in_client_workspace = False
                return self.get_response(request)

            return self.get_response(request)

        # attach company for both Sowa workspace and client workspace
        if request.user.is_staff or request.user.is_superuser:
            request.company = company
            request.in_sowa_workspace = request.workspace_mode == "sowa"
            request.in_client_workspace = request.workspace_mode == "client"
            return self.get_response(request)

        # normal client users must belong to selected company
        is_member = CompanyMember.objects.filter(
            company=company,
            user=request.user,
            is_active=True
        ).exists()

        if not is_member:
            request.session.pop("company_id", None)
            request.company = None
            request.in_client_workspace = True
            request.in_sowa_workspace = False
            return self.get_response(request)

        request.company = company
        request.in_client_workspace = True
        request.in_sowa_workspace = False
        return self.get_response(request)