from .models import Company, CompanyMember


class CompanyMiddleware:
    """
    Attaches:
      - request.company
      - request.in_client_workspace
      - request.in_sowa_workspace
      - request.workspace_mode

    Session keys used consistently:
      - active_company_id
      - workspace_mode
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

    def _get_session_company(self, request):
        active_company_id = request.session.get("active_company_id")
        if not active_company_id:
            return None

        return Company.objects.filter(id=active_company_id, is_active=True).first()

    def _get_or_create_sowa_company(self):
        """
        Ensures there is always one active internal SOWA tenant.
        This keeps request.company non-null in SOWA workspace even after DB reset/flush.
        """
        sowa_company = (
            Company.objects
            .filter(company_kind="SOWA", is_active=True)
            .order_by("id")
            .first()
        )
        if sowa_company:
            return sowa_company

        return Company.objects.create(
            name="SOWA Internal",
            company_kind="SOWA",
            is_active=True,
        )

    def __call__(self, request):
        import logging
        logger = logging.getLogger("tenancy.middleware")
        request.company = None
        request.in_client_workspace = False
        request.in_sowa_workspace = False
        request.workspace_mode = "client"

        if not request.user.is_authenticated:
            logger.debug(f"[CompanyMiddleware] Unauthenticated user. No company set.")
            return self.get_response(request)

        is_staff_user = bool(request.user.is_staff or request.user.is_superuser)

        # ---------------- STAFF USERS ----------------
        if is_staff_user:
            company = self._get_session_company(request)
            logger.debug(f"[CompanyMiddleware] Staff user. Session company: {company}")

            if company:
                request.company = company
                request.in_client_workspace = True
                request.in_sowa_workspace = False
                request.workspace_mode = "client"
            else:
                # In SOWA workspace, always attach internal SOWA tenant.
                request.company = self._get_or_create_sowa_company()
                request.in_client_workspace = False
                request.in_sowa_workspace = True
                request.workspace_mode = "sowa"

            request.session["workspace_mode"] = request.workspace_mode
            logger.debug(f"[CompanyMiddleware] Staff user. Final company: {request.company}, workspace_mode: {request.workspace_mode}")
            return self.get_response(request)

        # ---------------- CLIENT USERS ----------------
        # First respect session-selected company
        company = self._get_session_company(request)
        logger.debug(f"[CompanyMiddleware] Client user. Session company: {company}")

        # Fallback to user's default company
        if not company:
            company = self._get_user_default_company(request.user)
            logger.debug(f"[CompanyMiddleware] Client user. Default company: {company}")
            if company:
                request.session["active_company_id"] = company.id

        if not company:
            logger.debug(f"[CompanyMiddleware] Client user. No company found. Returning.")
            request.company = None
            request.in_client_workspace = False
            request.in_sowa_workspace = False
            request.workspace_mode = "client"
            request.session["workspace_mode"] = "client"
            return self.get_response(request)

        # Ensure the selected company actually belongs to this client user
        has_membership = CompanyMember.objects.filter(
            company=company,
            user=request.user,
            is_active=True,
            company__is_active=True,
        ).exists()
        logger.debug(f"[CompanyMiddleware] Client user. Has membership: {has_membership}")

        if not has_membership:
            logger.debug(f"[CompanyMiddleware] Client user. No valid membership for company {company}. Returning.")
            request.company = None
            request.in_client_workspace = False
            request.in_sowa_workspace = False
            request.workspace_mode = "client"
            request.session["active_company_id"] = None
            request.session["workspace_mode"] = "client"
            return self.get_response(request)

        request.company = company
        request.in_client_workspace = True
        request.in_sowa_workspace = False
        request.workspace_mode = "client"
        request.session["workspace_mode"] = "client"
        logger.debug(f"[CompanyMiddleware] Client user. Final company: {request.company}, workspace_mode: {request.workspace_mode}")

        return self.get_response(request)