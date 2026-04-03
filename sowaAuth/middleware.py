from django.contrib.auth import logout
from django.shortcuts import redirect
from django.conf import settings


class SessionSecurityMiddleware:
    """
    Enforces single-session-per-user:
    When a user logs in, their session_key is saved on the user model.
    On every request, if the current session_key doesn't match the stored one,
    the user is logged out (another browser took over).
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if request.user.is_authenticated:
            stored_key = getattr(request.user, "session_key", None)
            current_key = request.session.session_key

            if stored_key and current_key and stored_key != current_key:
                logout(request)
                return redirect(settings.LOGIN_URL)

        return self.get_response(request)
