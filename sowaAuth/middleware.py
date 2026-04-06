import logging

from django.contrib.auth import logout
from django.contrib.messages import get_messages
from django.shortcuts import redirect
from django.conf import settings

msg_logger = logging.getLogger("sowafinance.messages")


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


class MessageToLogMiddleware:
    """
    Intercepts all Django messages after the view runs, logs them to the
    'sowafinance.messages' logger, and clears them so they never appear
    on any template.
    """

    LEVEL_MAP = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "success": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
    }

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)

        # get_messages() also clears the message storage
        for message in get_messages(request):
            level = self.LEVEL_MAP.get(message.tags, logging.INFO)
            msg_logger.log(level, "[%s] %s", message.tags.upper() if message.tags else "MSG", message)

        return response
