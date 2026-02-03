# sowaAuth/backends.py
from django.contrib.auth.backends import ModelBackend
from django.contrib.auth import get_user_model
from django.db.models import Q

User = get_user_model()

class UsernameEmailPhoneBackend(ModelBackend):
    """
    Allow authentication with username OR email OR contact
    """
    def authenticate(self, request, username=None, password=None, **kwargs):
        if username is None or password is None:
            return None

        try:
            user = User.objects.get(
                Q(username__iexact=username) |
                Q(email__iexact=username) |
                Q(contact__iexact=username)
            )
        except User.DoesNotExist:
            return None

        if user.check_password(password) and self.user_can_authenticate(user):
            return user
        return None
