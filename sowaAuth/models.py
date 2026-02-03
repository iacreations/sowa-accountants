# sowaAuth/models.py
from django.contrib.auth.models import AbstractUser
from django.db import models

class Newuser(AbstractUser):
    contact = models.CharField(max_length=15, blank=True, null=True, unique=True)

    def __str__(self):
        return f"user- {self.username} | email- {self.email} | contact- {self.contact}"
