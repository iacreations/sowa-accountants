from django.db import models
from django.contrib.auth.models import AbstractUser

# Create your models here.
class Newuser(AbstractUser):
    username = models.CharField(max_length=150, unique=True)
    email = models.EmailField(unique=True)
    contact = models.CharField(max_length=15)
    password = models.CharField(max_length=128)
    def __str__(self):
        return f'user- {self.username} | user email- {self.email} | contact- {self.contact}'
