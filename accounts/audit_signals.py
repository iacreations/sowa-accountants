# accounts/audit_signals.py

import json
import datetime
import sys
from decimal import Decimal

from django.db import connection
from django.db.utils import ProgrammingError, OperationalError
from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver
from django.db.models.fields.files import FieldFile
from django.contrib.sessions.models import Session  # NEW: skip sessions

from .models import AuditTrail


def safe_serialize(value):
    """
    Make sure values we put into JSONField are JSON-safe.
    Handles Decimal, date, datetime, file fields, etc.
    """
    # Decimals -> float (or string)
    if isinstance(value, Decimal):
        return float(value)

    # date / datetime -> ISO string
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()

    # file fields -> file name
    if isinstance(value, FieldFile):
        return value.name or None

    try:
        json.dumps(value)
        return value
    except TypeError:
        return str(value)


def build_changes_dict(instance):
    """
    Turn a model instance.__dict__ into a plain dict
    with only JSON-serializable values.
    """
    return {
        k: safe_serialize(v)
        for k, v in instance.__dict__.items()
        if not k.startswith("_")
    }


def _safe_int_pk(pk):
    """
    AuditTrail.object_id expects an integer.
    Session.pk is a string, and some models can have UUID/string PKs too.
    If pk can't be converted to int -> return None.
    """
    try:
        return int(pk)
    except (TypeError, ValueError):
        return None


def _is_running_migrations():
    """
    If Django is running migrate/makemigrations, skip audit logging.
    """
    return any(cmd in sys.argv for cmd in ["migrate", "makemigrations"])


def _audit_table_exists():
    """
    Check whether the audit table exists in the DB.
    During early migration steps, the DB/table may not exist yet.
    """
    try:
        return "accounts_audittrail" in connection.introspection.table_names()
    except (ProgrammingError, OperationalError):
        return False


def _should_skip_audit(sender):
    """
    Central place for all skip conditions.
    """
    # Don't log while running migrations (tables may not exist yet)
    if _is_running_migrations():
        return True

    # Don't log the audit model itself
    if sender is AuditTrail:
        return True

    # Don't log session writes (pk is string)
    if sender is Session:
        return True

    # If audit table isn't ready yet, skip (prevents Render migrate crash)
    if not _audit_table_exists():
        return True

    return False


@receiver(post_save)
def log_save(sender, instance, created, **kwargs):
    if _should_skip_audit(sender):
        return

    # Ensure object_id is an int (skip if not)
    obj_id = _safe_int_pk(getattr(instance, "pk", None))
    if obj_id is None:
        return

    changes_dict = build_changes_dict(instance)

    AuditTrail.objects.create(
        user=getattr(instance, "_audit_user", None),
        action="CREATE" if created else "UPDATE",
        model_name=sender.__name__,
        object_id=obj_id,
        description=f"{'CREATE' if created else 'UPDATE'} {sender.__name__}",
        old_data=None,
        new_data=changes_dict,
        ip_address=getattr(instance, "_audit_ip", None),
    )


@receiver(post_delete)
def log_delete(sender, instance, **kwargs):
    if _should_skip_audit(sender):
        return

    # Ensure object_id is an int (skip if not)
    obj_id = _safe_int_pk(getattr(instance, "pk", None))
    if obj_id is None:
        return

    AuditTrail.objects.create(
        user=getattr(instance, "_audit_user", None),
        action="DELETE",
        model_name=sender.__name__,
        object_id=obj_id,
        description=f"DELETE {sender.__name__}",
        old_data=build_changes_dict(instance),
        new_data=None,
        ip_address=getattr(instance, "_audit_ip", None),
    )
