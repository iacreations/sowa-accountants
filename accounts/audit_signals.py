# # accounts/audit_signals.py

# import json
# import datetime
# from decimal import Decimal

# from django.conf import settings
# from django.db.models.signals import post_save, post_delete
# from django.dispatch import receiver
# from django.db.models.fields.files import FieldFile
# from django.contrib.sessions.models import Session  #  NEW: skip sessions

# # from .models import AuditTrail


# def safe_serialize(value):
#     """
#     Make sure values we put into JSONField are JSON-safe.
#     Handles Decimal, date, datetime, file fields, etc.
#     """
#     # Decimals -> float (or string)
#     if isinstance(value, Decimal):
#         return float(value)

#     # date / datetime -> ISO string
#     if isinstance(value, (datetime.date, datetime.datetime)):
#         return value.isoformat()

#     # file fields -> file name
#     if isinstance(value, FieldFile):
#         return value.name or None

#     try:
#         json.dumps(value)
#         return value
#     except TypeError:
#         return str(value)


# def build_changes_dict(instance):
#     """
#     Turn a model instance.__dict__ into a plain dict
#     with only JSON-serializable values.
#     """
#     return {
#         k: safe_serialize(v)
#         for k, v in instance.__dict__.items()
#         if not k.startswith("_")
#     }


# def _safe_int_pk(pk):
#     """
#     AuditTrail.object_id expects an integer.
#     Session.pk is a string, and some models can have UUID/string PKs too.
#     If pk can't be converted to int -> return None.
#     """
#     try:
#         return int(pk)
#     except (TypeError, ValueError):
#         return None


# @receiver(post_save)
# def log_save(sender, instance, created, **kwargs):
#     #  don’t log the audit model itself
#     if sender is AuditTrail:
#         return

#     #  IMPORTANT: don't log session saves (pk is a string)
#     if sender is Session:
#         return

#     #  ensure object_id is an int (skip if not)
#     obj_id = _safe_int_pk(getattr(instance, "pk", None))
#     if obj_id is None:
#         return

#     changes_dict = build_changes_dict(instance)

#     AuditTrail.objects.create(
#         user=getattr(instance, "_audit_user", None),
#         action="CREATE" if created else "UPDATE",
#         model_name=sender.__name__,
#         object_id=obj_id,  #  FIXED
#         description=f"{'CREATE' if created else 'UPDATE'} {sender.__name__}",
#         old_data=None,
#         new_data=changes_dict,
#         ip_address=getattr(instance, "_audit_ip", None),
#     )


# @receiver(post_delete)
# def log_delete(sender, instance, **kwargs):
#     if sender is AuditTrail:
#         return

#     #  don't log session deletes
#     if sender is Session:
#         return

#     #  ensure object_id is an int (skip if not)
#     obj_id = _safe_int_pk(getattr(instance, "pk", None))
#     if obj_id is None:
#         return

#     AuditTrail.objects.create(
#         user=getattr(instance, "_audit_user", None),
#         action="DELETE",
#         model_name=sender.__name__,
#         object_id=obj_id,  #  FIXED
#         description=f"DELETE {sender.__name__}",
#         old_data=build_changes_dict(instance),
#         new_data=None,
#         ip_address=getattr(instance, "_audit_ip", None),
#     )
