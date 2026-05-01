"""
Migration: Enforce FIFO as the sole inventory valuation method.

1. Data migration: update all Product records with valuation_method='WEIGHTED_AVERAGE'
   to 'FIFO'.  No data is deleted; only the enum value is changed.
2. Schema change: restrict the valuation_method field choices to FIFO only.
"""

import logging

from django.db import migrations, models

logger = logging.getLogger(__name__)


def migrate_weighted_average_to_fifo(apps, schema_editor):
    """Convert all WEIGHTED_AVERAGE products to FIFO."""
    Product = apps.get_model("inventory", "Product")
    updated = Product.objects.filter(valuation_method="WEIGHTED_AVERAGE").update(
        valuation_method="FIFO"
    )
    if updated:
        logger.info(
            "FIFO migration: updated %d product(s) from WEIGHTED_AVERAGE to FIFO.",
            updated,
        )


class Migration(migrations.Migration):

    dependencies = [
        ("inventory", "0023_assembly_upgrade"),
    ]

    operations = [
        # Step 1: data migration – convert any WEIGHTED_AVERAGE records to FIFO.
        migrations.RunPython(
            migrate_weighted_average_to_fifo,
            reverse_code=migrations.RunPython.noop,
        ),
        # Step 2: schema change – restrict choices to FIFO only.
        migrations.AlterField(
            model_name="product",
            name="valuation_method",
            field=models.CharField(
                choices=[("FIFO", "FIFO (First In, First Out)")],
                default="FIFO",
                help_text=(
                    "Inventory costing method. FIFO is the only supported method. "
                    "Weighted Average is not supported."
                ),
                max_length=20,
            ),
        ),
    ]
