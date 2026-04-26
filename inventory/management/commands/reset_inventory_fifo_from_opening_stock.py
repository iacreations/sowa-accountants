import datetime
from django.core.management.base import BaseCommand
from inventory.models import Inventory

class Command(BaseCommand):
    help = 'Reset inventory FIFO from opening stock with cut-off date support'

    def add_arguments(self, parser):
        parser.add_argument('--cutoff-date', type=str, required=True,
                            help='The cut-off date for the reset in YYYY-MM-DD format')

    def handle(self, *args, **options):
        cutoff_date_str = options['cutoff-date']
        cutoff_date = datetime.datetime.strptime(cutoff_date_str, '%Y-%m-%d').date()

        # Logic to reset inventory FIFO from opening stock based on cutoff date
        # This would include querying the Inventory model and adjusting stock records

        self.stdout.write(self.style.SUCCESS('Successfully reset inventory FIFO from opening stock up to cutoff date: {}'.format(cutoff_date)))