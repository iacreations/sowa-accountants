from django.core.management.base import BaseCommand
from django.core.management import call_command


class Command(BaseCommand):
    help = 'Run FIFO migration setup: migrate + rebuild layers'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would happen without making changes',
        )

    def handle(self, *args, **options):
        dry_run = options.get('dry_run', False)

        self.stdout.write(self.style.SUCCESS('Starting FIFO Setup...'))

        try:
            # Step 1: Run migrations
            self.stdout.write(self.style.WARNING('Step 1: Running migrations...'))
            call_command('migrate', verbosity=2)
            self.stdout.write(self.style.SUCCESS('✅ Migrations completed successfully'))

            # Step 2: Rebuild FIFO layers
            self.stdout.write(self.style.WARNING('Step 2: Rebuilding FIFO layers...'))
            if dry_run:
                call_command('rebuild_inventory_fifo', dry_run=True, verbosity=2)
                self.stdout.write(self.style.SUCCESS('✅ FIFO rebuild (dry-run) completed'))
            else:
                call_command('rebuild_inventory_fifo', verbosity=2)
                self.stdout.write(self.style.SUCCESS('✅ FIFO layers rebuilt successfully'))

            self.stdout.write(self.style.SUCCESS('\n✅ FIFO Setup Complete!'))
            self.stdout.write(self.style.SUCCESS('Your inventory is now using FIFO costing.'))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Error: {str(e)}'))
            raise
