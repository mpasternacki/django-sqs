import sys

from django.core.management.base import BaseCommand

import django_sqs


class Command(BaseCommand):
    help = "Clean Amazon SQS queues registered with django_sqs."
    args = '[queue_name [queue_name [...]]]'

    def handle(self, *queue_names, **options):
        self.validate()

        if not queue_names:
            queue_names = django_sqs.queues.keys()

        for queue_name in queue_names:
            q = django_sqs.queues[queue_name].get_queue()
            print 'Clearing queue %s...' % queue_name,
            sys.stdout.flush()
            n = q.clear()
            print '%d deleted.' % n

        
