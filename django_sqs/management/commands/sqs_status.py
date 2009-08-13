import django_sqs
import logging

from django.core.management.base import BaseCommand


from django.core.management.base import NoArgsCommand


class Command(NoArgsCommand):
    help = "Provides information about used SQS queues and the number of items in each of them."

    def handle_noargs(self, **options):
        logging.disable(logging.WARNING)
        print
        print "Active SQS queues"
        print "-----------------"
        print
        for label, queue in django_sqs.queues.items():
            q = queue.get_queue()
            print "%-30s: %4s" % (label, q.count())

