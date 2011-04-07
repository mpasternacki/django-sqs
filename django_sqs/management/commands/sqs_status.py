from django.core.management.base import NoArgsCommand

import django_sqs


class Command(NoArgsCommand):
    help = "Provides information about used SQS queues and the number of items in each of them."

    def handle_noargs(self, **options):
        print
        print "Active SQS queues"
        print "-----------------"
        print
        for label, queue in django_sqs.queues.items():
            q = queue.get_queue()
            print "%-30s: %4s" % (label, q.count())
            if queue.suffixes:
                for suffix in queue.suffixes:
                    q = queue.get_queue(suffix)
                    print " .%-28s: %4s" % (suffix, q.count())
