from optparse import make_option
import time

from django.conf import settings
from django.core.management.base import BaseCommand

import django_sqs

WAIT_TIME = getattr(settings, 'SQS_WAIT_TIME', 60)
WAIT_CYCLES = getattr(settings, 'SQS_WAIT_CYCLES', 1)

class Command(BaseCommand):
    help = "Wait until queues registered with django_sqs are empty" \
           " and all messages are processed."
    args = '[queue_name [queue_name [...]]]'

    option_list = BaseCommand.option_list + (
        make_option('--wait-time', '-t',
                    type='int', dest="wait_time", metavar='SECONDS',
                    default=WAIT_TIME,
                    help = "Time to sleep between queue count checks."
                    " Default is %d." % WAIT_TIME),
        make_option('--wait-cycles', '-c',
                    type='int', dest="wait_cycles", metavar='N',
                    default=WAIT_CYCLES,
                    help="Number of wait-time periods for which queues"
                    " must be empty to assume all messages have been"
                    " processed and stop waiting.  Default is %d, set"
                    " to 0 to stop waiting immediately when queue is empty"
                    % WAIT_CYCLES),
        )

    def handle(self, *queue_names, **options):
        self.validate()
        if not queue_names:
            queue_names = django_sqs.queues.keys()
        verbosity = int(options.get('verbosity', 1))

        empty_cycles = 0
        while True:
            empty = True
            for qname in queue_names:
                q = django_sqs.queues[qname].get_queue()
                c = q.count()
                if c > 0:
                    empty = False
                    if verbosity > 1:
                        print "Queue %s has %d messages, keep waiting." % (
                            qname, c)
                    break

            if empty:
                empty_cycles += 1
                if empty_cycles > options['wait_cycles']:
                    if verbosity > 1:
                        print '%d empty cycles, finishing.' % (empty_cycles - 1)
                    break
                else:
                    if verbosity > 1:
                        print '%d empty cycles, less than %d, still waiting.' % (
                            empty_cycles - 1, options['wait_cycles'])
            else:
                empty_cycles = 0

            time.sleep(options['wait_time'])
