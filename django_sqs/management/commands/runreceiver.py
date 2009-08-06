import os

from django.core.management.base import BaseCommand

import django_sqs


class Command(BaseCommand):
    help = "Run Amazon SQS receiver for queues registered with django_sqs."
    args = '[queue_name [queue_name [...]]]'

    def handle(self, *queue_names, **options):
        self.validate()

        if not queue_names:
            queue_names = django_sqs.queues.keys()

        if len(queue_names) == 1:
            self.receive(queue_names[0])
        else:
            # fork a group of processes.  Quick hack, to be replaced
            # ASAP with something decent.
            os.setpgrp()
            pids = []
            for queue_name in queue_names:
                pid = os.fork()
                if not pid:
                    self.receive(queue_name)
                    return
                pids.append(pid)

            while pids:
                pid, status = os.wait()
                pids.remove(pid)

    def receive(self, queue_name):
        rq = django_sqs.queues[queue_name]
        if rq.receiver:
            print 'Receiving from queue %s...' % queue_name
            rq.receive_loop()
        else:
            print 'Queue %s has no receiver, aborting.' % queue_name

        
