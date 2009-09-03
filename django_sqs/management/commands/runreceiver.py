import os
from optparse import make_option

from django.core.management.base import BaseCommand

import django_sqs


class Command(BaseCommand):
    help = "Run Amazon SQS receiver for queues registered with django_sqs."
    args = '[queue_name [queue_name [...]]]'
    option_list = BaseCommand.option_list + (
        make_option('--daemonize',
                    action='store_true', dest='daemonize', default=False,
                    help='Fork into background as a daemon.'),
        make_option('--daemon-stdout-log',
                    dest='stdout_log', default='/dev/null',
                    help="Log daemon's standard output stream to a file"),
        make_option('--daemon-stderr-log',
                    dest='stderr_log', default='/dev/null',
                    help="Log daemon's standard error stream to a file"),
        make_option('--pid-file',
                    dest='pid_file', default='',
                    help="Store process ID in a file"),
        )

    def handle(self, *queue_names, **options):
        self.validate()

        if not queue_names:
            queue_names = django_sqs.queues.keys()

        if options.get('daemonize', False):
            from django.utils.daemonize import become_daemon
            become_daemon(out_log=options.get('stdout_log', '/dev/null'),
                          err_log=options.get('stderr_log', '/dev/null'))

        if options.get('pid_file', ''):
            with open(options['pid_file'], 'w') as f:
                f.write('%d\n' % os.getpid())

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

        
