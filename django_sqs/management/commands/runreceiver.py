import logging
import os
from optparse import make_option
import signal

from django.core.management.base import BaseCommand

import django_sqs

# null handler to avoid warnings
class _NullHandler(logging.Handler):
    def emit(self, record):
        pass

# signal name dict for logging
_signals = {}
for name in dir(signal):
    if name.startswith('SIG'):
        _signals[getattr(signal, name)] = name
def _status_string(status):
    "Pretty status description for exited child."

    if os.WIFSIGNALED(status):
        return "Terminated by %s (%d)" % (
            _signals.get(os.WTERMSIG(status), "unknown signal"),
            os.WTERMSIG(status))

    if os.WIFEXITED(status):
        return "Exited with status %d" % os.WEXITSTATUS(status)

    if os.WIFSTOPPED(status):
        return "Stopped by %s (%d)" % (
            _signals.get(os.WSTOPSIG(status), "unknown signal"),
            os.WSTOPSIG(status))

    if os.WIFCONTINUED(status):
        return "Continued from stop"

    return "Unknown reason (%r)" % status

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
        make_option('--suffix', dest='suffix', default=None, metavar='SUFFIX',
                    help="Append SUFFIX to queue name."),
        make_option('--pid-file',
                    dest='pid_file', default='',
                    help="Store process ID in a file"),
        make_option('--message-limit',
                    dest='message_limit', default=None, type='int',
                    metavar='N', help='Exit after processing N messages'),
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
            self.receive(queue_names[0],
                         suffix=options.get('suffix'),
                         message_limit=options.get('message_limit', None))
        else:
            # fork a group of processes.  Quick hack, to be replaced
            # ASAP with something decent.
            _log = logging.getLogger('django_sqs.runreceiver.master')
            _log.addHandler(_NullHandler())

            # Close the DB connection now and let Django reopen it when it
            # is needed again.  The goal is to make sure that every
            # process gets its own connection
            from django.db import connection
            connection.close()

            os.setpgrp()
            children = {}               # queue name -> pid
            for queue_name in queue_names:
                pid = self.fork_child(queue_name,
                                      options.get('message_limit', None))
                children[pid] = queue_name
                _log.info("Forked %s for %s" % (pid, queue_name))

            while children:
                pid, status = os.wait()
                queue_name = children[pid]
                _log.error("Child %d (%s) exited: %s" % (
                    pid, children[pid], _status_string(status) ))
                del children[pid]

                pid = self.fork_child(queue_name)
                children[pid] = queue_name
                _log.info("Respawned %s for %s" % (pid, queue_name))

    def fork_child(self, queue_name, message_limit=None):
        pid = os.fork()
        if pid:                         # parent
            return pid
        # child
        _log = logging.getLogger('django_sqs.runreceiver.%s' % queue_name)
        _log.addHandler(_NullHandler())
        _log.info("Start receiving.")
        self.receive(queue_name, message_limit=message_limit)
        _log.error("CAN'T HAPPEN: exiting.")
        raise SystemExit(0)

    def receive(self, queue_name, message_limit=None, suffix=None):
        rq = django_sqs.queues[queue_name]
        if rq.receiver:
            if message_limit is None:
                message_limit_info = ''
            else:
                message_limit_info = ' %d messages' % message_limit

            print 'Receiving%s from queue %s%s...' % (
                message_limit_info, queue_name,
                ('.%s' % suffix if suffix else ''),
                )
            rq.receive_loop(message_limit=message_limit,
                            suffix=suffix)
        else:
            print 'Queue %s has no receiver, aborting.' % queue_name
