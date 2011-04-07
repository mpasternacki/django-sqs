import logging
import signal
import time
from warnings import warn

import boto.sqs.message

from django.conf import settings

try:
    from django.db import connections
    CONNECTIONS = connections.all()
except ImportError:
    from django.db import connection
    CONNECTIONS = (connection, )

class _NullHandler(logging.Handler):
    def emit(self, record):
        pass

if settings.DEBUG:
    boto_debug = 1
else:
    boto_debug=0

DEFAULT_VISIBILITY_TIMEOUT = getattr(
    settings, 'SQS_DEFAULT_VISIBILITY_TIMEOUT', 60)

POLL_PERIOD = getattr(
    settings, 'SQS_POLL_PERIOD', 10)


class TimedOut(Exception):
    """Raised by timeout handler."""
    pass

def sigalrm_handler(signum, frame):
    raise TimedOut()


class RestartLater(Exception):
    """Raised by receivers to stop processing and leave message in queue."""
    pass


class UnknownSuffixWarning(RuntimeWarning):
    """Unknown suffix passed to a registered queue"""
    pass


class RegisteredQueue(object):

    class ReceiverProxy(object):
        """Callable object that sends message via appropriate SQS queue.

        Available attributes:
        - direct - inner (decorated) function
        - registered_queue - a RegisteredQueue instance for this queue
        """
        def __init__(self, registered_queue):
            self.registered_queue = registered_queue
            self.direct = registered_queue.receiver

        def __call__(self, message=None, **kwargs):
            self.registered_queue.send(message, **kwargs)

    def __init__(self, name,
                 receiver=None, visibility_timeout=None, message_class=None,
                 timeout=None, delete_on_start=False, close_database=False,
                 suffixes=()):
        self._connection = None
        self.name = name
        self.receiver = receiver
        self.visibility_timeout = visibility_timeout or DEFAULT_VISIBILITY_TIMEOUT
        self.message_class = message_class or boto.sqs.message.Message
        self.queues = {}
        self.timeout = timeout
        self.delete_on_start = delete_on_start
        self.close_database = close_database
        self.suffixes = suffixes

        if self.timeout and not self.receiver:
            raise ValueError("timeout is meaningful only with receiver")

        if not issubclass(self.message_class, boto.sqs.message.RawMessage):
            raise ValueError(
                "%s is not a subclass of boto.sqs.message.RawMessage"
                % self.message_class)

        self.prefix = getattr(settings, 'SQS_QUEUE_PREFIX', None)

        self._log = logging.getLogger('django_sqs.queue.%s' % self.name)
        self._log.addHandler(_NullHandler())
        self._log.info("Using queue %s" % self.full_name())

    def full_name(self, suffix=None):
        name = self.name
        if suffix:
            if suffix not in self.suffixes:
                warn("Unknown suffix %s" % suffix, UnknownSuffixWarning)
            name = '%s__%s' % ( name, suffix )
        if self.prefix:
            return '%s__%s' % (self.prefix, name)
        else:
            return name

    def get_connection(self):
        if self._connection is None:
            self._connection = boto.sqs.connection.SQSConnection(
                settings.AWS_ACCESS_KEY_ID,
                settings.AWS_SECRET_ACCESS_KEY,
                debug=boto_debug)
        return self._connection

    def get_queue(self, suffix=None):
        if suffix not in self.queues:
            self.queues[suffix] = self.get_connection().create_queue(
                self.full_name(suffix), self.visibility_timeout)
            self.queues[suffix].set_message_class(self.message_class)
        return self.queues[suffix]

    def get_receiver_proxy(self):
        return self.ReceiverProxy(self)

    def send(self, message=None, suffix=None, **kwargs):
        q = self.get_queue(suffix)
        if message is None:
            message = self.message_class(**kwargs)
        else:
            if not isinstance(message, self.message_class):
                raise ValueError('%r is not an instance of %r' % (
                    message, self.message_class))
        q.write(message)

    def receive(self, message):
        if self.receiver is None:
            raise Exception("Not configured to received messages.")
        if self.timeout:
           signal.alarm(self.timeout)
           signal.signal(signal.SIGALRM, sigalrm_handler)
        try:
            self.receiver(message)
        finally:
            if self.timeout:
                try:
                    signal.alarm(0)
                    signal.signal(signal.SIGALRM, signal.SIG_DFL)
                except TimedOut:
                    # possible race condition if we don't cancel the
                    # alarm in time.  Now there is no race condition
                    # threat, since alarm already rang.
                    signal.alarm(0)
                    signal.signal(signal.SIGALRM, signal.SIG_DFL)
            if self.close_database:
                for connection in CONNECTIONS:
                    print 'Closing', connection
                    connection.close()


    def receive_single(self, suffix=None):
        """Receive single message from the queue.

        This method is here for debugging purposes.  It receives
        single message from the queue, processes it, deletes it from
        queue and returns (message, handler_result_value) pair.
        """
        q = self.get_queue(suffix)
        mm = q.get_messages(1)
        if mm:
            if self.delete_on_start:
                q.delete_message(mm[0])
            rv1 = self.receive(mm[0])
            if not self.delete_on_start:
                q.delete_message(mm[0])
            return (mm[0], rv1)

    def receive_loop(self, message_limit=None, suffix=None):
        """Run receiver loop.

        If `message_limit' number is given, return after processing
        this number of messages.
        """
        q = self.get_queue(suffix)
        i = 0
        while True:
            if message_limit:
                i += 1
                if i > message_limit:
                    return
            mm = q.get_messages(1)
            if not mm:
                time.sleep(POLL_PERIOD)
            else:
                for m in mm:
                    try:
                        if self.delete_on_start:
                            q.delete_message(m)
                        self.receive(m)
                    except KeyboardInterrupt, e:
                        raise e
                    except RestartLater:
                        self._log.debug("Restarting message handling")
                    except:
                        try:
                            body = repr(m.get_body())
                        except Exception, e:
                            body = "(cannot run %r.get_body(): %s)" % (m, e)
                        self._log.exception(
                            "Caught exception in receive loop for %s %s" % (
                                m.__class__, body))
                        if not self.delete_on_start:
                            q.delete_message(m)
                    else:
                        if not self.delete_on_start:
                            q.delete_message(m)
