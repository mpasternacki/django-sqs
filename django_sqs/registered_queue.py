import logging
import signal
import time

import boto.sqs.message

from django.conf import settings

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
                 timeout=None):
        self._connection = None
        self.name = name
        self.receiver = receiver
        self.visibility_timeout = visibility_timeout or DEFAULT_VISIBILITY_TIMEOUT
        self.message_class = message_class or boto.sqs.message.Message
        self.queue = None
        self.timeout = timeout

        if self.timeout and not self.receiver:
            raise ValueError("timeout is meaningful only with receiver")

        if not issubclass(self.message_class, boto.sqs.message.RawMessage):
            raise ValueError(
                "%s is not a subclass of boto.sqs.message.RawMessage"
                % self.message_class)

        prefix = getattr(settings, 'SQS_QUEUE_PREFIX', None)
        if prefix:
            self.full_name = '%s__%s' % (prefix, self.name)
        else:
            self.full_name = self.name

        self._log = logging.getLogger('django_sqs.queue.%s' % self.name)
        self._log.addHandler(_NullHandler())
        self._log.info("Using queue %s" % self.full_name)

    def get_connection(self):
        if self._connection is None:
            self._connection = boto.sqs.connection.SQSConnection(
                settings.AWS_ACCESS_KEY_ID,
                settings.AWS_SECRET_ACCESS_KEY,
                debug=boto_debug)
        return self._connection

    def get_queue(self):
        if self.queue is None:
            self.queue = self.get_connection().create_queue(
                self.full_name, self.visibility_timeout)
            self.queue.set_message_class(self.message_class)
        return self.queue

    def get_receiver_proxy(self):
        return self.ReceiverProxy(self)

    def send(self, message=None, **kwargs):
        q = self.get_queue()
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
                    

    def receive_single(self):
        """Receive single message from the queue.

        This method is here for debugging purposes.  It receives
        single message from the queue, processes it, deletes it from
        queue and returns (message, handler_result_value) pair.
        """
        q = self.get_queue()
        mm = q.get_messages(1)
        if mm:
            rv1 = self.receive(mm[0])
            q.delete_message(mm[0])
            return (mm[0], rv1)

    def receive_loop(self):
        q = self.get_queue()
        while True:
            mm = q.get_messages(1)
            if not mm:
                time.sleep(POLL_PERIOD)
            else:
                for m in mm:
                    try:
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
                        q.delete_message(m)
                    else:
                        q.delete_message(m)
