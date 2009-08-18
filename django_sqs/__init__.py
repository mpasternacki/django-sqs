import boto.sqs.connection

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from registered_queue import RegisteredQueue


# ensure settings are there

try:
    from settings import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
    if settings.DEBUG:
        from settings import SQS_QUEUE_PREFIX
except Exception, e:
    raise ImproperlyConfigured("Misconfigured: %s" % e)

# make connection

if settings.DEBUG:
    boto_debug = 1
else:
    boto_debug=0

connection = boto.sqs.connection.SQSConnection(
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
    debug=boto_debug)


# registry

queues = {}

# convenience

def register(queue_name, fn=None, **kwargs):
    rv = RegisteredQueue(connection, queue_name, fn, **kwargs)
    queues[queue_name] = rv
    return rv


def receiver(queue_name=None, **kwargs):
    """Registers decorated function as SQS message receiver."""
    def _decorator(fn):
        qn = queue_name or '%s__%s' % (
            fn.__module__.replace('.','__'), fn.__name__ )
        return register(qn, fn, **kwargs).get_receiver_proxy()
    return _decorator


def send(queue_name, message=None, **kwargs):
    queues[queue_name].send(message, **kwargs)
