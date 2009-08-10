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
    queues[queue_name] = RegisteredQueue(connection, queue_name, fn, **kwargs)

def receiver(queue_name, **kwargs):
    """Registers decorated function as SQS message receiver."""
# this seems to happen when a moduel with a receiver is imported
#     if queue_name in queues:
#         raise ValueError(
#             "Queue %s already received: %r" % (queue_name, queues[queue_name]))
    def _decorator(fn):
        register(queue_name, fn, **kwargs)
        return fn
    return _decorator

def send(queue_name, message=None, **kwargs):
    queues[queue_name].send(message, **kwargs)

