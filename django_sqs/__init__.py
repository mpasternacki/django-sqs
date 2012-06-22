import boto.sqs.connection

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from registered_queue import RegisteredQueue, TimedOut, RestartLater


# ensure settings are there
if not getattr(settings, 'AWS_ACCESS_KEY_ID'):
    raise ImproperlyConfigured('Missing setting "AWS_ACCESS_KEY_ID"')

if not getattr(settings, 'AWS_SECRET_ACCESS_KEY'):
    raise ImproperlyConfigured('Missing setting "AWS_SECRET_ACCESS_KEY"')

if settings.DEBUG and not getattr(settings, 'SQS_QUEUE_PREFIX'):
    raise ImproperlyConfigured('Missing setting "SQS_QUEUE_PREFIX"')

# Try to get regions, otherwise let to DefaultRegionName
# TODO this is bad! never set settings on the fly, better provide an
# app_settings.py with default values
if not getattr(settings, 'AWS_REGION'):
    settings.AWS_REGION = "us-east-1"


# ============
# registry
# ============
queues = {}


# ============
# convenience
# ============
def register(queue_name, fn=None, **kwargs):
    rv = RegisteredQueue(queue_name, fn, **kwargs)
    queues[queue_name] = rv
    return rv


def receiver(queue_name=None, **kwargs):
    """Registers decorated function as SQS message receiver."""
    def _decorator(fn):
        qn = queue_name or '%s__%s' % (
            fn.__module__.replace('.','__'), fn.__name__ )
        return register(qn, fn, **kwargs).get_receiver_proxy()
    return _decorator


def send(queue_name, message=None, suffix=None, **kwargs):
    queues[queue_name].send(message, suffix, **kwargs)
