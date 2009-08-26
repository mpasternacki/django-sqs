import base64

try:
    import json
except ImportError:
    try:
        import simplejson as json
    except ImportError:
        import django.utils.simplejson as json

import boto.sqs.message

from django.contrib.contenttypes.models import ContentType


class ModelInstanceMessage(boto.sqs.message.RawMessage):
    """SQS Message class that returns 
    """
    def __init__(self, queue=None, instance=None):
        boto.sqs.message.RawMessage.__init__(
            self, queue=queue, body=instance)

    def encode(self, value):
        ct = ContentType.objects.get_for_model(value)
        return base64.b64encode(
            json.dumps(
                (ct.app_label, ct.model, value.pk)))

    def decode(self, value):
        try:
            app_label, model, pk = json.loads(base64.b64decode(value))
        except Exception, e:
            self.__reason = "Error decoding payload: %s" % e
            return None
            
        try:
            ct = ContentType.objects.get(app_label=app_label, model=model)
        except ContentType.DoesNotExist:
            self.__reason = "Invalid content type."
            return None

        cls = ct.model_class()
        try:
            return cls.objects.get(pk=pk)
        except cls.DoesNotExist:
            self.__reason = "%s.%s %r does not exist" % (
                cls.__module__, cls.__name__, pk)
            return None

    def get_body(self):
        rv = boto.sqs.message.RawMessage.get_body(self)
        if rv is not None:
            return rv
        raise ValueError(self.__reason)

    def get_instance(self):
        return self.get_body()
