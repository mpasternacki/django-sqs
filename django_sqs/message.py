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
        app_label, model, pk = json.loads(base64.b64decode(value))
        ct = ContentType.objects.get(app_label=app_label, model=model)
        return ct.get_object_for_this_type(pk=pk)

    def get_instance(self):
        return self.get_body()
