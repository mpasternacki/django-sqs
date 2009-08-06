import time
import traceback

import boto.sqs.message

from django.conf import settings

DEFAULT_VISIBILITY_TIMEOUT = getattr(
    settings, 'SQS_DEFAULT_VISIBILITY_TIMEOUT', 60)

POLL_PERIOD = getattr(
    settings, 'SQS_POLL_PERIOD', 10)

class RegisteredQueue(object):

    def __init__(self, connection, name,
                 receiver=None, visibility_timeout=None, message_class=None):
        self.connection = connection
        self.name = name
        self.receiver = receiver
        self.visibility_timeout = visibility_timeout or DEFAULT_VISIBILITY_TIMEOUT
        self.message_class = message_class or boto.sqs.message.Message
        self.queue = None

        if not issubclass(self.message_class, boto.sqs.message.Message):
            raise ValueError(
                "%s is not a subclass of boto.sqs.message.Message"
                % self.message_class)

        if settings.DEBUG:
            self.full_name = '%s__%s' % (settings.SQS_QUEUE_PREFIX, self.name)
        else:
            self.full_name = self.name

    def get_queue(self):
        if self.queue is None:
            self.queue = self.connection.create_queue(
                self.full_name, self.visibility_timeout)
            self.queue.set_message_class(self.message_class)
        return self.queue

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
        self.receiver(message)

    def receive_loop(self):
        q = self.get_queue()
        while True:
            mm = q.get_messages(10)
            if not mm:
                time.sleep(POLL_PERIOD)
            else:
                for m in mm:
                    try:
                        self.receive(m)
                    except:
                        # FIXME: handle exception
                        traceback.print_exc()
                    else:
                        q.delete_message(m)
