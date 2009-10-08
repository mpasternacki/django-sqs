import time

from django.http import HttpResponse
from django.views.decorators.cache import cache_page

import django_sqs

@cache_page(5*60)
def status(request):
    """Returns a simple plain text rendering of queue status."""
    response_text = ['Queue status as of %s GMT\n' % time.strftime("%F %T", time.gmtime())]
    for label, queue in django_sqs.queues.items():
        q = queue.get_queue()
        response_text.append( "%s\t%s" % (label, q.count()) )
    
    return HttpResponse('\n'.join(response_text), mimetype='text/plain')
