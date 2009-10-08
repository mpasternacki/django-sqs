from django.http import HttpResponse

import django_sqs

def status(request):
    """Returns a simple plain text rendering of queue status."""
    response_text = []
    for label, queue in django_sqs.queues.items():
        q = queue.get_queue()
        response_text.append( "%s\t%s" % (label, q.count()) )
    
    return HttpResponse('\n'.join(response_text), mimetype='text/plain')
