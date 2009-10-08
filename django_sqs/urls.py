from django.conf.urls.defaults import *

from views import status

urlpatterns = patterns('',
                       url(r'^status/$', status, name='sqs_status'),
                       )
