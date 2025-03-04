from django.urls import path
from . import views
from django.conf.urls.static import static
from django.conf import settings

urlpatterns = [
    path('', views.index, name='index'),
    path('bulk_email_verify', views.BulkEmailVerifyAPIView.as_view(), name='bulk_email_verify'),
    path('single_email_verify', views.SingleEmailVerifyAPIView.as_view(), name='single_email_verify'),
    path('single_email_verify_daily', views.DailySingleEmailVerifyAPIView.as_view(), name='single_email_verify_daily'),
    path('find_emails', views.EmailFinderAPIView.as_view(), name='email_finder_view'),
    path('create_api_key', views.CreateAPIKeyView.as_view(), name='create_api_key'),
    path('api/v1/verify', views.VerifyEmailsAPIView.as_view(), name='verfiy_through_api'),
    path('api/v1/results', views.BatchResultsStreamView.as_view(), name='status_through_api'),
    path('system_monitor', views.system_monitor, name='system_monitor'),
    path('api/v1/woo-credits-update', views.subscription_credits_update, name='subscription_credits_update'),
    path('test', views.test, name='test'),


    # 'process functions of apikey'

]

urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)



