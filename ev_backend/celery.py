import os
from celery import Celery

# Set Django settings module for celery
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "emailverifier.settings")
app = Celery("ev_backend") 
app.config_from_object("django.conf:settings", namespace="CELERY")

# Load settings from Django settings
# app.config_from_object("django.conf:settings", namespace="CELERY")

# Explicit Celery settings
# app.conf.update(
#     broker_url="redis://localhost:6379/0",  # Redis as broker
#     result_backend="redis://localhost:6379/1",  # Store results separately
#     broker_transport_options={"visibility_timeout": 3600},  # Prevent task loss
#     task_acks_late=True,  # Ensure tasks are retried on failure
#     broker_connection_retry_on_startup=True,  # Retry broker connection on start
# )

# Auto-discover tasks from all installed apps
app.autodiscover_tasks()

@app.task(bind=True)
def debug_task(self):
    print(f'Request: {self.request!r}')
