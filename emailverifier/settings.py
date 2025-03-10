"""
Django settings for emailverifier project.

Generated by 'django-admin startproject' using Django 5.1.5.

For more information on this file, see
https://docs.djangoproject.com/en/5.1/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/5.1/ref/settings/
"""

from pathlib import Path
import os
from mongoengine import connect
from urllib.parse import quote_plus
from kombu import Exchange, Queue

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/5.1/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'django-insecure-f!oi(pdluesiid+-6i3@nujho=wbjix2s$23xl45=9k92fu)1_'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = ['*']
# Redis as the message broker
CELERY_BROKER_URL = "redis://localhost:6379/0"

# Store task results separately in Redis
CELERY_RESULT_BACKEND = "redis://localhost:6379/1"

# Celery configuration options
CELERY_BROKER_TRANSPORT_OPTIONS = {
    "visibility_timeout": 3600  # Prevent task loss
}

CELERY_TASK_ACKS_LATE = True  # Ensure tasks are retried if a worker crashes

CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP = True  # Retry broker connection on start

# CELERY SETTINGS
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TASK_SERIALIZER = "json"
# Define Celery Queues
CELERY_TASK_QUEUES = (
    Queue('default', Exchange('default'), routing_key='default'),
    Queue('high_priority', Exchange('high_priority'), routing_key='high_priority'),
)

# Default queue for tasks without a queue specified
CELERY_TASK_DEFAULT_QUEUE = 'default'
# Store Celery task results in Redis (optional)

USE_X_FORWARDED_HOST = True
# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'corsheaders',    
    'rest_framework',
    'rest_framework.authtoken', 
    'ev_backend'

]

MIDDLEWARE = [
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',    
    'django.middleware.clickjacking.XFrameOptionsMiddleware',

]

CORS_ALLOW_ALL_ORIGINS = True
CORS_ALLOW_CREDENTIALS = True
CORS_ALLOW_METHODS = [
    'GET',
    'POST',
    'PUT',
    'DELETE'
]

CORS_ALLOW_HEADERS = ['*']


ROOT_URLCONF = 'emailverifier.urls'
TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

# settings.py
CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_redis.core.RedisChannelLayer",
        "CONFIG": {
            "hosts": [("127.0.0.1", 6379,0)],
        },
    },
}

WSGI_APPLICATION = 'emailverifier.wsgi.application'
ASGI_APPLICATION = 'emailverifier.asgi.application'


# Database
# https://docs.djangoproject.com/en/5.1/ref/settings/#databases

# DATABASES = {
#     'default': {
#         'ENGINE': 'django.db.backends.sqlite3',
#         'NAME': BASE_DIR / 'db.sqlite3',
#     }
# }
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.dummy'  # Dummy engine (MongoEngine handles DB connection)
    }
}
# MONGO_URI =''
# MONGO_URI = "mongodb+srv://insideemails-mongo:OYTpyFVmm1o6nM0d@insideemails.5cqxj.mongodb.net/?retryWrites=true&w=majority&appName=insideemails"
# connect(
#     db="insideemails",  # Your MongoDB database name
#     host="mongodb+srv://insideemails-django:h_jupW!6kK9nn7V@insideemails.5cqxj.mongodb.net/?retryWrites=true&w=majority&appName=insideemails",  # Change if using a different host
#     alias="default",  # Default connection alias
# )
# URL encode the username and password
encoded_username = quote_plus("admin")
encoded_password = quote_plus("hc$#@xf44")
connect(
    db="insideemails",  # Your MongoDB database name
    host=f"mongodb://{encoded_username}:{encoded_password}@157.20.172.27:27017/?authSource=admin",  # Change if using a different host
    alias="default",  # Default connection alias
)
# DATABASES = {
#     'default': {
#         'ENGINE': 'django.db.backends.mysql',
#         'NAME': 'insideemails',
#         'USER': 'root',
#         'PASSWORD': 'hc$#@xf44',
#         'HOST': '127.0.0.1',
#         'PORT': '3306',
#         'OPTIONS': {
#             'sql_mode': 'STRICT_TRANS_TABLES',
#             'charset': 'utf8mb4',
#             'init_command': "SET default_storage_engine=INNODB",
#         },
#         'CONN_MAX_AGE': 60,  # Connection keep-alive for efficient reuse
#         'POOL_OPTIONS': {
#             'MAX_CONN': 3600,
#             'POOL_SIZE': 50,  # You can adjust based on your needs
#         },
#     }
# }
# Password validation
# https://docs.djangoproject.com/en/5.1/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]


# Internationalization
# https://docs.djangoproject.com/en/5.1/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/5.1/howto/static-files/

STATIC_URL = 'static/'
MEDIA_URL = "media/"
MEDIA_ROOT = os.path.join(BASE_DIR, "media")
BASE_URL = 'https://api.insideemails.com'
# Default primary key field type
# https://docs.djangoproject.com/en/5.1/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'
