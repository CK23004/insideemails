from django.db import models
import uuid
from django.contrib.auth.hashers import make_password, check_password

# Create your models here.
from mongoengine import (
    Document, UUIDField, StringField, IntField, DictField, DateTimeField, EmailField, BooleanField
)
from django.utils import timezone
class UserProfile(Document):
    created_at = DateTimeField(default=timezone.now)
    wpuser_id = IntField(unique=True, required=True)
    api_key = StringField(max_length=32, unique=True, default=None)

    def __str__(self):
        return self.wpuser_id

    meta = {
        'collection': 'userprofile',  # Optional: Defines MongoDB collection name
        'ordering': ['-created_at']
    }


class BatchTask(Document):
    id = UUIDField(binary=False, default=uuid.uuid4, unique=True)
    batch_id = StringField(max_length=255, primary_key=True)  # Unique identifier
    total = IntField(required=True)  # Total emails
    completed = IntField(default=0)  # Completed emails
    output_file_name = StringField(max_length=500, null=True, blank=True)  # Output file path
    initial_count = IntField(required=True)  # Initial email count
    status = StringField(max_length=50, default="pending")  # Task status
    spam_block_retries = IntField(null=True, blank=True)
    valid_email_retries = IntField(null=True, blank=True)
    post_id = IntField(null=True, blank=True)  # WooCommerce Post ID
    wpuser_id = IntField(null=True, blank=True)  # WordPress User ID
    service_type = StringField(max_length=100, null=True, blank=True)  # Service type
    results = DictField(default=dict)  # Store email results
    created_at = DateTimeField(default=timezone.now)
    def __str__(self):
        return f"BatchTask {self.batch_id} - {self.status}"
    meta = {
        'collection': 'batchtask',  # Optional: Defines MongoDB collection name
        'ordering': ['-created_at']
    }

class EmailData(Document):
    email = EmailField(unique=True, required=True)
    status = StringField(max_length=50, required=True)
    checked_at = DateTimeField(default=timezone.now)

    def __str__(self):
        return f"{self.email} - {self.status}"


class CatchAllDomains(Document):
    domain = StringField(unique=True, max_length=50, required=True)
    checked_at = DateTimeField(default=timezone.now)

    def __str__(self):
        return f"CatchAll: {self.domain}"


class NoMXDomains(Document):
    domain = StringField(unique=True, max_length=50, required=True)
    checked_at = DateTimeField(default=timezone.now)

    def __str__(self):
        return f"NoMX: {self.domain}"
