from django.db import models
import uuid
# Create your models here.
class UserProfile(models.Model):
    wpuser_id = models.IntegerField(unique=True)
    api_key = models.CharField(max_length=16, unique=True, null=True, blank=True)
    
    def __str__(self):
        return f"Profile of {self.user_id}"
    

class BatchTask(models.Model):
    id = models.UUIDField(default=uuid.uuid4, editable=False, unique=True)
    batch_id = models.CharField(max_length=255, primary_key=True)  # Unique identifier
    total = models.IntegerField()  # Total emails
    completed = models.IntegerField(default=0)  # Completed emails
    output_file_name = models.CharField(max_length=500)  # Output file path
    initial_count = models.IntegerField()  # Initial email count
    status = models.CharField(max_length=50, default="pending")  # Task status
    spam_block_retries = models.IntegerField(null=True, blank=True)
    valid_email_retries = models.IntegerField(null=True, blank=True)
    part1 = models.TextField(null=True, blank=True)  # Store serialized parts
    part2 = models.TextField(null=True, blank=True)
    part3 = models.TextField(null=True, blank=True)
    part4 = models.TextField(null=True, blank=True)
    post_id = models.IntegerField(null=True, blank=True)  # WooCommerce Post ID
    wpuser_id = models.IntegerField(null=True, blank=True)  # WordPress User ID
    service_type = models.CharField(max_length=100, null=True, blank=True)  # Service type
    results = models.JSONField(default=dict)  # Store email results

    def __str__(self):
        return f"BatchTask {self.batch_id} - {self.status}"
    
class EmailData(models.Model):
    email = models.EmailField(unique=True, db_index=True)
    status = models.CharField(max_length=50)
    checked_at = models.DateTimeField(auto_now=True)

class CatchAllDomains(models.Model):
    domain = models.CharField(unique=True, max_length=50, db_index=True)
    checked_at = models.DateTimeField(auto_now=True)

class NoMXDomains(models.Model):
    domain = models.CharField(unique=True, max_length=50, db_index=True)
    checked_at = models.DateTimeField(auto_now=True)