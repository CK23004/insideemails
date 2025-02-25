from django.shortcuts import render
import requests, chardet
import redis
import csv, os 
from django.conf import settings
import hmac
import hashlib
import base64
import io
from django.http import HttpResponse, JsonResponse, StreamingHttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
import json, re
from .tasks import verify_emails_in_parallel, simple_task
from celery.signals import task_success
import pandas as pd
import string, random, time, datetime
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import AllowAny
from rest_framework.authentication import BaseAuthentication
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.exceptions import AuthenticationFailed
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync

from .models import BatchTask, UserProfile
from .tasks import db
def index(request):
    return HttpResponse("Hello, world! This is ev_backend.")


# Constants
CONSUMER_KEY = "1a446c8347d426a691caa39aa1c98d41"
CONSUMER_SECRET = "250cc64cd5ba487dce9ba53406ea6d9f"

sender_email = "kamlesh@sphurti.net"
proxy_host = "gw-open.netnut.net"
proxy_port = 9595
proxy_user = "kamlesh007-evsh-any"
proxy_password = "Kamleshsurana@007"
channel_layer = get_channel_layer()

# Hardcoded API Token
API_SECRET_KEY = "F3A6B9C8D4E2F170A5B3C7D8E9F62140"
WEBHOOK_SECRET = "IvdzDYeID(;Yv !t~~F3nzB=-$I,*k5qr[WTsG1dd2I%:097w,"


class TokenAuthentication(BaseAuthentication):
    def authenticate(self, request):
        api_key = request.headers.get("Authorization")
        if not api_key or api_key != f"Bearer {API_SECRET_KEY}":
            raise AuthenticationFailed("Invalid API Key")

        return (None, None)  # No user object needed

import psutil

def system_monitor(request):
    cpu_usage = psutil.cpu_percent(interval=1)  # CPU usage in percentage
    processes = []

    for proc in psutil.process_iter(attrs=['pid', 'name', 'cpu_percent']):
        try:
            processes.append(proc.info)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    return JsonResponse({
        "cpu_usage": cpu_usage,
        "running_processes": processes
    })


def is_email(val):
    return re.match(r"[^@]+@[^@]+\.[^@]+", val)

def detect_encoding(content):
    """Detects encoding of given file content."""
    detected = chardet.detect(content)
    encoding = detected.get("encoding", "utf-8")  # Default to UTF-8 if detection fails
    return encoding


# @csrf_exempt
# @require_POST
# def bulk_email_verify(request):
#     try:
#         data = json.loads(request.body)

#         wp_user_id = data.get("wp_userid", 1)
#         email_count = 0
#         file_name = ''
#         if not wp_user_id:
#             return JsonResponse({"error": "wpuserid not found"}, status=400)
        
        
#         # Get file URL
#         try : 
#             file_data = data.get("form_data", {})
#             print(1)
#         except:
#             pass

 
#         def is_email(val): return re.match(r"[^@]+@[^@]+\.[^@]+", val)
#         if file_data and "15" in file_data:
#             file_info = file_data.get("15", {})  # Ensure it doesn't break if key is missing
#             file_url = file_info.get("value")
#             file_name = file_info.get("file_original")
#             file_ext = file_info.get("ext")

#             print(1)
#             if file_ext not in ["csv", "txt"]:
#                 return JsonResponse({"error": "Uploaded file must be a CSV or TXT"}, status=400)

#             # Fetch file content
#             file_response = requests.get(file_url)
#             if file_response.status_code != 200:
#                 return JsonResponse({"error": "Failed to download file"}, status=500)

#             # Try detecting encoding
#             file_content_bytes = file_response.content
#             detected_encoding = detect_encoding(file_content_bytes)
#             file_content = file_content_bytes.decode(detected_encoding, errors="ignore")                

#             if file_content is None:
#                 return JsonResponse({"error": "Failed to decode file with supported encodings"}, status=500)

#             print(f"Using Encoding: {detected_encoding}")
#             file_stream = io.StringIO(file_content)

#             # Process file based on type
#             email_list = []
#             if file_ext == "csv":
#                 reader = csv.reader(file_stream)
#                 for row in reader:
#                     for col in row:  # Check all columns for emails
#                         email = col.strip()
#                         if is_email(email):
#                             email_list.append(email)
#             elif file_ext == "txt":
#                 for line in file_stream:
#                     email = line.strip()  # Remove any leading/trailing whitespace
#                     if is_email(email):
#                         email_list.append(email)

#             email_count = len(email_list)
#             # email_count = sum(1 for row in csv_reader if row) if is_email(first_row[0]) else sum(1 for row in csv_reader if row)
#         else:
#             file_name = "Untitled.csv"
#             email_string = data.get("form_data", {}).get("8", {}).get("value", "")
#             common_email_list = [email.strip() for email in email_string.split('\r\n')]
#             email_list = [email for email in common_email_list if is_email(email)]
#             email_count = len(email_list)

        
#         if email_count > 50000:
#             return JsonResponse({"error": "Maximum email list size is 1,00,000 per request"}, status=400)

#         wallet_action(wp_user_id, action="debit", credit_count=email_count, file_name=file_name)

        
#         # Fetch post_id from webhook data
#         post_id = data.get("post_id", {})
        
#         if email_list:
#             if file_name:
#                 base_file_name = os.path.splitext(file_name)[0]
#                 output_file_name = f"{base_file_name}_{wp_user_id}{post_id}.csv"
#             else:
#                 output_file_name = f"Untitled_{wp_user_id}{post_id}.csv"
                
#             # Trigger the Celery task
            
           
#             verify_emails_in_parallel.delay(sender_email, proxy_host, proxy_port, proxy_user, proxy_password, email_list, service_type = 'bulk_verify', wpuser_id = wp_user_id, post_id=post_id, output_file_name=output_file_name)

#             # Output to confirm the task is triggered
#             print("Celery task triggered successfully!")
#         else:
#             return JsonResponse({"error": f"Failed to Process Emails. No valid Emails Found."}, status=500)
#         if not post_id:
#             return JsonResponse({"error": "Post ID not found"}, status=400)
        
#         return JsonResponse({"success": True, "credits_used": email_count})
    
#     except Exception as e:
#         return JsonResponse({"error": str(e)}, status=500)

class BulkEmailVerifyAPIView(APIView):
    authentication_classes = [TokenAuthentication]

    def post(self, request):
        try:
            data = request.data
            wp_user_id = data.get("wp_userid", 1)
            post_id = data.get("post_id")

            if not wp_user_id:
                return Response({"error": "wp_userid not found"}, status=400)

            file_data = data.get("form_data", {})
            email_list = []
            file_name = None

            # Process uploaded file
            if "15" in file_data:
                file_info = file_data["15"]
                file_url = file_info.get("value")
                file_name = file_info.get("file_original", "Untitled.csv")
                file_ext = file_info.get("ext")

                if file_ext not in ["csv", "txt"]:
                    return Response({"error": "Uploaded file must be a CSV or TXT"}, status=400)

                file_response = requests.get(file_url)
                if file_response.status_code != 200:
                    return Response({"error": "Failed to download file"}, status=500)

                detected_encoding = detect_encoding(file_response.content)
                file_content = file_response.content.decode(detected_encoding, errors="ignore")
                file_stream = io.StringIO(file_content)

                # Read emails from file
                if file_ext == "csv":
                    reader = csv.reader(file_stream)
                    for row in reader:
                        for col in row:
                            email = col.strip()
                            if is_email(email):
                                email_list.append(email)
                elif file_ext == "txt":
                    for line in file_stream:
                        email = line.strip()
                        if is_email(email):
                            email_list.append(email)

            # Process manual email input
            else:
                email_string = file_data.get("8", {}).get("value", "")
                email_list = [email.strip() for email in email_string.split("\r\n") if is_email(email)]
                file_name = "Untitled.csv"

            email_count = len(email_list)
            post_update_payload = {
                "post_id": post_id,
                "post_content": f'<div id="status" class="d-none">Pending</div>'
                                f'<div id="email_count" class="d-none">{email_count}</div>'
                                f'<div id="progress" class="d-none">0</div>'
                                f'<br> <a href="#" class="d-none"></a>',
            }
            async_to_sync(channel_layer.group_send)(
                "global_progress",
                {
                    "type": "progress_update",
                    "post_id": post_id,
                    "post_content": post_update_payload["post_content"]
                }
            )

            if email_count > 50000:
                async_to_sync(channel_layer.group_send)(
                        f"user_{session_id}",
                        {"type": "alert.message", "message": "Maximum email list size is 50,000 Per Request"}
                    )
                
                post_update_payload = {
                "post_id": post_id,
                "post_content": f'<div id="status" class="d-none">Failed</div>'
                                f'<div id="email_count" class="d-none">{email_count}</div>'
                                f'<div id="progress" class="d-none">0</div>'
                                f'<br> <a href="#" class="d-none"></a>',
                }
                async_to_sync(channel_layer.group_send)(
                    "global_progress",
                    {
                        "type": "progress_update",
                        "post_id": post_id,
                        "post_content": post_update_payload["post_content"]
                    }
                )
                    
                return Response({"error": "Maximum email list size is 50,000 per request"}, status=200)
            
            #INSUFFICIENT CREDITS  MESSAGE
            session_id = data.get("session_id", None)
            if session_id:
                wallet_url = f"https://insideemails.com/wp-json/wsfw-route/v1/wallet/{wp_user_id}?consumer_key={CONSUMER_KEY}&consumer_secret={CONSUMER_SECRET}"
                wallet_response = requests.get(wallet_url)
                wallet_credits = float(wallet_response.json())
                if wallet_response.status_code != 200:
                    return Response({"error": "Failed to fetch wallet balance"}, status=500)
                # Send to a specific user session
                if wallet_credits < email_count:
                    async_to_sync(channel_layer.group_send)(
                        f"user_{session_id}",
                        {"type": "alert.message", "message": "Insufficient Credits"}
                    )
                    post_update_payload = {
                        "post_id": post_id,
                        "post_content":  f'<div id="status" class="d-none">Failed</div>'
                                        f'<div id="email_count" class="d-none">{email_count}</div>'
                                        f'<div id="progress" class="d-none">0</div>'
                                        f'<br> <a href="#" class="d-none"></a>',
                    }
                    async_to_sync(channel_layer.group_send)(
                        "global_progress",
                        {
                            "type": "progress_update",
                            "post_id": post_id,
                            "post_content": post_update_payload["post_content"]
                        }
                    )
                    return Response({"message": "Insufficient Credits"}, status=200)

            # Deduct wallet credits
            wallet_action(wp_user_id, action="debit", credit_count=email_count, file_name=file_name)

            # Generate output filename
            output_file_name = f"{os.path.splitext(file_name)[0]}_{wp_user_id}{post_id}.csv" if post_id else f"Untitled_{wp_user_id}.csv"

            # Trigger Celery task
            if email_list:
                verify_emails_in_parallel.delay(
                    sender_email, proxy_host, proxy_port, proxy_user, proxy_password,
                    email_list, service_type="bulk_verify",
                    wpuser_id=wp_user_id, post_id=post_id, output_file_name=output_file_name
                )
            else:
                return Response({"error": "No valid emails found"}, status=400)

            return Response({"success": True, "credits_used": email_count}, status=200)

        except Exception as e:
            return Response({"error": str(e)}, status=500)


# @csrf_exempt
# @require_POST
# def single_email_verify(request):
#         try:
#             data = json.loads(request.body)
#             post_id = data.get("post_id")
#             form_data = data.get("form_data", {})
#             wp_user_id = data.get("wp_user_id", 1)
#             email_id = None
#             for key, field in form_data.items():
#                 if field.get("type") == "email":
#                     email_id = field.get("value")
#                     break
            
#             if not email_id:
#                 return JsonResponse({"error": "No email found in form_data"}, status=400)
            
#             wallet_action(wp_user_id, action="debit", credit_count=1, file_name="Single Email Verify")
#             email_list = []
#             email_list.append(email_id)
#             print(email_list)
#             # Trigger Celery task
#             verify_emails_in_parallel.delay(
#                 sender_email, proxy_host, proxy_port, proxy_user, 
#                 proxy_password, email_list, service_type='single_verify', 
#                 wpuser_id=wp_user_id, post_id=post_id, 
#                 output_file_name=None
#             )
            
#             return JsonResponse({"message": "Verification task triggered successfully"})
        
#         except json.JSONDecodeError:
#             return JsonResponse({"error": "Invalid JSON data"}, status=400)
#         except Exception as e:
#             return JsonResponse({"error": str(e)}, status=500)
    
        
class SingleEmailVerifyAPIView(APIView):
    authentication_classes = [TokenAuthentication]  # Only use custom authentication

    def post(self, request):
        try:
            data = request.data  # DRF automatically parses JSON body
            post_id = data.get("post_id")
            form_data = data.get("form_data", {})
            wp_user_id = data.get("wp_user_id", 1)
            email_id = None

            # Extract email from form_data
            for key, field in form_data.items():
                if field.get("type") == "email":
                    email_id = field.get("value")
                    break
            
            if not email_id:
                return Response({"error": "No email found in form_data"}, status=400)

            #INSUFFICIENT CREDITS  MESSAGE
            session_id = data.get("session_id", None)
            if session_id:
                wallet_url = f"https://insideemails.com/wp-json/wsfw-route/v1/wallet/{wp_user_id}?consumer_key={CONSUMER_KEY}&consumer_secret={CONSUMER_SECRET}"
                wallet_response = requests.get(wallet_url)
                wallet_credits = float(wallet_response.json())
                if wallet_response.status_code != 200:
                    return Response({"error": "Failed to fetch wallet balance"}, status=500)
                # Send to a specific user session
                if wallet_credits < 1:
                    async_to_sync(channel_layer.group_send)(
                        f"user_{session_id}",
                        {"type": "alert.message", "message": "Insufficient Credits"}
                    )
                    post_update_payload = {
                        "post_id": post_id,
                        "post_content":  f'<div id="status" class="d-none">Failed</div>'
                                        f'<div id="progress" class="d-none">0</div>'
                    }
                    async_to_sync(channel_layer.group_send)(
                        "global_progress",
                        {
                            "type": "progress_update",
                            "post_id": post_id,
                            "post_content": post_update_payload["post_content"]
                        }
                    )
                    return Response({"message": "Insufficient Credits"}, status=200)
            
            # Debit user's wallet for email verification
            wallet_action(wp_user_id, action="debit", credit_count=1, file_name="Single Email Verify")
            # Trigger Celery task
            verify_emails_in_parallel.delay(
                sender_email, proxy_host, proxy_port, proxy_user, 
                proxy_password, [email_id], service_type='single_verify', 
                wpuser_id=wp_user_id, post_id=post_id, 
                output_file_name=''
            )
            print('triggered')
            return Response({"message": "Verification task triggered successfully"}, status=200)

        except json.JSONDecodeError:
            return Response({"error": "Invalid JSON data"}, status=400)
        except Exception as e:
            return Response({"error": str(e)}, status=500)


# @csrf_exempt
# @require_POST
# def email_finder_view(request):
#         try:
#             data = json.loads(request.body)
#             post_id = data.get("post_id")
#             wp_user_id = data.get("wp_user_id")
#             form_data = data.get("form_data", {})
            
#             person_name = None
#             company_name = None
            
#             for key, field in form_data.items():
#                 if field.get("type") == "name":
#                     person_name = field.get("value")
#                 elif field.get("type") == "text":
#                     company_name = field.get("value")
            
#             if not person_name or not company_name:
#                 return JsonResponse({"error": "Missing required fields"}, status=400)
            
#             first_name, *last_name_parts = person_name.split()
#             last_name = last_name_parts[-1] if last_name_parts else ""
            
#             company_domain = company_name

#             # Generate email patterns
#             email_list = [
#                 f"{first_name}@{company_domain}",
#                 f"{last_name}@{company_domain}",
#                 f"{first_name}{last_name}@{company_domain}",
#                 f"{first_name}.{last_name}@{company_domain}",
#                 f"{first_name[0]}{last_name}@{company_domain}",
#                 f"{first_name[0]}.{last_name}@{company_domain}",
#                 f"{first_name}{last_name[0]}@{company_domain}",
#                 f"{first_name}.{last_name[0]}@{company_domain}",
#                 f"{first_name[0]}{last_name[0]}@{company_domain}",

#             ]

#             # Remove duplicates and ensure validity
#             email_list = list(set(email_list))

#             # Call Celery task with the properly formatted email list
#             verify_emails_in_parallel.delay(
#                 sender_email, proxy_host, proxy_port, proxy_user, 
#                 proxy_password, email_list=email_list, service_type='email_finder', 
#                 wpuser_id=wp_user_id, post_id=post_id, output_file_name=None
#             )

            
#             return JsonResponse({"message": "Verification task triggered successfully"})
        
#         except Exception as e:
#             return JsonResponse({"error": str(e)}, status=500)

class EmailFinderAPIView(APIView):
    authentication_classes = [TokenAuthentication]  # Only one authentication class

    def post(self, request):
        try:
            data = request.data  # DRF automatically parses JSON body
            post_id = data.get("post_id")
            wp_user_id = data.get("wp_user_id")
            form_data = data.get("form_data", {})

            person_name = None
            company_name = None

            for key, field in form_data.items():
                if field.get("type") == "name":
                    person_name = field.get("value")
                elif field.get("type") == "text":
                    company_name = field.get("value")

            if not person_name or not company_name:
                return Response({"error": "Missing required fields"}, status=400)

            first_name, *last_name_parts = person_name.split()
            last_name = last_name_parts[-1] if last_name_parts else ""

            company_domain = company_name

            # Generate email patterns
            email_list = list(set([
                f"{first_name}@{company_domain}",
                f"{last_name}@{company_domain}",
                f"{first_name}{last_name}@{company_domain}",
                f"{first_name}.{last_name}@{company_domain}",
                f"{first_name[0]}{last_name}@{company_domain}",
                f"{first_name[0]}.{last_name}@{company_domain}",
                f"{first_name}{last_name[0]}@{company_domain}",
                f"{first_name}.{last_name[0]}@{company_domain}",
                f"{first_name[0]}{last_name[0]}@{company_domain}",
            ]))

            #INSUFFICIENT CREDITS  MESSAGE
            session_id = data.get("session_id", None)
            if session_id:
                wallet_url = f"https://insideemails.com/wp-json/wsfw-route/v1/wallet/{wp_user_id}?consumer_key={CONSUMER_KEY}&consumer_secret={CONSUMER_SECRET}"
                wallet_response = requests.get(wallet_url)
                wallet_credits = float(wallet_response.json())
                if wallet_response.status_code != 200:
                    return Response({"error": "Failed to fetch wallet balance"}, status=500)
                # Send to a specific user session
                if wallet_credits < 10:
                    async_to_sync(channel_layer.group_send)(
                        f"user_{session_id}",
                        {"type": "alert.message", "message": "Insufficient Credits (You Need Minimum 10 credits to Find Emails)"}
                    )
                    post_update_payload = {
                        "post_id": post_id,
                        "post_content":  f'<div id="status" class="d-none">Failed</div>'
                                        f'<div id="progress" class="d-none">0</div>'
                                        f'<div id="emails"></div>',
                    }
                    async_to_sync(channel_layer.group_send)(
                        "global_progress",
                        {
                            "type": "progress_update",
                            "post_id": post_id,
                            "post_content": post_update_payload["post_content"]
                        }
                    )
                return Response({"message": "Insufficient Credits (You Need Minimum 10 credits to Find Emails)"}, status=200)
            # Call Celery task
            verify_emails_in_parallel.delay(
                sender_email, proxy_host, proxy_port, proxy_user, 
                proxy_password, email_list=email_list, service_type='email_finder', 
                wpuser_id=wp_user_id, post_id=post_id, output_file_name=''
            )
            
            return Response({"message": "Verification task triggered successfully"}, status=200)

        except Exception as e:
            return Response({"error": str(e)}, status=500)

def generate_token():
    while True:
        token = ''.join(random.choices(string.ascii_letters + string.digits, k=32))
        if not db.userprofile.find_one({"api_key": token}):
            return token

# @csrf_exempt
# def create_api_key(request):
#     if request.method == "POST":
#         try:
#             data = json.loads(request.body)
#             user_id = data.get("user_id")
            
#             if not user_id:
#                 return JsonResponse({"error": "User ID is required"}, status=400)
            
#             token = generate_token()
            
#             # Store in user profile
#             user_profile, created = UserProfile.objects.get_or_create(wpuser_id=user_id)
#             user_profile.api_key = token
#             user_profile.save()
            
#             # Update WordPress usermeta
#             url = "https://insideemails.com/?wpwhpro_action=wp-modify-usermeta&wpwhpro_api_key=5zvkfisvqtt2y6fxwcnfrvgczcyt9jeatvyo5e2l7zludi2ps7z6crtkd0vr6lyk&action=wp_manage_user_meta_data"
#             payload = {
#                 "user_id": user_id,
#                 "meta_update": {"api_key": token}
#             }
#             response = requests.post(url, json=payload)
            
#             return JsonResponse({"message": "API key created successfully", "api_key": token})
        
#         except json.JSONDecodeError:
#             return JsonResponse({"error": "Invalid JSON data"}, status=400)
#         except Exception as e:
#             return JsonResponse({"error": str(e)}, status=500)
    
#     return JsonResponse({"error": "Invalid request method"}, status=405)




class APIKeyAuthentication(BaseAuthentication):
    def authenticate(self, request):
        api_key = request.headers.get("Authorization")
        if not api_key:
            return None

        api_key = api_key.replace("Bearer ", "")  # Extract token if prefixed with "Bearer "

        user_profile = db.userprofile.find_one({"api_key": api_key})
        if not user_profile:
            return Response({"error": "Invalid API Key"}, status=401)

        return (user_profile, None)

# View to create API Key
class CreateAPIKeyView(APIView):
    authentication_classes = [TokenAuthentication]

    def post(self, request):
        try:
            data = request.data
            print(data)
            user_id = data.get("user_id")
            if not user_id:
                return Response({"error": "User ID is required"}, status=400)
            token = generate_token()
            
            user_profile = {
            "wpuser_id": user_id,
            "api_key": token
            }
            db.userprofile.insert_one(user_profile)
            # Update WordPress usermeta
            url = "https://insideemails.com/?wpwhpro_action=wp-modify-usermeta&wpwhpro_api_key=5zvkfisvqtt2y6fxwcnfrvgczcyt9jeatvyo5e2l7zludi2ps7z6crtkd0vr6lyk&action=wp_manage_user_meta_data"
            payload = {
                "user_id": user_id,
                "meta_update": {"api_key": token}
            }
            response = requests.post(url, json=payload)
            
            return Response({"message": "API key created successfully", "api_key": token})
        except Exception as e:
            return Response({"error": str(e)}, status=500)

redis_client = redis.Redis(host="localhost", port=6379, db=1, decode_responses=True)

# View to verify emails using API Key Authentication
class VerifyEmailsAPIView(APIView):
    authentication_classes = [APIKeyAuthentication]
    
    def post(self, request):
        try:
            data = request.data
            email_list = data.get("email_list", [])
            
            if not email_list or not isinstance(email_list, list):
                return Response({"error": "Invalid email list format"}, status=400)
            
            if len(email_list) > 50000:
                return Response({"error": "Maximum email list size is 50,000 per request"}, status=400)
            
            customer_id = request.user.get("wpuser_id")  

            wallet_url = f"https://insideemails.com/wp-json/wsfw-route/v1/wallet/{customer_id}?consumer_key={CONSUMER_KEY}&consumer_secret={CONSUMER_SECRET}"
            wallet_response = requests.get(wallet_url)
            wallet_credits = float(wallet_response.json())

            if wallet_credits < len(email_list):
                return Response({"error": "Insufficient Credits"}, status=400)
            
            batch_id = f"batch_{int(time.time())}"

            batch_task_data = {
                "batch_id": batch_id,
                "total": len(email_list),
                "output_file_name": f'{customer_id}__{batch_id}.csv',
                "initial_count": len(email_list),
                "completed": 0,
                "status": "pending",
                "wpuser_id": customer_id,
                "service_type": "email_verification_through_api",
                "results": json.dumps({})
            }
            redis_client.hset(f"batch:{batch_id}", mapping=batch_task_data)
            redis_client.expire(f"batch:{batch_id}", 86400)
            # Call Celery task
            verify_emails_in_parallel.delay(
                sender_email, proxy_host, proxy_port, proxy_user, 
                proxy_password, email_list=email_list, service_type='email_verification_through_api',  
                output_file_name=batch_task_data['output_file_name'], wpuser_id=customer_id, batch_id=batch_id
            )
            
            return Response({
                            "message": "Email verification started.",
                            "batch_id": batch_id
                        })        
        except Exception as e:
            return Response({"error": str(e)}, status=500)

class BatchResultsStreamView(APIView):
    authentication_classes = [APIKeyAuthentication]

    def get(self, request, *args, **kwargs):
        batch_id = request.GET.get('batch_id','')
        if not batch_id:
            return JsonResponse({'error': 'batch_id is required'}, status=400)
        batch_data = redis_client.hgetall(f"batch:{batch_id}")
        if not batch_data:
            return Response({"error": "Batch not found"}, status=404)

        if batch_data["status"] != "completed":
            progress = batch_data['progress'] 
            data = {
                "batch_id": batch_id,
                "status": batch_data["status"],
                "progress" : f"{progress}%"
                
            }
            return Response(data, status=202)

        batch_results = json.loads(batch_data.get("results", "{}")) # Ensure batch_results is a dictionary

        def stream_results():
            yield json.dumps({
                "batch_id": batch_id,
                "status": "completed",
                "total": batch_data["initial_count"]
            }) + "\n"

            chunk_size = 500
            email_results = list(batch_results.items())

            for i in range(0, len(email_results), chunk_size):
                chunk = email_results[i:i + chunk_size]
                yield json.dumps(dict(chunk)) + "\n"

        return StreamingHttpResponse(stream_results(), content_type="application/json")

def wallet_action(wp_user_id, action, credit_count, file_name=''):
    wallet_url = f"https://insideemails.com/wp-json/wsfw-route/v1/wallet/{wp_user_id}?consumer_key={CONSUMER_KEY}&consumer_secret={CONSUMER_SECRET}"
    wallet_response = requests.get(wallet_url)
    wallet_credits = float(wallet_response.json())
    if wallet_response.status_code != 200:
        return JsonResponse({"error": "Failed to fetch wallet balance"}, status=500)
    if action == 'debit':
        if credit_count <= wallet_credits and credit_count > 0:
                # Debit wallet
                debit_url = f"https://insideemails.com/wp-json/wsfw-route/v1/wallet/{wp_user_id}"
                debit_payload = {
                    "amount": credit_count,
                    "action": f"{action}",
                    "consumer_key": CONSUMER_KEY,
                    "consumer_secret": CONSUMER_SECRET,
                    "transaction_detail": f"debit through {file_name}",
                    "payment_method" : "Auto Deduct through Utilisation"
                }
                
                headers = {"Content-Type": "application/json"}
                debit_response = requests.put(debit_url, headers=headers, json=debit_payload)
                
                if debit_response.status_code == 200:
                    return True
                else:
                    return False
    else:
        credit_url = f"https://insideemails.com/wp-json/wsfw-route/v1/wallet/{wp_user_id}"
        credit_payload = {
            "amount": credit_count,
            "action": f"{action}",
            "consumer_key": CONSUMER_KEY,
            "consumer_secret": CONSUMER_SECRET,
            "transaction_detail": f"Credit by Subscription",
            "payment_method" : "Credit Card"
        }
        
        headers = {"Content-Type": "application/json"}
        debit_response = requests.put(credit_url, headers=headers, json=credit_payload)
        
        if debit_response.status_code == 200:
            return True
        else:
            # Log or return specific error details from the response
            return False


@csrf_exempt
def subscription_credits_update(request):
    if request.method == "POST":
        try:

            raw_body = request.body

            # Step 2: Get the WooCommerce signature from headers
            received_signature = request.headers.get("X-WC-Webhook-Signature")
            if not received_signature:
                return JsonResponse({"error": "Missing signature"}, status=403)

            # Step 3: Generate the expected HMAC signature
            expected_signature = base64.b64encode(
                hmac.new(WEBHOOK_SECRET.encode(), raw_body, hashlib.sha256).digest()
            ).decode()

            # Step 4: Compare received vs expected signature
            if not hmac.compare_digest(received_signature, expected_signature):
                return JsonResponse({"error": "Invalid signature"}, status=403)
            
            data = json.loads(raw_body)

            # Check if order status is "completed"
            order_status = data.get("status")
            if order_status != "completed":
                return JsonResponse({"message": "Order is not completed"}, status=400)

            # Get customer ID
            customer_id = data.get("customer_id")
            if not customer_id:
                return JsonResponse({"message": "Customer ID not found"}, status=400)

            # Extract email credits from product name
            line_items = data.get("line_items", [])
            total_credits = 0

            for item in line_items:
                product_name = item.get("name", "")
                match = re.search(r'(\d+)\s*/\s*month', product_name)  # Extract number before "/month"
                if match:
                    total_credits += int(match.group(1))  # Add extracted credits

            if total_credits > 0:
                # Update user wallet
                wallet_action(customer_id, action="credit", credit_count=total_credits)
                return JsonResponse({"message": f"Wallet credited with {total_credits} credits"}, status=200)

            return JsonResponse({"message": "No valid credits found in order"}, status=400)

        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON data"}, status=400)

    return JsonResponse({"error": "Invalid request method"}, status=405)



# def test(request):
#     verify_emails_in_parallel.delay(
#                 sender_email, proxy_host, proxy_port, proxy_user, 
#                 proxy_password, 

#                 email_list = [] 

#                 , service_type='bulk_verify', 
#                 wpuser_id='1', post_id='1', 
#                 output_file_name=None
#             )
    
#     return JsonResponse( {'test': '0'}, status=200)

# @csrf_exempt
# def test(request):
#     csv_file_path = r"E:\insideemails\home\insideemails\ev_backend\k_emails.csv"  # Change filename if needed
#     email_list = []
    
#     try:
#         with open(csv_file_path, mode='r', encoding='utf-8') as file:
#             reader = csv.reader(file)
#             for row in reader:
#                 for col in row:
#                     col = col.strip()
#                     if '@' in col:  # Basic email validation
#                         email_list.append(col)
#     except FileNotFoundError:
#         print('file not found')
#         return
#     except Exception as e:
#         print('error')
#         return 
    
    
#     verify_emails_in_parallel.delay(
#         sender_email, proxy_host, proxy_port, proxy_user, 
#         proxy_password, email_list=['zindex@zindex.co.in',
#                                     'smurugadhas@zippelbay.com',
#                                     'vlakshmi@zithara.ai',
#                                     'kbhatt@zithas.com',
#                                     'vhansraj@zobele.com',
#                                     'ztechnologies@zognu.com',
#                                     'gs@zontecsolutions.com',
#                                     'mlakshmi@zontecsolutions.com',
#                                     'athakur@zoom.com',
#                                     'achakrabarti@zoom.com',
#                                     'mgeorge@zoom.com',
#                                     'rvemula@zoom.com',
#                                     'skumar@zoom.com',
#                                     'ykapde@zoom.com',
#                                     'kk@zvky.com'],
#         service_type='email_verification_through_api', 
#         output_file_name='k_test_done.csv',
#         post_id =1
#     )
    
#     print('EV Started Successfully')


# def test():
#     simple_task.delay()
#     print("triggered")
#     return JsonResponse({'status': 'Task triggered'})
@csrf_exempt
def test(request):
    simple_task.delay()
    print("triggered")
    return JsonResponse({'status': 'Task triggered'})