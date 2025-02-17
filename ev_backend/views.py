from django.shortcuts import render
import requests, chardet
import csv, os 
from django.conf import settings
import io
from django.http import HttpResponse, JsonResponse, StreamingHttpResponse
from django.views.decorators.csrf import csrf_exempt
import json, re
from .tasks import verify_emails_in_parallel
from celery.signals import task_success
import pandas as pd
import string, random, time
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import AllowAny
from rest_framework.authentication import BaseAuthentication, TokenAuthentication
from rest_framework.decorators import api_view, authentication_classes, permission_classes
from rest_framework.permissions import IsAuthenticated
from .models import UserProfile, BatchTask
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



def detect_encoding(content):
    """Detects encoding of given file content."""
    detected = chardet.detect(content)
    encoding = detected.get("encoding", "utf-8")  # Default to UTF-8 if detection fails
    return encoding




@api_view(["POST"])
@authentication_classes([TokenAuthentication])
@permission_classes([IsAuthenticated])
def bulk_email_verify(request):
    try:
        data = request.data

        wp_user_id = data.get("wp_userid", 1)
        email_count = 0
        file_name = ''
        if not wp_user_id:
            return JsonResponse({"error": "wpuserid not found"}, status=400)
        
        
        # Get file URL
        try : 
            file_data = data.get("form_data", {})
            print(1)
        except:
            pass

 
        def is_email(val): return re.match(r"[^@]+@[^@]+\.[^@]+", val)
        if file_data and "15" in file_data:
            file_info = file_data.get("15", {})  # Ensure it doesn't break if key is missing
            file_url = file_info.get("value")
            file_name = file_info.get("file_original")
            file_ext = file_info.get("ext")

            print(1)
            if file_ext not in ["csv", "txt"]:
                return JsonResponse({"error": "Uploaded file must be a CSV or TXT"}, status=400)

            # Fetch file content
            file_response = requests.get(file_url)
            if file_response.status_code != 200:
                return JsonResponse({"error": "Failed to download file"}, status=500)

            # Try detecting encoding
            file_content_bytes = file_response.content
            detected_encoding = detect_encoding(file_content_bytes)
            file_content = file_content_bytes.decode(detected_encoding, errors="ignore")                

            if file_content is None:
                return JsonResponse({"error": "Failed to decode file with supported encodings"}, status=500)

            print(f"Using Encoding: {detected_encoding}")
            file_stream = io.StringIO(file_content)

            # Process file based on type
            email_list = []
            if file_ext == "csv":
                reader = csv.reader(file_stream)
                for row in reader:
                    for col in row:  # Check all columns for emails
                        email = col.strip()
                        if is_email(email):
                            email_list.append(email)
            elif file_ext == "txt":
                for line in file_stream:
                    email = line.strip()  # Remove any leading/trailing whitespace
                    if is_email(email):
                        email_list.append(email)

            email_count = len(email_list)
            # email_count = sum(1 for row in csv_reader if row) if is_email(first_row[0]) else sum(1 for row in csv_reader if row)
        else:
            file_name = "Untitled.csv"
            email_string = data.get("form_data", {}).get("8", {}).get("value", "")
            common_email_list = [email.strip() for email in email_string.split('\r\n')]
            email_list = [email for email in common_email_list if is_email(email)]
            email_count = len(email_list)

        
        if email_count > 50000:
            return JsonResponse({"error": "Maximum email list size is 50,000 per request"}, status=400)

        wallet_action(wp_user_id, action="debit", credit_count=email_count, file_name=file_name)

        
        # Fetch post_id from webhook data
        post_id = data.get("post_id", {})
        if email_list:
            if file_name:
                base_file_name = os.path.splitext(file_name)[0]
                output_file_name = os.path.join(settings.MEDIA_ROOT, f"{base_file_name}_{wp_user_id}{post_id}.csv") 
            else:
                output_file_name = os.path.join(settings.MEDIA_ROOT, f"Untitled_{wp_user_id}{post_id}.csv") 
                
            # Trigger the Celery task
           
            verify_emails_in_parallel.delay(sender_email, proxy_host, proxy_port, proxy_user, proxy_password, email_list, service_type = 'bulk_verify', wpuser_id = wp_user_id, post_id=post_id, output_file_name=output_file_name)

            # Output to confirm the task is triggered
            print("Celery task triggered successfully!")
        else:
            return JsonResponse({"error": f"Failed to Process Emails. No valid Emails Found."}, status=500)
        if not post_id:
            return JsonResponse({"error": "Post ID not found"}, status=400)
        
        return JsonResponse({"success": True, "credits_used": email_count})
    
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)



@api_view(["POST"])
@authentication_classes([TokenAuthentication])
@permission_classes([IsAuthenticated])
def single_email_verify(request):
        try:
            data = request.data
            post_id = data.get("post_id")
            form_data = data.get("form_data", {})
            wp_user_id = data.get("wp_user_id", 1)
            email_id = None
            for key, field in form_data.items():
                if field.get("type") == "email":
                    email_id = field.get("value")
                    break
            
            if not email_id:
                return JsonResponse({"error": "No email found in form_data"}, status=400)
            
            wallet_action(wp_user_id, action="debit", credit_count=1, file_name="Single Email Verify")
            email_list = []
            email_list.append(email_id)
            print(email_list)
            # Trigger Celery task
            verify_emails_in_parallel.delay(
                sender_email, proxy_host, proxy_port, proxy_user, 
                proxy_password, email_list, service_type='single_verify', 
                wpuser_id=wp_user_id, post_id=post_id, 
                output_file_name=None
            )
            
            return JsonResponse({"message": "Verification task triggered successfully"})
        
        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON data"}, status=400)
        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)
    
        


@api_view(["POST"])
@authentication_classes([TokenAuthentication])
@permission_classes([IsAuthenticated])  # Use this if you are testing without CSRF tokens
def email_finder_view(request):
        try:
            data = request.data
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
                return JsonResponse({"error": "Missing required fields"}, status=400)
            
            first_name, *last_name_parts = person_name.split()
            last_name = last_name_parts[-1] if last_name_parts else ""
            
            company_domain = company_name

            # Generate email patterns
            email_list = [
                f"{first_name}@{company_domain}",
                f"{last_name}@{company_domain}",
                f"{first_name}{last_name}@{company_domain}",
                f"{first_name}.{last_name}@{company_domain}",
                f"{first_name[0]}{last_name}@{company_domain}",
                f"{first_name[0]}.{last_name}@{company_domain}",
                f"{first_name}{last_name[0]}@{company_domain}",
                f"{first_name}.{last_name[0]}@{company_domain}",
                f"{first_name[0]}{last_name[0]}@{company_domain}",

            ]

            # Remove duplicates and ensure validity
            email_list = list(set(email_list))

            # Call Celery task with the properly formatted email list
            verify_emails_in_parallel.delay(
                sender_email, proxy_host, proxy_port, proxy_user, 
                proxy_password, email_list=email_list, service_type='email_finder', 
                wpuser_id=wp_user_id, post_id=post_id, output_file_name=None
            )

            
            return JsonResponse({"message": "Verification task triggered successfully"})
        
        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON data"}, status=400)
        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)
    


def generate_token(length=32):
    characters = string.ascii_letters + string.digits
    return ''.join(random.choices(characters, k=length))

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




# Custom Authentication Class
class APIKeyAuthentication(BaseAuthentication):
    def authenticate(self, request):
        api_key = request.headers.get("Authorization")
        if not api_key:
            return None
        
        api_key = api_key.replace("Bearer ", "")  # Extract token if prefixed with "Bearer "
        
        try:
            user_profile = UserProfile.objects.get(api_key=api_key)
            return (user_profile, None)
        except UserProfile.DoesNotExist:
            return None

# View to create API Key
class CreateAPIKeyView(APIView):
    permission_classes = [AllowAny]

    def post(self, request):
        try:
            data = request.data
            user_id = data.get("user_id")
            
            if not user_id:
                return Response({"error": "User ID is required"}, status=400)
            
            token = generate_token()
            
            user_profile, created = UserProfile.objects.get_or_create(wpuser_id=user_id)
            user_profile.api_key = token
            user_profile.save()
            
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

# import redis.asyncio as redis
# redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

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
            
            user_profile = request.user
            customer_id = user_profile.wpuser_id

            wallet_url = f"https://insideemails.com/wp-json/wsfw-route/v1/wallet/{customer_id}?consumer_key={CONSUMER_KEY}&consumer_secret={CONSUMER_SECRET}"
            wallet_response = requests.get(wallet_url)
            wallet_credits = float(wallet_response.json())

            if wallet_credits < len(email_list):
                return Response({"error": "Insufficient Credit"}, status=400)
            
            batch_id = f"batch_{int(time.time())}"
                    
            batch_task = BatchTask.objects.create(
            batch_id=batch_id,
            total=len(email_list),
            output_file_name=f'{customer_id}__{batch_id}.csv',
            initial_count=len(email_list),
            completed=0,
            status="pending",
            service_type="email_verification_through_api",
            results={},
            )

            # Call Celery task
            verify_emails_in_parallel.delay(
                sender_email, proxy_host, proxy_port, proxy_user, 
                proxy_password, email_list=email_list, service_type='email_verification_through_api',  
                output_file_name=batch_task.output_file_name, wpuser_id=customer_id, batch_id=batch_id
            )
            
            return Response({
                            "message": "Email verification started.",
                            "batch_id": batch_id
                        })        
        except Exception as e:
            return Response({"error": str(e)}, status=500)

class BatchResultsStreamView(APIView):
    authentication_classes = [APIKeyAuthentication]

    def get(self, request, batch_id):
        try:
            batch_data = BatchTask.objects.get(batch_id=batch_id)
        except BatchTask.DoesNotExist:
            return Response({"error": "Batch not found"}, status=404)

        if batch_data.status != "completed":
            return Response({"message": "Batch is still processing"}, status=202)

        batch_results = batch_data.results or {}  # Ensure batch_results is a dictionary

        def stream_results():
            yield json.dumps({
                "batch_id": batch_id,
                "status": "completed",
                "total": batch_data.initial_count
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
                    # Log or return specific error details from the response
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
            data = json.loads(request.body)

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


def test():
    csv_file_path = r"E:\email verifier dj-wp\emailverifier\ev_backend\k_emails.csv"  # Change filename if needed
    email_list = []
    
    try:
        with open(csv_file_path, mode='r', encoding='utf-8') as file:
            reader = csv.reader(file)
            for row in reader:
                for col in row:
                    col = col.strip()
                    if '@' in col:  # Basic email validation
                        email_list.append(col)
    except FileNotFoundError:
        print('file not found')
        return
    except Exception as e:
        print('error')
        return 
    
    
    verify_emails_in_parallel.delay(
        sender_email, proxy_host, proxy_port, proxy_user, 
        proxy_password, email_list=['zindex@zindex.co.in',
                                    'smurugadhas@zippelbay.com',
                                    'vlakshmi@zithara.ai',
                                    'kbhatt@zithas.com',
                                    'vhansraj@zobele.com',
                                    'ztechnologies@zognu.com',
                                    'gs@zontecsolutions.com',
                                    'mlakshmi@zontecsolutions.com',
                                    'athakur@zoom.com',
                                    'achakrabarti@zoom.com',
                                    'mgeorge@zoom.com',
                                    'rvemula@zoom.com',
                                    'skumar@zoom.com',
                                    'ykapde@zoom.com',
                                    'kk@zvky.com'],
        service_type='email_verification_through_api', 
        output_file_name='k_test_done.csv'
    )
    
    print('EV Started Successfully')