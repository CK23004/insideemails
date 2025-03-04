from celery import Celery, group, chord
from celery.signals import worker_ready
from asgiref.sync import sync_to_async
from concurrent.futures import ThreadPoolExecutor, as_completed
import smtplib, threading
import socks
import aiodns, aiohttp, json, re
import redis
import redis.asyncio as aioredis
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
import asyncio, contextvars
import socket
import logging
import time, sys, os
import pandas as pd
import random
import logging, threading
from celery import shared_task
import motor.motor_asyncio
from pymongo import MongoClient, ASCENDING, UpdateOne
from django.conf import settings
from urllib.parse import quote_plus


if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())



async def get_redis_batch_connection():
    return await aioredis.from_url("redis://localhost:6379/1")
redis_result = redis.Redis(host='localhost', port=6379, db=1)


encoded_username = quote_plus("admin")
encoded_password = quote_plus("hc$#@xf44")
mongo_client = MongoClient(f"mongodb://{encoded_username}:{encoded_password}@157.20.172.27:27017/?authSource=admin", maxPoolSize=4000) 
# mongo_client = motor.motor_asyncio.AsyncIOMotorClient(settings.MONGO_URI, maxPoolSize=3600)
db = mongo_client["insideemails"]
db.email_data.create_index([("email", ASCENDING)], unique=True)  # Email index
db.no_mx_domains.create_index([("domain", ASCENDING)], unique=True)
db.catch_all_domains.create_index([("domain", ASCENDING)], unique=True)




def update(batch_id, progress):
    batch_key = f'batch:{batch_id}'
    redis_client.hset(batch_key, "progress", progress)
    data = redis_client.hgetall(batch_key)
    batch_task = {key.decode(): value.decode() for key, value in data.items()}
    
    if not batch_task:
        print(f"No batch task found for batch_id: {batch_id}")
        return
    service_type = batch_task.get('service_type')
    post_id = batch_task.get('post_id', '')
    initial_count = batch_task.get('initial_count', '')
    status = batch_task.get('status', 'Processing')
    client_ip = batch_task.get('client_ip') or None
    results = json.loads(batch_task.get('results', '{}'))

    if  service_type == 'bulk_verify':
        if progress == 100:
            df = pd.DataFrame(list(results.items()), columns=["Email", "Status"])
            status_counts = df['Status'].value_counts()

            # Assign counts with default value 0 if missing
            valid_count = status_counts.get("Valid", 0)
            invalid_count = status_counts.get("Invalid", 0)
            # no_mx = status_counts.get("Invalid", 0)
            # spamBlock_count = status_counts.get("Spam Block", 0)
            catchall_count = status_counts.get("Catch All", 0)
            unknown_count = status_counts.get("Unknown", 0)
            disposable_count = status_counts.get("Disposable", 0)

            # Save DataFrame to CSV
            output_file_name = batch_task.get("output_file_name")
            if output_file_name:
                output_file_name = generate_unique_file_name(output_file_name)
                media_path = os.path.join(settings.MEDIA_ROOT, output_file_name)
                os.makedirs(settings.MEDIA_ROOT, exist_ok=True)
                df.to_csv(media_path, index=False)
                output_file_name = os.path.basename(output_file_name)
                output_file_url = f'https://verifier.insideemails.com/media/{output_file_name}'
            else:
                output_file_url = ""

            post_update_payload = {
                "post_id": post_id,
                "post_content": f'<div id="status" style="display: none;">{status}</div>'
                                f'<div id="email_count" style="display: none;">{initial_count}</div>'
                                f'<div id="progress" style="display: none;">{progress}</div><br> '
                                f'<div id="download-btn" style="display: none;">{output_file_url}</div> '
                                f'<div id="deliverable_count" style="display: none;">{valid_count}</div><br> '
                                f'<div id="undeliverable_count" style="display: none;">{invalid_count}</div><br> '
                                f'<div id="catchall_count" style="display: none;">{catchall_count}</div><br> '
                                f'<div id="unknown_count" style="display: none;">{unknown_count}</div><br> '
                                f'<div id="disposable_count" style="display: none;">{disposable_count}</div><br> '
                                f'[email_verification_report output_file_url="{output_file_url}" '
                                f'output_file_name="{output_file_name}" deliverable_count={valid_count} undeliverable_count={invalid_count} '
                                f'catchall_count={catchall_count} unknown_count={unknown_count} '
                                f'disposable_count={disposable_count} '
                                f'total_count={initial_count}]'
            }
        else:
            post_update_payload = {
                "post_id": post_id,
                "post_content": f'<div id="status" style="display: none;">{status}</div>'
                                f'<div id="email_count" style="display: none;">{initial_count}</div>'
                                f'<div id="progress" style="display: none;">{progress}</div>'
                                f'<br><div id="download-btn" style="display: none;"></div> '
            }
        return post_update_payload
    elif service_type == 'single_verify_daily':
        email, dailystatus = next(iter(results.items()), (None, None))
            
        post_update_payload = {
            "client_ip": client_ip,
            "email": email,
            "status" : dailystatus

        }
        return post_update_payload



    elif service_type == 'single_verify':
        if progress == 100:
            print(f"catch-all result : {results}")
            value = next(iter(results.values()), None)  # Prevent StopIteration
            post_update_payload = {
                "post_id": post_id,
                "post_content": f'<div id="status" style="display: none;">{value}</div>'
                                f'<div id="progress" style="display: none;">{progress}</div>',
            }
            return post_update_payload
        else:
            return

    elif service_type == 'email_finder':
        from .views import wallet_action
        if progress == 100:
            valid_emails_found = [email for email, status in results.items() if status == "Valid"]
            print(f"valid_emails_found {valid_emails_found}")
            wallet_action(wp_user_id=batch_task.get('wpuser_id'), action="debit", credit_count=len(valid_emails_found), file_name="Email Finder")

            post_update_payload = {
                "post_id": post_id,
                "post_content": f'<div id="status" style="display: none;">Completed</div>'
                                f'<div id="progress" style="display: none;">{progress}</div>'
                                f'<div id="emails">{", ".join(valid_emails_found)}</div>',
            }
        else:
            post_update_payload = {
                "post_id": post_id,
                "post_content": f'<div id="status" style="display: none;">Processing</div>'
                                f'<div id="progress" style="display: none;">{progress}</div>'
                                f'<div id="emails"></div>',
            }
        
        return post_update_payload




            



# Thread-safe lock for batch_tasks updates
# batch_lock = asyncio.Lock()
# semaphore = asyncio.Semaphore(10)  # Limit concurrent requests

# async def update_frontend_async(progress, batch_id):
#     """Send progress updates asynchronously to the frontend."""
#     post_update_url = "https://insideemails.com/?wpwhpro_action=update-post-wp&wpwhpro_api_key=gukzza6i6qykirfkwc0maa59s28zxer0t6kn41uzi5j8k31cqmjkjjkqnhze6mbk&action=update_post"

#     async with batch_lock:  
#         if batch_id not in batch_tasks:
#             print(f"Error: batch_id {batch_id} not found in batch_tasks.")
#             return
        
#         batch = batch_tasks[batch_id]
#         data = {"batch_id": batch_id, "progress": progress}
#         post_update_payload = {}

#         if batch.get("service_type") == "bulk_verify":
#             if progress == 100:
#                 print('Results:', batch.get('results', {}).values())

#                 df = pd.DataFrame(list(batch.get("results", {}).items()), columns=["Email", "Status"])
#                 status_counts = df['Status'].value_counts().to_dict()

#                 # Extract counts with defaults
#                 valid_count = status_counts.get("Valid", 0)
#                 invalid_count = status_counts.get("Invalid", 0)
#                 no_mx = status_counts.get("Invalid", 0)
#                 spamBlock_count = status_counts.get("Spam Block", 0)
#                 risky_count = status_counts.get("Catch All", 0)
#                 unknown_count = status_counts.get("Unknown", 0)
#                 tempLimited_count = status_counts.get("Disposable", 0)

#                 output_file_name = batch.get("output_file_name", "")
#                 if output_file_name:
#                     df.to_csv(output_file_name, index=False)
#                     output_file_name = os.path.basename(output_file_name)
#                     output_file_url = f'https://verifier.insideemails.com/media/{output_file_name}'
#                 else:
#                     output_file_url = ""

#                 post_update_payload = {
#                     "post_id": batch.get("post_id"),
#                     "post_content": f'''
#                         <div id="status" style="display: none;">{batch.get("status")}</div>
#                         <div id="email_count" style="display: none;">{batch.get("initial_count", 0)}</div>
#                         <div id="progress" style="display: none;">{progress}</div>
#                         <br> [email_verification_report output_file_url="{output_file_url}" 
#                         deliverable_count={valid_count} undeliverable_count={invalid_count} 
#                         risky_count={risky_count} unknown_count={unknown_count} 
#                         spamBlock_count={spamBlock_count} tempLimited_count={tempLimited_count} 
#                         total_count={batch.get("initial_count", 0)}]'''
#                 }
#             else:
#                 post_update_payload = {
#                     "post_id": batch.get("post_id"),
#                     "post_content": f'''
#                         <div id="status" style="display: none;">{batch.get("status")}</div>
#                         <div id="email_count" style="display: none;">{batch.get("initial_count", 0)}</div>
#                         <div id="progress" style="display: none;">{progress}</div>
#                         <br> <a href="#" style="display: none;"></a>'''
#                 }

#         elif batch.get("service_type") == "single_verify":
#             if progress == 100:
#                 value = next(iter(batch.get("results", {}).values()), "Unknown")
#                 post_update_payload = {
#                     "post_id": batch.get("post_id"),
#                     "post_content": f'''
#                         <div id="status" style="display: none;">{value}</div>
#                         <div id="progress" style="display: none;">{progress}</div>'''
#                 }
#             else:
#                 return  # Avoid sending unnecessary requests

#         elif batch.get("service_type") == "email_finder":
#             from .views import wallet_action
#             if progress == 100:
#                 valid_emails_found = [
#                     email for email, status in batch.get("results", {}).items() if status == "Valid"
#                 ]
#                 print(f"Valid emails found: {valid_emails_found}")

#                 wallet_action(
#                     wp_user_id=batch.get("wpuser_id"),
#                     action="debit",
#                     credit_count=len(valid_emails_found),
#                     file_name="Email Finder"
#                 )

#                 post_update_payload = {
#                     "post_id": batch.get("post_id"),
#                     "post_content": f'''
#                         <div id="status" style="display: none;">Completed</div>
#                         <div id="progress" style="display: none;">{progress}</div>
#                         <div id="emails">{", ".join(valid_emails_found)}</div>'''
#                 }
#             else:
#                 post_update_payload = {
#                     "post_id": batch.get("post_id"),
#                     "post_content": f'''
#                         <div id="status" style="display: none;">Processing</div>
#                         <div id="progress" style="display: none;">{progress}</div>
#                         <div id="emails"></div>'''
#                 }

#     # Send the update request with retries
#     async with semaphore:
#         for attempt in range(3):  # Retry up to 3 times
#             try:
#                 async with aiohttp.ClientSession() as session:
#                     async with session.post(post_update_url, json=post_update_payload, timeout=5) as response:
#                         if response.status == 200:
#                             print(f"Frontend update successful: {batch_id}")
#                             return
#                         else:
#                             print(f"Frontend update failed [{response.status}], attempt {attempt + 1}")

#             except Exception as e:
#                 print(f"Error updating frontend [{attempt + 1}]: {e}")

#             await asyncio.sleep(2 ** attempt)  # Exponential backoff

#frontend_update_semaphore = asyncio.Semaphore(10) 







# @sync_to_async
# def update_batch_task(batch_id):
#     """Wrap the database query for batch task."""
#     return BatchTask.objects.get(batch_id=batch_id)


# @sync_to_async
# def bulk_create_email_data(batch_results):
#     """Wrap bulk_create to avoid database access in async context."""
#     with transaction.atomic():
#         # Prepare the list of EmailData objects to insert
#         email_objs = [
#             EmailData(email=email, status=status)
#             for email, status in batch_results.items()
#         ]
        
#         # Insert data using raw SQL with ON DUPLICATE KEY UPDATE (MySQL)
#         values = ', '.join([f"('{email.email}', '{email.status}')" for email in email_objs])
#         sql = f"""
#             INSERT INTO ev_backend_emaildata (email, status)
#             VALUES {values}
#             ON DUPLICATE KEY UPDATE
#             status=VALUES(status), checked_at=NOW();
#         """
        
#         # Execute raw SQL
#         with connection.cursor() as cursor:
#             cursor.execute(sql)


# @sync_to_async
# def update_file_name(batch_id, output_file_name):
#     """Wrap the database query for batch task."""
#     return
#     # batch = BatchTask.objects.get(batch_id=batch_id)
#     # batch.output_file_name = output_file_name
#     # batch.save()

def generate_unique_file_name(output_file_name):
    # Check if file already exists, if so, add a counter to the file name
    base_name, extension = os.path.splitext(output_file_name)
    counter = 1
    while os.path.exists(os.path.join(settings.MEDIA_ROOT, output_file_name)):
        output_file_name = f"{base_name}_{counter}{extension}"
        counter += 1
    return output_file_name








# async def update_frontend_async(progress, batch_id):
#     """Send progress updates asynchronously to the frontend with thread safety."""
    
#     post_update_url = "https://insideemails.com/?wpwhpro_action=update-post-wp&wpwhpro_api_key=gukzza6i6qykirfkwc0maa59s28zxer0t6kn41uzi5j8k31cqmjkjjkqnhze6mbk&action=update_post"
#     semaphore = get_frontend_update_semaphore()
#     async with semaphore:  # Ensure only one update happens at a time
#         try:
            
#             batch = await update_batch_task(batch_id)  # Lock batch row for safe updates
#             service_type = batch.service_type
#             batch_results = batch.results
#             batch_status = batch.status
#             post_id = batch.post_id
#             initial_count = batch.initial_count
#             output_file_name = batch.output_file_name

#             if service_type == 'email_verification_through_api':
#                 if progress == 100:
#                     await bulk_create_email_data(batch_results)
#                     df = pd.DataFrame(list(batch_results.items()), columns=["Email", "Status"])
#                     output_file_name = generate_unique_file_name(output_file_name)
#                     media_path = os.path.join(settings.MEDIA_ROOT, output_file_name)
#                     os.makedirs(settings.MEDIA_ROOT, exist_ok=True)
#                     df.to_csv(media_path, index=False)
#                     await update_file_name(batch_id, output_file_name)
#                 return

#             elif service_type == 'bulk_verify':
#                 if progress == 100:
#                     print('Results:', batch_results.values())
#                     await bulk_create_email_data(batch_results)
#                     df = pd.DataFrame(list(batch_results.items()), columns=["Email", "Status"])

#                     status_counts = df['Status'].value_counts()
                    
#                     valid_count = status_counts.get("Valid", 0)
#                     invalid_count = status_counts.get("Invalid", 0)
#                     no_mx = status_counts.get("Invalid", 0)
#                     spamBlock_count = status_counts.get("Spam Block", 0)
#                     risky_count = status_counts.get("Catch All", 0)
#                     unknown_count = status_counts.get("Unknown", 0)
#                     tempLimited_count = status_counts.get("Disposable", 0)
#                     output_file_name = generate_unique_file_name(output_file_name)
#                     media_path = os.path.join(settings.MEDIA_ROOT, output_file_name)
#                     os.makedirs(settings.MEDIA_ROOT, exist_ok=True)
#                     df.to_csv(output_file_name, index=False)
#                     output_file_url = f'https://verifier.insideemails.com/media/{output_file_name}'
#                     await update_file_name(batch_id, output_file_name)
#                     post_update_payload = {
#                         "post_id": post_id,
#                         "post_content": f'''<div id="status" style="display: none;">{batch_status}</div>
#                                             <div id="email_count" style="display: none;">{initial_count}</div>
#                                             <div id="progress" style="display: none;">{progress}</div><br> 
#                                             [email_verification_report output_file_url="{output_file_url}" 
#                                             deliverable_count={valid_count} 
#                                             undeliverable_count={invalid_count} 
#                                             risky_count={risky_count} 
#                                             unknown_count={unknown_count} 
#                                             spamBlock_count={spamBlock_count} 
#                                             tempLimited_count={tempLimited_count} 
#                                             total_count={initial_count}]'''
#                     }
#                 else:
#                     post_update_payload = {
#                         "post_id": post_id,
#                         "post_content": f'''<div id="status" style="display: none;">{batch_status}</div>
#                                             <div id="email_count" style="display: none;">{initial_count}</div>
#                                             <div id="progress" style="display: none;">{progress}</div>
#                                             <br> <a href='#' style="display: none;"></a>''',
#                     }

#             elif service_type == 'single_verify':
#                 if progress == 100:
#                     print(f"Catch-all result: {batch_results}")
#                     await bulk_create_email_data(batch_results)
#                     value = next(iter(batch_results.values()), None)  # Prevent StopIteration
#                     print(f'value : {value}')
#                     post_update_payload = {
#                         "post_id": post_id,
#                         "post_content": f'''<div id="status" style="display: none;">{value}</div>
#                                             <div id="progress" style="display: none;">{progress}</div>''',
#                     }
#                 else:
#                     return  # No update needed

#             elif service_type == 'email_finder':
#                 from .views import wallet_action
#                 if progress == 100:
#                     valid_emails_found = [email for email, status in batch_results.items() if status == "Valid"]
#                     print(f"Valid emails found: {valid_emails_found}")
#                     await bulk_create_email_data(batch_results)

#                     wallet_action(
#                         wp_user_id=batch.wpuser_id, 
#                         action="debit", 
#                         credit_count=len(valid_emails_found), 
#                         file_name="Email Finder"
#                     )

#                     post_update_payload = {
#                         "post_id": post_id,
#                         "post_content": f'''<div id="status" style="display: none;">Completed</div>
#                                             <div id="progress" style="display: none;">{progress}</div>
#                                             <div id="emails">{", ".join(valid_emails_found)}</div>''',
#                     }
#                 else:
#                     post_update_payload = {
#                         "post_id": post_id,
#                         "post_content": f'''<div id="status" style="display: none;">Processing</div>
#                                             <div id="progress" style="display: none;">{progress}</div>
#                                             <div id="emails"></div>''',
#                     }
            
#             async with aiohttp.ClientSession() as session:
#                 async with session.post(post_update_url, json=post_update_payload, timeout=5) as response:
#                     if response.status != 200:
#                         print(f"Frontend update failed: {response.status}")

#             print(f"Frontend update successful for batch {batch_id}")

#         except BatchTask.DoesNotExist:
#             print(f"Batch {batch_id} not found.")
#         except Exception as e:
#             print(f"Error updating frontend: {e}")



def generate_random_triggers():
    """Generate exactly 10 random trigger points between 1 and 100."""
    random_points = sorted(random.sample(range(1, 100), 30))  # Ensure 10 points
    return set(random_points)


# Initialize Redis connection
async def get_redis_connection():
    return await aioredis.from_url("redis://localhost:6379/1")

async def fetch_mx_records_async(domain):
    """Fetch MX records asynchronously with Redis caching."""
    redis = await get_redis_connection()

    # ✅ Check Redis cache first
    cached_mx = await redis.get(f"mx:{domain}")  # Use `await` for async Redis
    if cached_mx:
        return json.loads(cached_mx)  # Return cached result

    resolver = aiodns.DNSResolver(timeout=10)
    try:
        answers = await resolver.query(domain, 'MX')
        mx_records = sorted([(record.priority, record.host) for record in answers], key=lambda x: x[0])
        mx_hosts = [record[1] for record in mx_records]

        # ✅ Store result in Redis with a TTL of 24 hours
        await redis.setex(f"mx:{domain}", 86400, json.dumps(mx_hosts))  # Use `await`
        return mx_hosts
    except Exception as e:
        print(f"Error fetching MX records: {e}")
        return []


# async def fetch_mx_records_async(domain):
#     if domain in dns_cache:
#         return dns_cache[domain]
#     resolver = aiodns.DNSResolver(timeout=10)
#     try:
#         answers = await resolver.query(domain, 'MX')
#         mx_records = sorted([(record.priority, record.host) for record in answers], key=lambda x: x[0])
#         dns_cache[domain] = [record[1] for record in mx_records]
#         return dns_cache[domain]
#     except:
#         return []


# def fetch_mx_records(domain):
#     try:
#         loop = asyncio.get_running_loop()
#         future = asyncio.ensure_future(fetch_mx_records_async(domain))
#         return loop.run_until_complete(future)
#     except RuntimeError:
#         return asyncio.run(fetch_mx_records_async(domain))

def fetch_mx_records(domain):
    try:
        return asyncio.run(fetch_mx_records_async(domain))
    except RuntimeError:  # If already inside an event loop (e.g., in an async context)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(fetch_mx_records_async(domain))


class ProxySMTP(smtplib.SMTP):
    def __init__(self, host='', port=0, proxy_host=None, proxy_port=None, proxy_user=None, proxy_password=None, *args, **kwargs):
        self.proxy_host = proxy_host
        self.proxy_port = proxy_port
        self.proxy_user = proxy_user
        self.proxy_password = proxy_password
        super().__init__(host, port, *args, **kwargs)
    
    def _get_socket(self, host, port, timeout):
        sock = socks.socksocket()
        sock.set_proxy(socks.SOCKS5, self.proxy_host, self.proxy_port, username=self.proxy_user, password=self.proxy_password)
        sock.settimeout(timeout)
        sock.connect((host, port))
        return sock



@shared_task
def verify_email_via_smtp(sender, recipient, proxy_host, proxy_port, proxy_user, proxy_password, batch_id,total_emails , try_round=1):
    recipient = recipient.lower()

    def is_valid_email(email):
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    status = True if is_valid_email(recipient) else "Invalid"
    if (status):
        email_data = db.email_data.find_one({"email": recipient})
        status = email_data.get("status") if email_data else None
        
            
        if status is None or status == "Spam Block" or  status == "Error": # Catches both "not found" and "spam block"
            domain = recipient.split('@')[1]
            
            no_mx_entry = db.no_mx_domains.find_one({"domain": domain})
            status = "Invalid" if no_mx_entry else None
            if (status is None):
                mx_records = fetch_mx_records(domain)
                if not mx_records:
                    db.no_mx_domains.update_one(
                            {"domain": domain},  # Filter by domain
                            {"$set": {"checked_at": time.time()}},  # Update checked_at
                            upsert=True  # Insert if not exists
                        )
                    status = "Invalid"
                else:
                    smtp_server = mx_records[0]
                    smtp_port = 25
                    
                    try:
                        with ProxySMTP(smtp_server, smtp_port, proxy_host=proxy_host, proxy_port=proxy_port, proxy_user=proxy_user, proxy_password=proxy_password, timeout=60) as server:
                            server.ehlo()
                            server.mail(sender)
                            code, message = server.rcpt(recipient)
                            server_message = message.decode('utf-8')

                            if code in [250, 251, 252]:
                                status = "Valid"
                            elif code in [550, 551, 552, 553, 554]:
                                if 'spam policy' in server_message.lower() or 'blocked using spamhaus' in server_message.lower():
                                    status = "Spam Block"
                                else:
                                    status =  "Invalid"
                            elif code in [450, 451, 452, 421]:
                                status =  "Disposable"
                            else:
                                status = "Unknown"
                    except socket.timeout:
                        status = "Unknown"
                    except Exception as e:
                        status = "Error" if try_round == 1 else "Unknown"

    batch_key = f"batch:{batch_id}"    
    with redis_client.pipeline() as pipe:
        comple = redis_client.hget(batch_key, "completed")
        print(f"completed: {comple} round: {try_round}" )
        while True:
            try:
                pipe.watch(batch_key)
                pipe.multi()
                pipe.hincrby(batch_key, "completed", 1)
                print("INCREMENT")
                pipe.execute()
                break
            except redis.WatchError:
                continue
    
    completed = int(redis_client.hget(batch_key, "completed") or 0)
    total_emails = redis_client.hget(batch_key, "total")
    total_emails = int(total_emails) if total_emails else 0

    if total_emails > 0:
        base = 40 if try_round == 1 else 30
        offset = 0 if try_round == 1 else 40
        progress = round((completed / total_emails) * base + offset)
    else:
        progress = 0
    triggers = json.loads(redis_client.hget(batch_key, "triggers") or "[]")  # Convert back to list
    print(triggers)
    last_progress = int(redis_client.hget(batch_key, "last_progress") or 0)
    client_ip = redis_client.hget(batch_key, "client_ip")
    client_ip = client_ip.decode() if client_ip else None
    if progress in triggers and progress != last_progress:
        print("triggered in triggers")
        redis_client.hset(batch_key, "last_progress", progress)
        post_content = update(batch_id, progress)
        if post_content and progress!=100 and not client_ip:
            post_id = int(redis_client.hget(batch_key, "post_id") or 0)
            channel_layer = get_channel_layer()
            async_to_sync(channel_layer.group_send)(
                "global_progress",
                {
                    "type": "progress_update",
                    "post_id": post_id,
                    "post_content": post_content["post_content"] 
                }
            )
        
    return {recipient: status}


@shared_task()
def verify_email_for_catchall(sender, recipient, proxy_host, proxy_port, proxy_user, proxy_password,batch_id, total_emails):
    recipient = recipient.lower()
    domain = recipient.split('@')[1]


    catch_all_domain = db.catch_all_domains.find_one({"domain": domain})
    status = "Catch All" if catch_all_domain else None
    if (status is None):
        mx_records = fetch_mx_records(domain)
        if not mx_records:
            db.no_mx_domains.update_one(
                    {"domain": domain},  # Filter by domain
                    {"$set": {"checked_at": time.time()}},  # Update checked_at
                    upsert=True  # Insert if not exists
                )
            status = "Invalid"
        else:
            smtp_server = mx_records[0]
            smtp_port = 25
            invalid_email = f"non8xistent1.@{domain}"
            
            try:
                with ProxySMTP(smtp_server, smtp_port, proxy_host=proxy_host, proxy_port=proxy_port, proxy_user=proxy_user, proxy_password=proxy_password, timeout=60) as server:
                    server.ehlo()
                    server.mail(sender)
                    code, message = server.rcpt(invalid_email)
                    if code in [250, 251, 252]:
                        db.catch_all_domains.update_one(
                            {"domain": domain},  # Filter by domain
                            {"$set": {"checked_at": time.time()}},  # Update checked_at
                            upsert=True  # Insert if not exists
                        )
                        status = "Catch All"
                    else:
                        status = "Valid"
            except:
                status = "Unknown"

    batch_key = f"batch:{batch_id}" 
           
    with redis_client.pipeline() as pipe:
        comple = redis_client.hget(batch_key, "completed")
        print(f"completed: {comple} round: 3" )
        while True:
            try:
                pipe.watch(batch_key)
                pipe.multi()
                pipe.hincrby(batch_key, "completed", 1)
                print("INCREMENT")
                pipe.execute()
                break
            except redis.WatchError:
                continue 
    
    completed = int(redis_client.hget(batch_key, "completed") or 0)
    total_emails = redis_client.hget(batch_key, "total")
    total_emails = int(total_emails) if total_emails else 0

    if total_emails > 0:
        base =  30
        offset = 70
        progress = round((completed / total_emails) * base + offset)
    else:
        progress = 0
    triggers = json.loads(redis_client.hget(batch_key, "triggers") or "[]")  # Convert back to list
    print(triggers)
    last_progress = int(redis_client.hget(batch_key, "last_progress") or 0)
    client_ip = redis_client.hget(batch_key, "client_ip")
    client_ip = client_ip.decode() if client_ip else None
    if progress in triggers and progress != last_progress:
        redis_client.hset(batch_key, "last_progress", progress)
        print("triggered in triggers")
        post_content = update(batch_id, progress)
        if post_content and progress!=100 and not client_ip: 
            post_id = int(redis_client.hget(batch_key, "post_id") or 0)
            channel_layer = get_channel_layer()
            async_to_sync(channel_layer.group_send)(
                "global_progress",
                {
                    "type": "progress_update",
                    "post_id": post_id,
                    "post_content": post_content["post_content"]
                }
            )
        

    

    return {recipient: status}


redis_client = redis.Redis(host="localhost", port=6379, db=1)


@shared_task
def process_first_round_results(first_round_results_list, batch_id, sender, proxy_host, proxy_port, proxy_user, proxy_password, queue_name):
    """Processes first-round results and triggers spam-block retry using Redis for async batch tracking."""
    first_round_results = {email: status for result in first_round_results_list for email, status in result.items()}

    # Get Redis connection
    # Retrieve existing batch progress from Redis (or set initial values)
    batch_key = f"batch:{batch_id}"
    # batch_status_key = f"batch_status:{batch_id}"
    # current_completed =  redis_client.hget(batch_key, "completed")
    # print(f"current_completed first round {current_completed}")
    # current_completed = int(current_completed) if current_completed else 0
    
    # Update progress asynchronously
    # new_completed = current_completed
    # print(f"current_completed first round updated {new_completed}")
    redis_client.hset(batch_key, "total_completed", len(first_round_results_list))
    redis_client.hset(batch_key, "results", json.dumps(first_round_results))
    spam_blocked_emails = [email for email, status in first_round_results.items() if status in ["Spam Block", "Error"]]

    if spam_blocked_emails:
        random_triggers = generate_random_triggers()
        sorted_triggers = sorted(random_triggers)
        total_emails = len(spam_blocked_emails)

        # Update Redis batch status
        redis_client.hmset(batch_key, {
            "total": total_emails,
            "completed": 0,
             "triggers": json.dumps(sorted_triggers),
            "status": "processing",
            "spam_block_retries": len(spam_blocked_emails),
        })

        # Create retry tasks for spam-blocked emails
        retry_tasks = group(
            verify_email_via_smtp.s(sender, email, proxy_host, proxy_port, proxy_user, proxy_password, batch_id, total_emails, try_round=2).set(queue=queue_name) 
            for email in spam_blocked_emails
        )

        # Execute tasks and process spam block results
        return chord(retry_tasks)(process_spam_block_results.s(first_round_results, batch_id, sender, proxy_host, proxy_port, proxy_user, proxy_password, queue_name).set(queue=queue_name) )
    else:
        return process_spam_block_results([], first_round_results, batch_id, sender, proxy_host, proxy_port, proxy_user, proxy_password, queue_name)
    
@shared_task
def process_spam_block_results(retry_results_list , first_round_results , batch_id, sender, proxy_host, proxy_port, proxy_user, proxy_password, queue_name):
    """Merges spam-block retry results and triggers valid email processing."""
    

    retry_results = {email: status for result in retry_results_list for email, status in result.items()}
    # Get Redis connection

    # Retrieve existing batch progress from Redis (or set initial values)
    batch_key = f"batch:{batch_id}"
    batch_status_key = f"batch_status:{batch_id}"
    # current_completed = redis_client.hget(batch_key, "completed")
    # print(f"current_completed spam block {current_completed}")
    # current_completed = int(current_completed) if current_completed else 0
    # # Update progress asynchronously
    # new_completed = current_completed + len(retry_results_list)
    # print(f"current_completed spam block updated {new_completed}")

    redis_client.hincrby(batch_key, "total_completed", len(retry_results_list))
    
  
    for email, new_status in retry_results.items():
        if email in first_round_results:  #  Only update existing keys
            first_round_results[email] = new_status 
    redis_client.hset(batch_key, "results", json.dumps(first_round_results))
    
    # Identify Valid Emails
    valid_emails = [email for email, status in first_round_results.items() if status == "Valid"]
    total_emails = len(valid_emails)
    if valid_emails:
        random_triggers = generate_random_triggers()
        sorted_triggers = sorted(random_triggers)
        redis_client.hmset(batch_key, {
            "total": total_emails,
            "completed": 0,
             "triggers": json.dumps(sorted_triggers),
            "status": "processing",
            "valid_email_retries": len(valid_emails),
        })

        catch_all_tasks = group(
            verify_email_for_catchall.s(sender, email, proxy_host, proxy_port, proxy_user, proxy_password, batch_id, total_emails).set(queue=queue_name) 
            for email in valid_emails
        )
        print(f" SB {first_round_results}")
        return chord(catch_all_tasks)(finalize_results.s(first_round_results, batch_id, queue_name).set(queue=queue_name))
    else:
        return finalize_results([], first_round_results, batch_id, queue_name)


# @shared_task
# def process_spam_block_results(retry_results_list , first_round_results , batch_id, sender, proxy_host, proxy_port, proxy_user, proxy_password, third_quarter_parts):
#     """Merges spam-block retry results and triggers valid email processing."""
#     # Update first_round_results with retried statuses
#     if isinstance(retry_results_list, list):  # More robust type check
#         retry_results = {}
#         for result in retry_results_list:
#             retry_results.update(result)
  

#     for email, new_status in retry_results.items():
#                 if email in first_round_results:  #  Only update existing keys
#                     first_round_results[email] = new_status 

#     # Identify Valid Emails
#     valid_emails = [email for email, status in first_round_results.items() if status == "Valid"]

#     if valid_emails:
#         total_emails = len(valid_emails)
#         batch_tasks[batch_id].update({
#             "total": total_emails,
#             "completed": 0,
#             "status": "processing",
#             "part1": third_quarter_parts[0],
#             "part2": third_quarter_parts[1],
#             "part3": third_quarter_parts[2],
#             "part4": third_quarter_parts[3],
#         })
#         batch_tasks[batch_id]["valid_email_retries"] = len(valid_emails)

#         catch_all_tasks = group(
#             verify_email_for_catchall.s(sender, email, proxy_host, proxy_port, proxy_user, proxy_password, batch_id, total_emails) 
#             for email in valid_emails
#         )
#         print(f" SB {first_round_results}")
#         return chord(catch_all_tasks)(finalize_results.s(first_round_results, batch_id))
#     else:
#         return finalize_results([],first_round_results, batch_id)


@shared_task
def finalize_results(catch_all_results_list, first_round_results, batch_id, queue_name):
    """Final step: Merges results and marks batch as complete."""
    
    print(f" FF {first_round_results}")
    print(f" CC {catch_all_results_list}")

    # Redis Keys
    batch_key = f"batch:{batch_id}"
    batch_status_key = f"batch_status:{batch_id}"
    
    # # Retrieve existing batch progress
    # current_completed = redis_client.hget(batch_key, "completed")
    # print(f"current_completed finalize {current_completed}")
    # current_completed = int(current_completed) if current_completed else 0
    # print(f"current_completed finalize updated {current_completed}")
    # # Update progress
    # new_completed = current_completed + len(catch_all_results_list)
    redis_client.hincrby(batch_key, "total_completed", len(catch_all_results_list))

    # Merge catch_all_results_list
    catch_all_results = {}
    if isinstance(catch_all_results_list, list):
        for result in catch_all_results_list:
            catch_all_results.update(result)

    # Update first_round_results
    if catch_all_results:
        for email, new_status in catch_all_results.items():
            if email in first_round_results:
                first_round_results[email] = new_status

    

    # Save results to CSV
    # if output_file_name is None :
    #     output_file_name = "Test.csv"
    # media_path = os.path.join(settings.MEDIA_ROOT, output_file_name)
    # os.makedirs(settings.MEDIA_ROOT, exist_ok=True)
    # df.to_csv(media_path, index=False)

    # MongoDB Bulk Write
    collection = db["email_data"]
    operations = [
        UpdateOne({"email": email}, {"$set": {"status": status}}, upsert=True)
        for email, status in first_round_results.items()
    ]
    
    if operations:
        result = collection.bulk_write(operations)
        print(f"Updated {result.modified_count} documents in MongoDB.")

    for email, status in first_round_results.items():
        if status == "Spam Block" or status == "No MX Records Found":
            first_round_results[email] = "Invalid"
    redis_client.hset(batch_key, "results", json.dumps(first_round_results))
    redis_client.hset(batch_key, "status", "completed")

    print(f"Completed processing for batch {batch_id}")
    data = redis_client.hgetall(batch_key)
    json_data = {key.decode(): value.decode() for key, value in data.items()}
    post_content = update(batch_id, 100)
    client_ipaddr =  json_data.get('client_ip') or None
    if client_ipaddr and post_content:
        channel_layer = get_channel_layer()
        async_to_sync(channel_layer.group_send)(
            f"dailyfree_progress_{client_ipaddr}",
            {
                "type": "forward.message",
                "email": post_content["email"],
                "status": post_content["status"]
            }
        )
    if post_content and not client_ipaddr:
        post_id = int(redis_client.hget(batch_key, "post_id") or 0)
        channel_layer = get_channel_layer()
        async_to_sync(channel_layer.group_send)(
            "global_progress",
            {
                "type": "progress_update",
                "post_id": post_id,
                "post_content": post_content["post_content"]
            }
        )
    print(f"Final update triggered for batch: {batch_id}")

    return {"batch_id": batch_id, "results": json_data, "queue_name":queue_name }

    
# @shared_task
# def finalize_results(catch_all_results_list, first_round_results , batch_id):
#     print(f" FF {first_round_results}")
#     print(f" CC {catch_all_results_list}")

#     """Final step: Merges results and marks batch as complete."""
    
#     if isinstance(catch_all_results_list, list):  # More robust type check
#         catch_all_results = {}
#         for result in catch_all_results_list:
#             catch_all_results.update(result)
    

#     if catch_all_results:
#         for email, new_status in catch_all_results.items():
#                 if email in first_round_results:  #  Only update existing keys
#                     first_round_results[email] = new_status 
#     print(f" C {catch_all_results}")
#     ##send user mail on completion on its mail id

#     batch_tasks[batch_id]["status"] = "completed"

#     # Ensure 'results' key is set
#     print(f" Final {first_round_results}")
#     batch_tasks[batch_id]["results"] = first_round_results
#     print("Final update triggered for:", batch_id)

#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)
#     loop.run_until_complete(update_frontend_async(100, batch_id))
#     loop.close()

#     return {"batch_id": batch_id, "results": first_round_results}




# async def add_to_batch_stream(batch_id, email, status, total_emails, round):
#     """Append email verification result to Redis Stream for batch processing"""

#     stream_key = f"batch_stream:{batch_id}"
#     redis_batch = await get_redis_batch_connection()

#     await redis_batch.xadd(
#         stream_key,
#         {"email": email, "status": status, "total_emails": total_emails, "round":round},
#         maxlen=100000  # Prevents stream from growing indefinitely
#     )



# async def process_batch_updates(batch_id):
#     """Read from Redis Stream and update batch progress in Redis Hash"""
#     redis_batch = await get_redis_batch_connection()
#     stream_key = f"batch_stream:{batch_id}"
#     batch_status_key = f"batch_status:{batch_id}"  # Redis Hash for tracking progress
#     last_id = "0"

#     while True:
#         # Read new messages from the stream
#         messages = await redis_batch.xread({stream_key: last_id}, count=500, block=5000)
#         if not messages:
#             continue
        
#         # Fetch completed count and existing results in one Redis call
#         batch_data = await redis_batch.hgetall(batch_status_key)
#         completed = int(batch_data.get("completed", 0))
#         results = json.loads(batch_data.get("results", "{}"))  # Convert from JSON to dict
#         round = int(batch_data.get("round", 1))
#         async with redis_batch.pipeline() as pipeline:
#             for stream, msgs in messages:
#                 for msg_id, fields in msgs:
#                     email = fields["email"]
#                     status = fields["status"]
#                     total_emails = int(fields["total_emails"])

#                     # Update results dictionary
#                     results[email] = status
#                     completed += 1

#                     last_id = msg_id  # Update last processed ID

#             # Store updated results and progress back to Redis
#             pipeline.hset(batch_status_key, "completed", completed)
#             pipeline.hset(batch_status_key, "progress", int((completed / total_emails) * 100))
#             pipeline.hset(batch_status_key, "results", json.dumps(results))  # Store as JSON
            
#             await pipeline.execute()

        # Optional: Send WebSocket progress update
        # await send_progress_update(batch_id, progress, round)
# async def get_batch_task_data(batch_id):
#     """Retrieve batch task data including email verification results."""
    
#     batch_status_key = f"batch_status:{batch_id}"
#     redis_batch =  await get_redis_batch_connection()
#     # Get batch data
#     batch_data = await redis_batch.hgetall(batch_status_key)

#     # Convert stored JSON results back to dictionary
#     results = json.loads(batch_data.get("results", "{}"))
    
#     return {
#         "batch_id": batch_id,
#         "completed": int(batch_data.get("completed", 0)),
#         "progress": int(batch_data.get("progress", 0)),
#         "results": results
#     }



# @shared_task
# def start_batch_processor(batch_id):
#     """Run the batch processing worker"""
#     def run_event_loop():
#         loop = asyncio.new_event_loop()
#         asyncio.set_event_loop(loop)
#         loop.run_until_complete(process_batch_updates(batch_id))
#         loop.close()

#     # Start the event loop inside a separate thread
#     threading.Thread(target=run_event_loop, daemon=True).start()

@shared_task
def verify_emails_in_parallel(sender, proxy_host, proxy_port, proxy_user, proxy_password, email_list, service_type, wpuser_id=0, post_id=0, output_file_name='',client_ip='', batch_id=None):
    if not batch_id:
        batch_id = f"batch_{int(time.time())}"
    total_emails = len(email_list)
    random_triggers = generate_random_triggers()
    sorted_triggers = sorted(random_triggers)
    
    batch_task_data = {
        "batch_id": batch_id,
        "total": total_emails,
        "output_file_name": output_file_name,
        "initial_count": total_emails,
        "completed": 0,
         "triggers": json.dumps(sorted_triggers),
        "status": "processing",
        "client_ip": client_ip,
        "post_id": post_id,
        "wpuser_id": wpuser_id,
        "service_type": service_type,
        "results": json.dumps({})
    }
    redis_client.hset(f"batch:{batch_id}", mapping=batch_task_data)  # Store as a hash
    redis_client.expire(f"batch:{batch_id}", 86400)  # Set expiry time to 24 hours
    # start_batch_processor.delay(batch_id)
    queue_name = verify_emails_in_parallel.request.delivery_info.get('routing_key', 'default')

    print(email_list)
    verification_tasks = group(
        verify_email_via_smtp.s(sender, email, proxy_host, proxy_port, proxy_user, proxy_password, batch_id=batch_id, total_emails=total_emails, try_round=1).set(queue=queue_name)
        for email in email_list
    )

    return chord(verification_tasks)(process_first_round_results.s(batch_id, sender, proxy_host, proxy_port, proxy_user, proxy_password, queue_name).set(queue=queue_name))

    


# # Get progress for a batch
# def get_batch_progress(batch_id):
#     if batch_id not in batch_tasks:
#         return {"error": "Batch ID not found"}

#     completed = batch_tasks[batch_id]["completed"]
#     total = batch_tasks[batch_id]["total"]
#     progress = int((completed / total) * 100) if total > 0 else 0

#     return {"batch_id": batch_id, "progress": progress, "status": batch_tasks[batch_id]["status"]}

# Monitor and clean up completed batches
# def manage_email_batches():
#     while True:
#         for batch_id in list(batch_tasks.keys()):
#             if batch_tasks[batch_id]["completed"] == batch_tasks[batch_id]["total"]:
#                 batch_tasks[batch_id]["status"] = "completed"
#                 del batch_tasks[batch_id] # Remove completed batch
#         global dns_cache, no_mx_domains
#         # Clear cache after 1 hr
#         dns_cache = {}
#         no_mx_domains = set()
#         time.sleep(3600)  # Check every 10 seconds

# # Run the batch manager in a separate thread
# batch_manager_thread = threading.Thread(target=manage_email_batches, daemon=True)
# batch_manager_thread.start()





@shared_task
def simple_task():
    print("Task executed!")
    return "Success"