from celery import Celery, group, chord
from celery.signals import worker_ready
from asgiref.sync import sync_to_async
from concurrent.futures import ThreadPoolExecutor, as_completed
import smtplib, threading
import socks
import aiodns, aiohttp, json
from redis.asyncio import Redis
import asyncio
import socket
import logging
import time, sys, os
import pandas as pd
import random
import logging, threading
from celery import shared_task
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.db import transaction
from .models import BatchTask, EmailData, CatchAllDomains, NoMXDomains

if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


# Initialize Celery

# Cache for MX records
# dns_cache = {}
# no_mx_domains = set()
# catch_all_domains = set()
# batch_tasks = {}





# async def update_frontend_async(progress, batch_id):
#     import os
#     """Send progress updates asynchronously to the frontend."""
#     post_update_url = "https://insideemails.com/?wpwhpro_action=update-post-wp&wpwhpro_api_key=gukzza6i6qykirfkwc0maa59s28zxer0t6kn41uzi5j8k31cqmjkjjkqnhze6mbk&action=update_post"
#     data = {"batch_id": batch_id, "progress": progress}
#     if batch_tasks[batch_id]['service_type'] == 'bulk_verify':

#         if progress == 100:

#             print('resutls ', batch_tasks[batch_id]['results'].values())
#             df = pd.DataFrame(list(batch_tasks[batch_id]['results'].items()), columns=["Email", "Status"])

#             status_counts = df['Status'].value_counts()

#             # Assign counts with default value 0 if missing
#             valid_count = status_counts.get("Valid", 0)
#             invalid_count = status_counts.get("Invalid", 0)
#             no_mx = status_counts.get("No MX Records Found", 0)
#             spamBlock_count = status_counts.get("Spam Block", 0)
#             risky_count = status_counts.get("Catch All", 0)
#             unknown_count = status_counts.get("Unknown", 0)
#             tempLimited_count = status_counts.get("Temporarily Limited", 0)
#             # Save DataFrame to CSV
#             output_file_name =  batch_tasks[batch_id]['output_file_name']
#             print('file' , output_file_name)
#             df.to_csv(output_file_name, index=False)
#             output_file_name  = os.path.basename(output_file_name)
#             # output_file_name = '27K Data.csv'
#             print('file' , output_file_name)
#             output_file_url = f'https://verifier.insideemails.com/media/{output_file_name}'
            
            

#             post_update_payload = {
            
#                            "post_id": batch_tasks[batch_id]['post_id'],
#                             "post_content": f'<div id="status" class="d-none">{batch_tasks[batch_id]["status"]}</div><div id="email_count" class="d-none">{batch_tasks[batch_id]["initial_count"]}</div><div id="progress" class="d-none">{progress}</div><br> [email_verification_report output_file_url=f"{output_file_url}"  deliverable_count={valid_count} undeliverable_count={invalid_count} risky_count= {risky_count} unknown_count = {unknown_count} spamBlock_count = {spamBlock_count} tempLimited_count = {tempLimited_count} total_count = {batch_tasks[batch_id]["initial_count"]}]'
#                         }
#         else:  
#             post_update_payload = {
            
#                             "post_id": batch_tasks[batch_id]['post_id'],
#                             "post_content": f'''<div id="status" class="d-none">{batch_tasks[batch_id]['status']}</div>
#                                                 <div id="email_count" class="d-none">{batch_tasks[batch_id]['initial_count']}</div>
#                                                 <div id="progress" class="d-none">{progress}</div>
#                                                 <br> <a href='#' class="d-none"></a>''',
#                         }
    
#     if batch_tasks[batch_id]['service_type'] == 'single_verify':
#         if progress == 100:
#             print(f"catch-all result : {batch_tasks[batch_id]["results"]}")
#             # value = next(iter(batch_tasks[batch_id]['results'].values()))
#             value = next(iter(batch_tasks[batch_id]['results'].values()), None)  # Prevent StopIteration
#             post_update_payload = {
            
#                             "post_id": batch_tasks[batch_id]['post_id'],
#                             "post_content": f'''<div id="status" class="d-none">{value}</div>
#                                                 <div id="progress" class="d-none">{progress}</div>''',
#                         }
#         else:
#             return
    
#     if batch_tasks[batch_id]['service_type'] == 'email_finder':
#         from .views import wallet_action
#         if progress == 100:
#             valid_emails_found = [email for email, status in batch_tasks[batch_id]['results'].items() if status == "Valid"]
#             print(f"valid_emails_found {valid_emails_found}")
#             wallet_action(wp_user_id= batch_tasks[batch_id]['wpuser_id'], action="debit", credit_count=len(valid_emails_found), file_name="Email Finder")
    
#             post_update_payload = {
            
#                             "post_id": batch_tasks[batch_id]['post_id'],
#                             "post_content": f'''<div id="status" class="d-none">Completed</div>
#                                                 <div id="progress" class="d-none">{progress}</div>
#                                                <div id="emails">{", ".join(valid_emails_found)}</div>''',
#                         }
#         else:
#             post_update_payload = {
            
#                             "post_id": batch_tasks[batch_id]['post_id'],
#                             "post_content": f'''<div id="status" class="d-none">Processing</div>
#                                                 <div id="progress" class="d-none">{progress}</div>
#                                                <div id="emails"></div>''',
#                         }
            
            
#     async with aiohttp.ClientSession() as session:
#         try:
#             async with session.post(post_update_url, json=post_update_payload, timeout=5) as response:
#                 print('request sent')
#                 if response.status != 200:
#                     print(f"Frontend update failed: {response.status}")
#         except Exception as e:
#             print(f"Error updating frontend: {e}")


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
#                 no_mx = status_counts.get("No MX Records Found", 0)
#                 spamBlock_count = status_counts.get("Spam Block", 0)
#                 risky_count = status_counts.get("Catch All", 0)
#                 unknown_count = status_counts.get("Unknown", 0)
#                 tempLimited_count = status_counts.get("Temporarily Limited", 0)

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
#                         <div id="status" class="d-none">{batch.get("status")}</div>
#                         <div id="email_count" class="d-none">{batch.get("initial_count", 0)}</div>
#                         <div id="progress" class="d-none">{progress}</div>
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
#                         <div id="status" class="d-none">{batch.get("status")}</div>
#                         <div id="email_count" class="d-none">{batch.get("initial_count", 0)}</div>
#                         <div id="progress" class="d-none">{progress}</div>
#                         <br> <a href="#" class="d-none"></a>'''
#                 }

#         elif batch.get("service_type") == "single_verify":
#             if progress == 100:
#                 value = next(iter(batch.get("results", {}).values()), "Unknown")
#                 post_update_payload = {
#                     "post_id": batch.get("post_id"),
#                     "post_content": f'''
#                         <div id="status" class="d-none">{value}</div>
#                         <div id="progress" class="d-none">{progress}</div>'''
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
#                         <div id="status" class="d-none">Completed</div>
#                         <div id="progress" class="d-none">{progress}</div>
#                         <div id="emails">{", ".join(valid_emails_found)}</div>'''
#                 }
#             else:
#                 post_update_payload = {
#                     "post_id": batch.get("post_id"),
#                     "post_content": f'''
#                         <div id="status" class="d-none">Processing</div>
#                         <div id="progress" class="d-none">{progress}</div>
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

frontend_update_semaphore = asyncio.Semaphore(10) 

@sync_to_async
def update_batch_task(batch_id):
    """Wrap the database query for batch task."""
    return BatchTask.objects.get(batch_id=batch_id)

@sync_to_async
def bulk_create_email_data(batch_results):
    """Wrap bulk_create to avoid database access in async context."""
    with transaction.atomic():
            # Prepare the list of EmailData objects to insert
            email_objs = [
                EmailData(email=email, status=status)
                for email, status in batch_results.items()
            ]
            
            # Perform bulk_create with conflict resolution
            EmailData.objects.bulk_create(
                email_objs,
                update_conflicts=['status', 'checked_at'],  # Fields to update in case of conflict
                unique_fields=['email']  # Unique field constraint (email)
            )


async def update_frontend_async(progress, batch_id):
    """Send progress updates asynchronously to the frontend with thread safety."""
    
    post_update_url = "https://insideemails.com/?wpwhpro_action=update-post-wp&wpwhpro_api_key=gukzza6i6qykirfkwc0maa59s28zxer0t6kn41uzi5j8k31cqmjkjjkqnhze6mbk&action=update_post"
    
    async with frontend_update_semaphore:  # Ensure only one update happens at a time
        try:
            
            batch = await update_batch_task(batch_id)  # Lock batch row for safe updates
            service_type = batch.service_type
            batch_results = batch.results
            batch_status = batch.status
            post_id = batch.post_id
            initial_count = batch.initial_count
            output_file_name = batch.output_file_name

            if service_type == 'email_verification_through_api':
                if progress == 100:
                    await bulk_create_email_data(batch_results)
                    df = pd.DataFrame(list(batch_results.items()), columns=["Email", "Status"])
                    media_path = os.path.join(settings.MEDIA_ROOT, output_file_name)
                    os.makedirs(settings.MEDIA_ROOT, exist_ok=True)
                    df.to_csv(media_path, index=False)
                return

            elif service_type == 'bulk_verify':
                if progress == 100:
                    print('Results:', batch_results.values())
                    await bulk_create_email_data(batch_results)
                    df = pd.DataFrame(list(batch_results.items()), columns=["Email", "Status"])

                    status_counts = df['Status'].value_counts()
                    
                    valid_count = status_counts.get("Valid", 0)
                    invalid_count = status_counts.get("Invalid", 0)
                    no_mx = status_counts.get("No MX Records Found", 0)
                    spamBlock_count = status_counts.get("Spam Block", 0)
                    risky_count = status_counts.get("Catch All", 0)
                    unknown_count = status_counts.get("Unknown", 0)
                    tempLimited_count = status_counts.get("Temporarily Limited", 0)

                    df.to_csv(output_file_name, index=False)
                    output_file_url = f'https://verifier.insideemails.com/media/{os.path.basename(output_file_name)}'

                    post_update_payload = {
                        "post_id": post_id,
                        "post_content": f'''<div id="status" class="d-none">{batch_status}</div>
                                            <div id="email_count" class="d-none">{initial_count}</div>
                                            <div id="progress" class="d-none">{progress}</div><br> 
                                            [email_verification_report output_file_url="{output_file_url}" 
                                            deliverable_count={valid_count} 
                                            undeliverable_count={invalid_count} 
                                            risky_count={risky_count} 
                                            unknown_count={unknown_count} 
                                            spamBlock_count={spamBlock_count} 
                                            tempLimited_count={tempLimited_count} 
                                            total_count={initial_count}]'''
                    }
                else:
                    post_update_payload = {
                        "post_id": post_id,
                        "post_content": f'''<div id="status" class="d-none">{batch_status}</div>
                                            <div id="email_count" class="d-none">{initial_count}</div>
                                            <div id="progress" class="d-none">{progress}</div>
                                            <br> <a href='#' class="d-none"></a>''',
                    }

            elif service_type == 'single_verify':
                if progress == 100:
                    print(f"Catch-all result: {batch_results}")
                    await bulk_create_email_data(batch_results)
                    value = next(iter(batch_results.values()), None)  # Prevent StopIteration
                    print(f'value : {value}')
                    post_update_payload = {
                        "post_id": post_id,
                        "post_content": f'''<div id="status" class="d-none">{value}</div>
                                            <div id="progress" class="d-none">{progress}</div>''',
                    }
                else:
                    return  # No update needed

            elif service_type == 'email_finder':
                from .views import wallet_action
                if progress == 100:
                    valid_emails_found = [email for email, status in batch_results.items() if status == "Valid"]
                    print(f"Valid emails found: {valid_emails_found}")
                    await bulk_create_email_data(batch_results)

                    wallet_action(
                        wp_user_id=batch.wpuser_id, 
                        action="debit", 
                        credit_count=len(valid_emails_found), 
                        file_name="Email Finder"
                    )

                    post_update_payload = {
                        "post_id": post_id,
                        "post_content": f'''<div id="status" class="d-none">Completed</div>
                                            <div id="progress" class="d-none">{progress}</div>
                                            <div id="emails">{", ".join(valid_emails_found)}</div>''',
                    }
                else:
                    post_update_payload = {
                        "post_id": post_id,
                        "post_content": f'''<div id="status" class="d-none">Processing</div>
                                            <div id="progress" class="d-none">{progress}</div>
                                            <div id="emails"></div>''',
                    }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(post_update_url, json=post_update_payload, timeout=5) as response:
                    if response.status != 200:
                        print(f"Frontend update failed: {response.status}")

            print(f"Frontend update successful for batch {batch_id}")

        except BatchTask.DoesNotExist:
            print(f"Batch {batch_id} not found.")
        except Exception as e:
            print(f"Error updating frontend: {e}")



def generate_random_triggers():
    """Generate random trigger points between 1 and 100, ensuring 100 is always included."""
    random_points = sorted(random.sample(range(1, 100), random.randint(20, 40)))  # Random 3-7 points
    random_points.append(100)  # Ensure 100% is always triggered
    return set(random_points)

# Initialize Redis connection
async def get_redis_connection():
    return await Redis.from_url("redis://localhost:6379", decode_responses=True)

async def fetch_mx_records_async(domain):
    """Fetch MX records asynchronously with Redis caching."""
    redis = await get_redis_connection()

    # ✅ Check Redis cache first
    cached_mx = await redis.get(f"mx:{domain}")
    if cached_mx:
        return json.loads(cached_mx)  # Return cached result

    resolver = aiodns.DNSResolver(timeout=10)
    try:
        answers = await resolver.query(domain, 'MX')
        mx_records = sorted([(record.priority, record.host) for record in answers], key=lambda x: x[0])
        mx_hosts = [record[1] for record in mx_records]

        # ✅ Store result in Redis with a TTL of 24 hours
        await redis.setex(f"mx:{domain}", 86400, json.dumps(mx_hosts))
        return mx_hosts
    except:
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
def verify_email_via_smtp(sender, recipient, proxy_host, proxy_port, proxy_user, proxy_password, batch_id,total_emails , round=1):
    recipient = recipient.lower()
    try:
        status = EmailData.objects.get(email=recipient).status
        if status == "Spam Block":
            raise ValueError("Spam Block")  # Will now trigger the except block
    except (EmailData.DoesNotExist, ValueError):  # Catches both "not found" and "spam block"
        domain = recipient.split('@')[1]
        try:
            NoMXDomains.objects.get(domain=domain)
            status = "No MX Records Found"
        except NoMXDomains.DoesNotExist:
            mx_records = fetch_mx_records(domain)
            if not mx_records:
                NoMXDomains.objects.get_or_create(domain=domain)
                status = "No MX Records Found"
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
                            status =  "Temporarily Limited"
                        else:
                            status = "Unknown"
                except socket.timeout:
                    status = "Unknown"
                except Exception as e:
                    status = "Error" if round == 1 else "Unknown"

        
    # Update batch task progress in database
    with transaction.atomic():
        batch_task = BatchTask.objects.get(batch_id=batch_id)
        batch_task.completed += 1
        batch_task.results[recipient] = status
        progress = int((batch_task.completed / total_emails) * 100)
        batch_task.save()

    # Trigger frontend updates at different progress intervals
    for part, start, end in [("part1", 0, 25), ("part2", 26, 50), ("part3", 51, 75), ("part4", 76, 99)]:
        if start <= progress <= end and getattr(batch_task, part):
            triggers = sorted(getattr(batch_task, part))
            part_size = len(triggers)
            trigger_intervals = [start + int(i * ((end - start) / part_size)) for i in range(1, part_size + 1)]
            
            while triggers and trigger_intervals and progress >= trigger_intervals[0]:
                trigger_point = min(triggers)
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(update_frontend_async(trigger_point, batch_id))
                loop.close()
                trigger_intervals.pop(0)
                
                with transaction.atomic():
                    batch_task = BatchTask.objects.get(batch_id=batch_id)
                    triggers_str = getattr(batch_task, part)
                    try:
                        # Convert the string back to a list
                        triggers = json.loads(triggers_str)
                        
                        # Remove the trigger_point if it's present
                        triggers.remove(trigger_point)
                        
                        # Set the updated list back to the 'part' field
                        setattr(batch_task, part, json.dumps(triggers))
                        batch_task.save()
                    except (ValueError, json.JSONDecodeError):
                        pass
    
    return {recipient: status}


@shared_task()
def verify_email_for_catchall(sender, recipient, proxy_host, proxy_port, proxy_user, proxy_password,batch_id, total_emails):
    recipient = recipient.lower()
    domain = recipient.split('@')[1]

    try:
        CatchAllDomains.objects.get(domain=domain)
        status = "Catch All"
    except CatchAllDomains.DoesNotExist:
        mx_records = fetch_mx_records(domain)
        smtp_server = mx_records[0]
        smtp_port = 25
        invalid_email = f"non8xistent1.@{domain}"
        
        try:
            with ProxySMTP(smtp_server, smtp_port, proxy_host=proxy_host, proxy_port=proxy_port, proxy_user=proxy_user, proxy_password=proxy_password, timeout=60) as server:
                server.ehlo()
                server.mail(sender)
                code, message = server.rcpt(invalid_email)
                if code in [250, 251, 252]:
                    with transaction.atomic():
                        CatchAllDomains.objects.get_or_create(domain=domain)
                    status = "Catch All"
                else:
                    status = "Valid"
        except:
            status = "Valid"

    # Update batch task progress in database
    with transaction.atomic():
        batch_task = BatchTask.objects.get(batch_id=batch_id)
        batch_task.completed += 1
        batch_task.results[recipient] = status
        progress = int((batch_task.completed / total_emails) * 100)
        batch_task.save()

    # Trigger frontend updates at different progress intervals
    for part, start, end in [("part1", 0, 25), ("part2", 26, 50), ("part3", 51, 75), ("part4", 76, 99)]:
        if start <= progress <= end and getattr(batch_task, part):
            triggers = sorted(getattr(batch_task, part))
            part_size = len(triggers)
            trigger_intervals = [start + int(i * ((end - start) / part_size)) for i in range(1, part_size + 1)]
            
            while triggers and trigger_intervals and progress >= trigger_intervals[0]:
                trigger_point = min(triggers)
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(update_frontend_async(trigger_point, batch_id))
                loop.close()
                trigger_intervals.pop(0)
                
                with transaction.atomic():
                    batch_task = BatchTask.objects.get(batch_id=batch_id)
                    triggers_str = getattr(batch_task, part)
                    try:
                        # Convert the string back to a list
                        triggers = json.loads(triggers_str)
                        # Remove the trigger_point if it's present
                        triggers.remove(trigger_point)
                        # Set the updated list back to the 'part' field
                        setattr(batch_task, part, json.dumps(triggers))
                        batch_task.save()
                    except (ValueError, json.JSONDecodeError):
                        pass

    return {recipient: status}




@shared_task
def process_first_round_results(first_round_results_list, batch_id, sender, proxy_host, proxy_port, proxy_user, proxy_password, second_quarter_parts, third_quarter_parts):
    """Processes first-round results and triggers spam-block retry."""
    with transaction.atomic():
        batch_task = BatchTask.objects.get(batch_id=batch_id)
        batch_task.completed += len(first_round_results_list)
        batch_task.save()
    
    first_round_results = {email: status for result in first_round_results_list for email, status in result.items()}
    spam_blocked_emails = [email for email, status in first_round_results.items() if status in ["Spam Block", "Error"]]
    
    if spam_blocked_emails:
        total_emails = len(spam_blocked_emails)
        with transaction.atomic():
            batch_task = BatchTask.objects.get(batch_id=batch_id)
            batch_task.total = total_emails
            batch_task.completed = 0
            batch_task.status = "processing"
            batch_task.part1, batch_task.part2, batch_task.part3, batch_task.part4 = second_quarter_parts
            batch_task.spam_block_retries = len(spam_blocked_emails)
            batch_task.save()
        
        retry_tasks = group(
            verify_email_via_smtp.s(sender, email, proxy_host, proxy_port, proxy_user, proxy_password, batch_id, total_emails, round=2) 
            for email in spam_blocked_emails
        )
        return chord(retry_tasks)(process_spam_block_results.s(first_round_results, batch_id, sender, proxy_host, proxy_port, proxy_user, proxy_password, third_quarter_parts))
    else:
        return process_spam_block_results([], first_round_results, batch_id, sender, proxy_host, proxy_port, proxy_user, proxy_password, third_quarter_parts)
    

@shared_task
def process_spam_block_results(retry_results_list , first_round_results , batch_id, sender, proxy_host, proxy_port, proxy_user, proxy_password, third_quarter_parts):
    """Merges spam-block retry results and triggers valid email processing."""
    
    # if isinstance(retry_results_list, list):  # More robust type check
    #     retry_results = {}
    #     for result in retry_results_list:
    #         retry_results.update(result)
    retry_results = {email: status for result in retry_results_list for email, status in result.items()}

  
    for email, new_status in retry_results.items():
        if email in first_round_results:  #  Only update existing keys
            first_round_results[email] = new_status 

    with transaction.atomic():
        batch_task = BatchTask.objects.get(batch_id=batch_id)
        batch_task.results = first_round_results  # Ensure results are saved
        batch_task.completed += len(retry_results_list)
        batch_task.save()

    # Identify Valid Emails
    valid_emails = [email for email, status in first_round_results.items() if status == "Valid"]
    total_emails = len(valid_emails)
    if valid_emails:
        with transaction.atomic():
            batch_task = BatchTask.objects.get(batch_id=batch_id)
            batch_task.total = total_emails
            batch_task.completed = 0
            batch_task.status = "processing"
            batch_task.part1, batch_task.part2, batch_task.part3, batch_task.part4 = third_quarter_parts
            batch_task.valid_email_retries = len(valid_emails)
            batch_task.save()

        catch_all_tasks = group(
            verify_email_for_catchall.s(sender, email, proxy_host, proxy_port, proxy_user, proxy_password, batch_id, total_emails) 
            for email in valid_emails
        )
        print(f" SB {first_round_results}")
        return chord(catch_all_tasks)(finalize_results.s(first_round_results, batch_id))
    else:
        return finalize_results([], first_round_results, batch_id)


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
def finalize_results(catch_all_results_list, first_round_results, batch_id):
    """Final step: Merges results and marks batch as complete."""
    
    print(f" FF {first_round_results}")
    print(f" CC {catch_all_results_list}")

    if isinstance(catch_all_results_list, list):  # More robust type check
        catch_all_results = {}
        for result in catch_all_results_list:
            catch_all_results.update(result)
    
    if catch_all_results:
        for email, new_status in catch_all_results.items():
            if email in first_round_results:  # Only update existing keys
                first_round_results[email] = new_status 

    print(f" C {catch_all_results}")

    try:
        with transaction.atomic():
            batch_task = BatchTask.objects.get(id=batch_id)
            batch_task.status = "completed"
            batch_task.results = first_round_results  # Ensure results are saved
            batch_task.save()

        # Ensure UI update
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(update_frontend_async(100, batch_id))
        loop.close()

        print(f"Final update triggered for batch: {batch_id}")

        return {"batch_id": batch_id, "results": first_round_results}

    except batch_task.DoesNotExist:
        print(f"Batch {batch_id} not found.")
        return {"error": "Batch not found"}

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

@shared_task
def verify_emails_in_parallel(sender, proxy_host, proxy_port, proxy_user, proxy_password, email_list, service_type, wpuser_id=0, post_id=0, output_file_name=None, batch_id=None):
    if not batch_id:
        batch_id = f"batch_{int(time.time())}"
    total_emails = len(email_list)
    random_triggers = generate_random_triggers()

    sorted_triggers = sorted(random_triggers)
    quarter_size = len(sorted_triggers) // 3

    first_quarter = sorted_triggers[:quarter_size]
    second_quarter = sorted_triggers[quarter_size:2 * quarter_size]
    third_quarter = sorted_triggers[2 * quarter_size:]

    # Step 2: Divide each quarter into 4 equal parts
    def split_into_parts(lst, num_parts=4):
        """Splits a list into num_parts roughly equal sublists."""
        k, m = divmod(len(lst), num_parts)
        return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(num_parts)]

    first_quarter_parts = split_into_parts(first_quarter)
    second_quarter_parts = split_into_parts(second_quarter)
    third_quarter_parts = split_into_parts(third_quarter)
    print(first_quarter)
    print(first_quarter_parts)

    try:
        # If batch_id exists, update existing record
        batch_task = BatchTask.objects.get(batch_id=batch_id)
        batch_task.total = total_emails
        batch_task.output_file_name = output_file_name or batch_task.output_file_name
        batch_task.initial_count = total_emails
        batch_task.status = "processing"
        batch_task.part1 = first_quarter_parts[0]
        batch_task.part2 = first_quarter_parts[1]
        batch_task.part3 = first_quarter_parts[2]
        batch_task.part4 = first_quarter_parts[3]
        batch_task.post_id = post_id
        batch_task.wpuser_id = wpuser_id
        batch_task.service_type = service_type
        batch_task.save()
    except ObjectDoesNotExist:
        # Create a new BatchTask entry if batch_id does not exist
        batch_task = BatchTask.objects.create(
            batch_id=batch_id,
            total=total_emails,
            output_file_name=output_file_name,
            initial_count=total_emails,
            completed=0,
            status="processing",
            part1=first_quarter_parts[0],
            part2=first_quarter_parts[1],
            part3=first_quarter_parts[2],
            part4=first_quarter_parts[3],
            post_id=post_id,
            wpuser_id=wpuser_id,
            service_type=service_type,
            results={},
        )
    # Initialize batch tracking
    # batch_tasks[batch_id] = {"total": total_emails, "results" : {} , "output_file_name": output_file_name, "initial_count":total_emails,   "completed": 0, "status": "processing", "part1": first_quarter_parts[0], "part2": first_quarter_parts[1], 
    #                          "part3": first_quarter_parts[2], "part4": first_quarter_parts[3], "post_id" : post_id, "wpuser_id" : wpuser_id, "service_type": service_type }
    print(email_list)
    verification_tasks = group(
        verify_email_via_smtp.s(sender, email, proxy_host, proxy_port, proxy_user, proxy_password, batch_id= batch_id, total_emails=total_emails, round=1) for email in email_list
    )


    chord(verification_tasks)(process_first_round_results.s(batch_id, sender, proxy_host, proxy_port, proxy_user, proxy_password, second_quarter_parts, third_quarter_parts))

    return


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





