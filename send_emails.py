import os
from datetime import datetime

from mailjet_rest import Client

from persistent_storage import get_supabase_client


def send_email_updates():
    supabase = get_supabase_client()
    users = supabase.table('users').select('id, email, name, send_emails').neq('send_emails', 'never').execute()

    current_time = datetime.now()

    for user in users.data:
        send_emails = user['send_emails']
        if current_time.hour < 17 and send_emails == 'daily':
            continue

        user_id = user['id']
        user_name = user['name']
        user_email = user['email']
        unemailed_jobs = (supabase.table('recent_high_score_jobs').select('id, user_id, title, company, score')
                          .eq('user_id', user_id).eq('email_sent', False)
                          .order('score', desc=True)
                          .execute())

        if not unemailed_jobs.data or len(unemailed_jobs.data) == 0:
            print(f"No new jobs for {user_name}, skipping email.")
            continue

        print(f"Found {len(unemailed_jobs.data)} new jobs for {user_name}")
        email_content = (f'Hi {user_name}!  Good news, we scoured the internet and found new jobs that we think are'
                         f' good matches for you!\n\n')
        for job in unemailed_jobs.data:
            score = job['score']
            job_title = job['title']
            job_company = job['company']
            email_content += f"- ({score}) {job_title} at {job_company}\n"

        email_content += "\nClick here to view the jobs: https://jobs.davehague.com"

        # Send email
        send_email(user_email, user_name, email_content)
        update_email_sent_status(unemailed_jobs)


def send_email(user_email, user_name, email_content):
    print(f"Sending email to {user_name} at {user_email}")
    api_key = os.environ.get('MJ_APIKEY_PUBLIC')
    api_secret = os.environ.get('MJ_APIKEY_PRIVATE')
    mailjet = Client(auth=(api_key, api_secret), version='v3.1')

    data = {
        'Messages': [
            {
                "From": {
                    "Email": "dave@davehague.com",
                    "Name": "Dave Hague"
                },
                "To": [
                    {
                        "Email": user_email,
                        "Name": user_name
                    }
                ],
                "Subject": "New jobs are in from the jobs app!",
                "TextPart": email_content
            }
        ]
    }

    result = mailjet.send.create(data=data)
    print(result.status_code)
    print(result.json())


def update_email_sent_status(unemailed_jobs):
    supabase = get_supabase_client()

    for job in unemailed_jobs.data:
        job_id = job['id']
        user_id = job['user_id']

        (supabase.table('users_jobs').update({'email_sent': True})
         .eq('user_id', user_id)
         .eq('job_id', job_id)
         .execute())
