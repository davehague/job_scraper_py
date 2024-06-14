import os
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
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
        unemailed_jobs = (supabase.table('recent_high_score_jobs')
                          .select('id, user_id, score, title, company', 'location', 'comp_min', 'comp_max', 'guidance')
                          .eq('user_id', user_id).eq('email_sent', False)
                          .order('score', desc=True)
                          .execute())

        if not unemailed_jobs.data or len(unemailed_jobs.data) == 0:
            print(f"No new jobs for {user_name}, skipping email.")
            continue

        print(f"Found {len(unemailed_jobs.data)} new jobs for {user_name}")

        email_jobs_data = []
        for job in unemailed_jobs.data[:3]:
            score = int(job['score'])
            score_color = '#59c9a5' if score > 85 else '#93c1b2' if score > 75 else '#888'
            title = job['title']
            company = job['company']
            location = job['location']

            comp_min = job['comp_min']
            comp_max = job['comp_max']
            salary = f"${comp_min:,} - ${comp_max:,}" if comp_min and comp_max else "Not provided"

            guidance = job['guidance']
            email_jobs_data.append({
                'score': score,
                'score_color': score_color,
                'title': title,
                'company': company,
                'location': location,
                'salary': salary,
                'summary': guidance
            })

        print(email_jobs_data)

        # Send email
        send_email(user_email, user_name, email_jobs_data, total_job_count=len(unemailed_jobs.data))
        update_email_sent_status(unemailed_jobs)


def send_email(user_email, user_name, jobs, total_job_count):
    print(f"Sending email to {user_name} at {user_email}")
    api_key = os.environ.get('MJ_APIKEY_PUBLIC')
    api_secret = os.environ.get('MJ_APIKEY_PRIVATE')
    mailjet = Client(auth=(api_key, api_secret), version='v3.1')

    file_loader = FileSystemLoader('email_templates')
    env = Environment(loader=file_loader)
    template = env.get_template('jobs_update.html')
    rendered_html = template.render(user_name=user_name, jobs=jobs, total_job_count=total_job_count)

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
                "TextPart": "New jobs are in from the jobs app! Check them out at https://jobs.davehague.com/",
                "HTMLPart": rendered_html
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
