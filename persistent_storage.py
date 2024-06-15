import os
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client, Client
from supabase.lib.client_options import ClientOptions
import pandas as pd
from datetime import datetime, timedelta


def convert_to_int(value):
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def convert_to_date(value):
    try:
        if pd.isna(value) or value in ['NaT', '']:
            return None
        return pd.to_datetime(value).date().isoformat()
    except (ValueError, TypeError):
        return None


def get_supabase_client():
    # Load environment variables
    env_path = Path('.') / '.env'
    load_dotenv(dotenv_path=env_path)

    # Get Supabase URL and key from environment variables
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')

    # Initialize Supabase client
    opts = ClientOptions().replace(schema="jobscraper")
    supabase: Client = create_client(supabase_url, supabase_key, options=opts)

    return supabase


def get_user_configs(user_id):
    supabase = get_supabase_client()
    response = (supabase.table('user_configs')
                .select('*')
                .eq('user_id', user_id)
                .execute())

    if response.data:
        return response.data
    else:
        print(f"Error fetching configs or configs were empty")
        return {}


def get_users():
    supabase = get_supabase_client()
    response = (supabase.table('users')
                .select('*')
                .neq('resume', None)
                .neq('resume', '')
                .execute())

    if response.data:
        return response.data
    else:
        print(f"Error fetching roles: {response.get('error')}")
        return None


def get_recent_job_urls(days_old=5):
    supabase = get_supabase_client()
    response = (supabase.table('jobs')
                .select('url, date_posted, date_pulled')
                .execute())

    if response.data or response.data == []:
        cutoff_date = datetime.now() - timedelta(days=days_old)
        urls = []
        for item in response.data:
            date_posted = item.get('date_posted')
            date_pulled = item.get('date_pulled')
            if date_posted:
                job_date = pd.to_datetime(date_posted)
            else:
                job_date = pd.to_datetime(date_pulled)

            if job_date and job_date > cutoff_date:
                urls.append(item['url'])

        return urls
    else:
        print(f"Error fetching job URLs: {response.error}")
        return None


def save_jobs_to_supabase(user_id, df):
    print(f"Saving {len(df)} jobs to Supabase...")
    # Load environment variables
    env_path = Path('.') / '.env'
    load_dotenv(dotenv_path=env_path)

    # Get Supabase URL and key from environment variables
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')

    # Initialize Supabase client
    opts = ClientOptions().replace(schema="jobscraper")
    supabase: Client = create_client(supabase_url, supabase_key, options=opts)

    for index, row in df.iterrows():
        try:
            job_score = int(row.get('job_score'))
        except ValueError:
            print("job_score cannot be converted to an integer")
            continue

        if job_score <= 70:
            continue

        url_exists = supabase.table('jobs').select('id').eq('url', row['job_url']).execute()
        if url_exists.data:
            print(f"Job with URL {row['job_url']} already exists, skipping...")
            continue

        new_job = {
            'title': row.get('title'),
            'company': row.get('company'),
            'short_summary': row.get('short_summary'),
            'hard_requirements': row.get('hard_requirements'),
            'job_site': row.get('site'),
            'url': row.get('job_url'),
            'location': None if pd.isna(row.get('location')) else row.get('location'),
            'date_posted': convert_to_date(row.get('date_posted')),
            'comp_interval': None if pd.isna(row.get('interval')) else row.get('interval'),
            'comp_min': convert_to_int(row.get('min_amount')),
            'comp_max': convert_to_int(row.get('max_amount')),
            'comp_currency': None if pd.isna(row.get('currency')) else row.get('currency'),
            'emails': None if pd.isna(row.get('emails')) else row.get('emails'),
            'description': row.get('description'),
            'date_pulled': datetime.now().isoformat(),
            'searched_title': row.get('searched_title')
        }

        print(new_job)
        try:
            result = supabase.table('jobs').insert(new_job).execute()

            if result.data:
                print(f"Inserted job: {result.data}")
            else:
                print(f"Error inserting job: {result.error}")
                continue;

            users_jobs_row = {
                'user_id': user_id,
                'job_id': result.data[0].get('id'),
                'desire_score': row.get('desire_score'),
                'experience_score': row.get('experience_score'),
                'meets_requirements_score': row.get('meets_requirements_score'),
                'meets_experience_score': row.get('meets_experience_score'),
                'score': row.get('job_score'),
                'guidance': row.get('guidance')
            }

            association_result = supabase.table('users_jobs').insert(users_jobs_row).execute()
            if association_result.data:
                print(f"Inserted user job association!")  #: {association_result.data}")
            else:
                print(f"Error inserting user job: {association_result.error}")
        except Exception as e:
            print(f"Error inserting job: {e}")
            print(f"Error on job data: {new_job}")
            print(f"Error on user-job data: {users_jobs_row}")
            continue
