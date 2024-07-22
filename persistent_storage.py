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


def save_titles_for_user(user_id, titles):
    supabase = get_supabase_client()

    for title in titles:
        response = (supabase.table('user_configs')
                    .insert({'user_id': user_id, 'key': 'job_titles', 'string_value': title})
                    .execute())

    if response.data:
        print(f"Inserted titles for user {user_id}: {titles}")
    else:
        print(f"Error inserting titles for user {user_id}: {response.error}")
        return None


def get_user_by_id(user_id):
    supabase = get_supabase_client()
    response = (supabase.table('users')
                .select('*')
                .eq('id', user_id)
                .execute())

    if response.data:
        return response.data[0]
    else:
        print(f"Error fetching user: {response.error}")
        return None


def get_job_by_id(job_id):
    supabase = get_supabase_client()
    response = (supabase.table('jobs')
                .select('*')
                .eq('id', job_id)
                .execute())

    if response.data:
        return response.data[0]
    else:
        print(f"Error fetching job: {response.error}")
        return None


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


def get_active_users_with_resume():
    supabase = get_supabase_client()
    response = supabase.rpc('get_active_users_with_resume').execute()

    # Check the response
    if response.data:
        return response.data
    else:
        print(f"Error fetching roles: {response.get('error')}")
        return None


def get_recent_jobs(days_old=7):
    supabase = get_supabase_client()
    response = (supabase.table('jobs')
                .select('id, title, date_posted, date_pulled')
                .execute())

    if response.data:
        cutoff_date = datetime.now() - timedelta(days=days_old)
        jobs = [(item['id'], item['title']) for item in response.data if
                pd.to_datetime(item.get('date_posted') or item.get('date_pulled')) > cutoff_date]
        return jobs

    else:
        print(f"Error fetching jobs: {response.error}")
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

        if job_score < 50:
            continue

        job_exists = supabase.table('jobs').select('id').eq('url', row.get('job_url', 'N/A')).execute()
        if not job_exists.data:
            print(f"Job with URL {row.get('job_url', 'N/A')} does not exist, creating new job...")
            result = create_new_job(supabase, row)
            job_id = result.data[0].get('id')
            if not result.data:
                print(f"Error inserting job: {result.error}")
                continue
        else:
            job_id = job_exists.data[0].get('id')

        user_has_recommendation = (supabase.table('recent_high_score_jobs')
                                   .select('id')
                                   .eq('user_id', user_id)
                                   .eq('url', row.get('job_url', ''))
                                   .execute())

        if user_has_recommendation.data:
            print(f"Job with URL {row.get('job_url', 'N/A')} already exists for user {user_id}, skipping...")
            continue

        create_new_job_association(supabase, user_id, job_id, row)


def create_new_job(supabase, row):
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

    try:
        result = supabase.table('jobs').insert(new_job).execute()
        if result.data:
            print(f"Inserted job!")  # {result.data}")
        else:
            print(f"Error inserting job: {result.error}")
    except Exception as e:
        print(f"Error inserting job: {e}")
        print(f"Error on job data: {new_job}")
        return None

    return result


def user_has_recommendation(user_id, job_id):
    supabase = get_supabase_client()
    has_rec = (supabase.table('users_jobs')
               .select('*')
               .eq('user_id', user_id)
               .eq('job_id', job_id)
               .execute())

    if has_rec.data:
        return True
    else:
        return False


def add_user_job_association(user_id, job_id, ratings):
    supabase = get_supabase_client()
    users_jobs_row = {
        'user_id': user_id,
        'job_id': job_id,
        'desire_score': ratings.get('desire_score'),
        'experience_score': ratings.get('experience_score'),
        'meets_requirements_score': ratings.get('meets_requirements_score'),
        'meets_experience_score': ratings.get('meets_experience_score'),
        'score': ratings.get('overall_score'),
        'guidance': ratings.get('guidance')
    }
    try:
        association_result = supabase.table('users_jobs').insert(users_jobs_row).execute()
        if association_result.data:
            print(f"Inserted user job association!")  #: {association_result.data}")
        else:
            print(f"Error inserting user job: {association_result.error}")
    except Exception as e:
        print(f"Error inserting user job: {e}")
        print(f"Error on user-job data: {users_jobs_row}")
        return None

    return association_result


def create_new_job_association(supabase, user_id, job_id, row):
    users_jobs_row = {
        'user_id': user_id,
        'job_id': job_id,
        'desire_score': row.get('desire_score'),
        'experience_score': row.get('experience_score'),
        'meets_requirements_score': row.get('meets_requirements_score'),
        'meets_experience_score': row.get('meets_experience_score'),
        'score': row.get('job_score'),
        'guidance': row.get('guidance')
    }
    try:
        association_result = supabase.table('users_jobs').insert(users_jobs_row).execute()
        if association_result.data:
            print(f"Inserted user job association!")  #: {association_result.data}")
        else:
            print(f"Error inserting user job: {association_result.error}")
    except Exception as e:
        print(f"Error inserting user job: {e}")
        print(f"Error on user-job data: {users_jobs_row}")
        return None

    return association_result
