import os
import re
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client, Client
from supabase.lib.client_options import ClientOptions
import pandas as pd
from datetime import datetime, timedelta

from llm import query_llm


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


def get_users_with_resume_and_login_last_30_days():
    supabase = get_supabase_client()
    response = (supabase.table('users')
                .select('*')
                .neq('resume', None)
                .neq('resume', '')
                .gte('last_login', (datetime.now() - timedelta(days=30)).isoformat())
                .execute())

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


def add_association_if_not_exists(user_id, job_id):
    supabase = get_supabase_client()
    user_has_recommendation = (supabase.table('users_jobs')
                               .select('*')
                               .eq('user_id', user_id)
                               .eq('job_id', job_id)
                               .execute())

    if user_has_recommendation.data:
        print(f"Job with URL {job_id} already exists for user {user_id}, skipping...")
        return None

    # (db_user, user_configs, job_title, job_description):
    ratings = get_job_rating_for_user(user_id, job_id)
    if int(ratings.get('overall_score', 0)) < 70:
        print(f"Job with URL {job_id} has a score less than 70, skipping...")
    else:
        print(
            f"Job with URL {job_id} has a score of {ratings.get('overall_score')}, adding association for user {user_id}...")

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


def get_job_rating_for_user(user_id, job_id):
    db_user = get_user_by_id(user_id)
    user_configs = get_user_configs(user_id)

    job = get_job_by_id(job_id)
    job_description = job.get('description')
    job_title = job.get('title')

    db_job_titles = [config['string_value'] for config in user_configs if config['key'] == 'job_titles']
    db_skill_words = [config['string_value'] for config in user_configs if config['key'] == 'skill_words']
    db_stop_words = [config['string_value'] for config in user_configs if config['key'] == 'stop_words']
    db_resume = db_user.get('resume')

    job_titles = db_job_titles or []
    skill_words = db_skill_words or []
    stop_words = db_stop_words or []

    resume = consolidate_text(db_resume)

    job_description = consolidate_text(job_description)
    full_message = f"<job_titles>{', '.join(job_titles)}</job_titles>\n" + \
                   f"<desired_words>{', '.join(skill_words)}</desired_words>\n" + \
                   f"<undesirable_words>{', '.join(stop_words)}</undesirable_words>\n" + \
                   f"<resume>{resume}</resume>\n" + \
                   f"<job_title>{job_title}</job_title>\n" + \
                   f"<job_description>{job_description}</job_description>\n" + \
                   """
                   Given the job titles (job_titles tag), desired words (desired_words tag), undesired words 
                   (undesirable_words tag), resume (resume tag), job title (job_title tag) and job description 
                   (job_description tag), make the following ratings:

                   1) How the candidate would rate this job on a scale from 1 to 100 in terms of how well it 
                   matches their experience and the type of job they desire.
                   2) How the candidate would rate this job on a scale from 1 to 100 as a match for their 
                   experience level (they aren't underqualified or overqualified).
                   3) How a hiring manager for this job would rate the candidate on a scale from 1 to 100 on how 
                   well the candidate meets the skill requirements for this job.
                   4) How a hiring manager for this job would rate the candidate on a scale from 1 to 100 on how 
                   well the candidate meets the experience requirements for this job.
                   5) Consider the results from steps 1 through 5 then give a final assessment from 1 to 100,
                   where 1 is very little chance of this being a good match for the candidate and hiring manager, 
                   and 100 being a perfect match where the candidate will have a great chance to succeed in 
                   this role.

                   For experience level, look for cues in the jobs description that list years of experience, 
                   then compare that to the level of experience you believe the candidate to have (make an 
                   assessment based on year in directly applicable fields of work).

                   Start your answer immediately with a bulleted list as shown in the example below. Always include 
                   the left side prefix from the template below in your answer (including for the explanation). 
                   Address the candidate directly, closely following the template set in the example. NN should be 
                   replaced in the template with a 2 digit number each time.
                   """
    full_message = consolidate_text(full_message)
    full_message += \
        """
        - Candidate desire match: NN
        - Candidate experience match: NN
        - Hiring manager skill match: NN
        - Hiring manager experience match: NN
        - Final overall match assessment: NN
        - Explanation of ratings: 
        You may <like, be lukewarm on, or dislike> this job because of the following reasons: <reasons in one sentence>. The hiring manager may think you would be a <good, reasonable, or bad> fit for this job because of <reasons, in one sentence>. Overall, I think <your overall thoughts about the match between the user and the job in one sentence>.
        """

    ratings = query_llm(llm="gemini",
                        model_name="gemini-1.5-flash",
                        system="You are a helpful no-nonsense assistant. You listen to directions carefully and follow them to the letter.",
                        messages=[{"role": "user", "content": full_message}])

    if ratings is None:
        print("LLM failed to generate ratings.")
        return None

    print(f"Ratings for job: {ratings}")

    guidance_split = ratings.split("Explanation of ratings:")
    if len(guidance_split) == 2:
        guidance = guidance_split[1].strip()
    else:
        guidance = ""

    ratings = ratings.split("\n")

    if len(ratings) >= 6:
        desire_score_split = ratings[0].split(":")
        if len(desire_score_split) == 2:
            desire_score = desire_score_split[1].strip()
        else:
            print("Error: Unable to split desire score.")

        experience_score_split = ratings[1].split(":")
        if len(experience_score_split) == 2:
            experience_score = experience_score_split[1].strip()
        else:
            print("Error: Unable to split experience score.")

        meets_requirements_score_split = ratings[2].split(":")
        if len(meets_requirements_score_split) == 2:
            meets_requirements_score = meets_requirements_score_split[1].strip()
        else:
            print("Error: Unable to split meets requirements score.")

        meets_experience_score_split = ratings[3].split(":")
        if len(meets_experience_score_split) == 2:
            meets_experience_score = meets_experience_score_split[1].strip()
        else:
            print("Error: Unable to split meets experience score.")

        overall_job_score_split = ratings[4].split(":")
        if len(overall_job_score_split) == 2:
            overall_job_score = overall_job_score_split[1].strip()
        else:
            print("Error: Unable to split overall job score.")
    else:
        print("Error: Ratings list does not have enough elements.")

    score_dict = {
        'desire_score': desire_score,
        'experience_score': experience_score,
        'meets_requirements_score': meets_requirements_score,
        'meets_experience_score': meets_experience_score,
        'overall_score': overall_job_score,
        'guidance': guidance
    }

    return score_dict


def consolidate_text(text):
    consolidated = text.replace('\r', ' ').replace('\n', ' ')
    consolidated = re.sub(' +', ' ', consolidated)
    return consolidated


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
