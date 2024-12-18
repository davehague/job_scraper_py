import os
import time
import re

from datetime import datetime

from supabase import create_client, Client
from supabase.lib.client_options import ClientOptions

import pandas as pd
import anthropic
from openai import OpenAI
import google.generativeai as gemini

from jobspy import scrape_jobs  # python-jobspy package
# import numpy as np
# from sklearn.feature_extraction.text import TfidfVectorizer
# from sklearn.metrics.pairwise import cosine_similarity

from flask import Response, abort
import google.cloud.logging
import logging


def jobs_app_scheduled(event, context):
    logging.info(event)
    logging.info(context)
    return "Hello world!"


def jobs_app_function(context):
    api_key = context.headers.get('X-API-Key')
    expected_api_key = os.environ.get('GOOGLE_CLOUD_FUNCTION_API_KEY')

    if api_key != expected_api_key:
        return abort(403, description="Invalid Google Functions API Key")

    if context.method == 'POST' and 'X-CloudScheduler' in context.headers:
        jobs_app_scheduled(context.get_json(), context.context)
        return 'Scheduled job executed successfully', 200

    def query_llm(llm, model_name, system, messages=[]):
        max_retries = 3
        wait_time = 3

        for attempt in range(max_retries):
            try:
                if llm == "openai":
                    messages.insert(0, {"role": "system", "content": system})
                    client = OpenAI(
                        api_key=os.environ.get("OPENAI_API_KEY",
                                               'Specified environment variable OPENAI_API_KEY is not set.'),
                    )
                    completion = client.chat.completions.create(
                        messages=messages,
                        max_tokens=256,
                        model=model_name,
                        temperature=1.0
                    )
                    return completion.choices[0].message.content
                elif llm == "anthropic":
                    anthropic_api_key = os.environ.get("ANTHROPIC_API_KEY",
                                                       'Specified environment variable ANTHROPIC_API_KEY is not set.')
                    client = anthropic.Anthropic(api_key=anthropic_api_key)
                    message = client.messages.create(
                        model=model_name,
                        max_tokens=256,
                        temperature=1.0,
                        system=system,
                        messages=messages
                    )
                    return message.content[0].text
                elif llm == "gemini":
                    safe = [
                        {
                            "category": "HARM_CATEGORY_HARASSMENT",
                            "threshold": "BLOCK_NONE",
                        },
                        {
                            "category": "HARM_CATEGORY_HATE_SPEECH",
                            "threshold": "BLOCK_NONE",
                        },
                        {
                            "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                            "threshold": "BLOCK_NONE",
                        },
                        {
                            "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                            "threshold": "BLOCK_NONE",
                        }
                    ]

                    gemini.configure(api_key=os.environ.get("GEMINI_API_KEY",
                                                            'Specified environment variable GEMINI_API_KEY is not set.'))
                    model = gemini.GenerativeModel(model_name=model_name, safety_settings=safe)  # 'gemini-1.5-flash'
                    response = model.generate_content(system + " " + " ".join([msg["content"] for msg in messages]))
                    return response.text
                else:
                    return None

            except Exception as e:
                logging.error(
                    f"An unexpected error occurred: {e}. Attempt {attempt + 1} of {max_retries}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                wait_time *= 2  # Exponential backoff
                if attempt == max_retries - 1:
                    logging.error(f"Failed after {max_retries} attempts.")
                    return None

        return None

    def get_jobs_for_user(job_site, user_id, job_titles):
        scraped_data = pd.DataFrame()
        try:
            logging.info(f"Searching for job titles: {','.join(job_titles)} on {job_site}...")
            is_remote = True
            results_wanted = 3
            scraped_data = scrape_job_data(
                user_id,
                job_titles,
                job_sites=[job_site],
                location='USA',
                hours_old=24,
                results_wanted=20,
                distance=20,
                is_remote=is_remote)
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            return pd.DataFrame()

        # Filter out jobs where is_remote is True or is_remote is not specified
        if is_remote == "ONLY":
            scraped_data = scraped_data[scraped_data['is_remote'] != False]
            scraped_data = scraped_data[
                scraped_data['is_remote'] != True & scraped_data['description'].str.contains("remote", case=False,
                                                                                             na=False)]

        return scraped_data

    def scrape_job_data(user_id, job_titles, job_sites, location, distance, results_wanted, hours_old, is_remote):
        all_jobs = pd.DataFrame()
        for job_title in job_titles:
            job_df = get_jobs_with_backoff(user_id, job_title, job_sites, location, distance, results_wanted, hours_old,
                                           is_remote)

            if job_df is None:  # Something happened with pulling the jobs (e.g. max retries reached)
                continue

            if not job_df.empty:
                all_jobs = pd.concat([all_jobs, job_df], ignore_index=True)

        return all_jobs

    def get_jobs_with_backoff(user_id, job_title, job_sites, location, distance, results_wanted, hours_old, is_remote,
                              max_retries=5, initial_wait=5):
        attempt = 0
        wait_time = initial_wait

        while attempt < max_retries:
            try:
                jobs_df = scrape_jobs(
                    site_name=job_sites,
                    location=location,
                    distance=distance,
                    is_remote=is_remote,
                    job_type="fulltime",
                    linkedin_fetch_description=True,
                    search_term=job_title,
                    results_wanted=results_wanted,
                    hours_old=hours_old,  # (only Linkedin/Indeed is hour specific, others round up to days old)
                    country_indeed='USA'  # only needed for indeed / glassdoor
                )

                if jobs_df is None:
                    raise ValueError("scrape_jobs returned None dataframe")

                jobs_df['searched_title'] = job_title  # Add a column to indicate the job title
                jobs_df['user_id'] = user_id  # Add a column to indicate the ID
                jobs_df = jobs_df.dropna(axis=1, how='all') if not jobs_df.empty else pd.DataFrame()
                jobs_df = jobs_df.fillna("").infer_objects(copy=False)

                return jobs_df

            except Exception as e:
                logging.error(f"An error occurred: {e}")
                logging.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                wait_time *= 2  # Exponential backoff
                attempt += 1

        logging.info("Max retries reached, moving on to the next job title.")
        return None

    def clean_and_deduplicate_jobs(all_jobs, similarity_threshold=0.9):
        if all_jobs.empty:
            logging.info("No jobs found.")
            return all_jobs
        else:
            logging.info(f"Cleaning {len(all_jobs)} jobs")

        all_jobs_cols_removed = remove_extraneous_columns(all_jobs)

        long_desc_jobs = all_jobs_cols_removed[all_jobs_cols_removed['description'].str.len() >= 1000]
        logging.info(f"Removed jobs with short descriptions, now we have {len(long_desc_jobs)} jobs")

        deduped_by_url = remove_duplicates_by_url(long_desc_jobs, 'job_url')
        logging.info(f"Removed duplicates by URL, now we have {len(deduped_by_url)} jobs")

        # unsimilar = remove_duplicates_by_similarity(deduped_by_url, similarity_threshold)
        # logging.info(f"Removed duplicates by similarity, now we have {len(unsimilar)} jobs")

        return deduped_by_url

    def remove_extraneous_columns(df):
        columns_to_keep = ['site', 'job_url', 'job_url_direct', 'title', 'company', 'location', 'job_type',
                           'date_posted', 'interval', 'min_amount', 'max_amount', 'currency', 'is_remote',
                           'emails', 'description', 'searched_title', 'user_id']
        columns_to_drop = [col for col in df.columns if col not in columns_to_keep]
        return df.drop(columns=columns_to_drop)

    def remove_duplicates_by_url(df, column_name='job_url'):
        if df.empty:
            logging.info("DataFrame is empty. No duplicates to remove.")
            return df
        else:
            # Keep the first occurrence of each unique value in the specified column
            return df.drop_duplicates(subset=[column_name], keep='first')

    # def remove_duplicates_by_similarity(df, similarity_threshold=0.9):
    #     if df.empty:
    #         logging.info("DataFrame is empty. Nothing to de-duplicate.")
    #         return df

    #     df = df.fillna("")
    #     combined_text = df['title'] + " " + df['company'] + " " + df['description']

    #     # Use TF-IDF to vectorize the combined text
    #     vectorizer = TfidfVectorizer().fit_transform(combined_text)
    #     # Compute cosine similarity matrix
    #     cosine_sim = cosine_similarity(vectorizer, vectorizer)
    #     # Find indices to drop (where similarity is above the threshold, excluding self-comparison)
    #     to_drop = np.where(
    #         (cosine_sim > similarity_threshold).astype(int) &
    #         (np.ones_like(cosine_sim) - np.eye(len(cosine_sim), dtype=bool)).astype(int)
    #     )
    #     # Unique job indices to keep (inverting the logic to keep the first occurrence and remove subsequent similar ones)
    #     indices_to_keep = np.setdiff1d(np.arange(len(df)), np.unique(to_drop[0]))

    #     return df.iloc[indices_to_keep]

    def get_jobs_with_derived(jobs_df, user_info):
        derived_data_questions = [('short_summary',
                                   'Provide a short summary of the job.  If the job is fully remote, start with'
                                   ' the sentence "Fully remote! ", otherwise skip this step.  Then, after a'
                                   ' newline, include a single sentence related to the compensation.'
                                   ' Start this sentence with the words "Pay for this role is "'
                                   ' OR simply state "Pay was not specified. "'
                                   ' Next have a newline, then a single'
                                   ' sentence with the minimum number of years experience.  Include the type of'
                                   ' experience being looked for. Next have a newline, followed by key job'
                                   ' responsibilities (no more than 3 sentences).  Finally, have a newline and'
                                   ' follow with job benefits (no more than 3 sentences)'
                                   ),
                                  ('hard_requirements',
                                   'Summarize the hard requirements, things the candidate "must have" from the'
                                   ' description.  Start the list with the number of years experience,'
                                   ' if specified.  Limit this list to 4 bullet points of no more than 1 sentence'
                                   ' each')
                                  ]

        rated_jobs = get_job_ratings(jobs_df, user_info)
        todays_jobs = add_derived_data(rated_jobs, derived_data_questions, user_provided_info=user_info)

        return todays_jobs

    def add_derived_data(jobs_df, derived_data_questions=[], user_provided_info=None):
        if len(derived_data_questions) == 0:
            return jobs_df

        logging.info("Generating derived data...")
        derived_data = pd.DataFrame(index=jobs_df.index)

        for index, row in jobs_df.iterrows():
            job_description = f"Title: {row.get('title', 'N/A')}\nCompany: {row.get('company', 'N/A')}\nLocation: {row.get('location', 'N/A')}\n" \
                              f"Description: {row.get('description', 'N/A')}\n"

            pay_info = (
                f"Pays between {row.get('min_amount', 'N/A')} and {row.get('max_amount', 'N/A')} on a(n) {row.get('interval', 'N/A')}'"
                f" basis.") if row.get('interval', '') else ""

            job_description += pay_info
            logging.info(f"{index}: Processing: {row.get('title', 'N/A')} at {row.get('company', 'N/A')}")

            for column_name, question in derived_data_questions:
                full_message = build_context_for_llm(job_description, user_provided_info, question)
                full_message = consolidate_text(full_message)

                answer = query_llm(llm='openai', model_name='gpt-4o-mini',
                                   system="You are a helpful no-nonsense assistant. You listen to directions carefully and follow" \
                                          " them to the letter. Only return plain text, not markdown or HTML.",
                                   messages=[{"role": "user", "content": full_message}])

                derived_data.at[index, column_name] = answer

        jobs_df_updated = pd.concat([derived_data, jobs_df], axis=1)
        return jobs_df_updated

    def build_context_for_llm(job_description, user_provided_info, question):
        full_message = ""
        if user_provided_info is not None:
            full_message += "Here is the candidate's information: <user_provided_info>" + user_provided_info + "</user_provided_info> "
        if job_description:
            full_message += "Here is some information about a job. <job>" + job_description + "</job> "
        full_message += "Now for my question: " + question + " "
        return full_message

    def get_job_ratings(original_df, user_provided_info):
        jobs_df = original_df.copy()
        logging.info(f'Getting job ratings for {len(jobs_df)} jobs...')

        user_info = consolidate_text(user_provided_info)

        for index, row in jobs_df.iterrows():
            job_title = row.get('title', 'N/A')
            job_description = row.get('description', 'N/A')
            job_description = consolidate_text(job_description)

            full_message = f"<user_info>{user_info}</user_info>\n" + \
                           f"<job_title>{job_title}</job_title>\n" + \
                           f"<job_description>{job_description}</job_description>\n" + \
                           """
                           Given the user provided information (user_info tag), job title (job_title tag) and job description 
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

            ratings = query_llm(llm="openai",
                                model_name="gpt-4o-mini",
                                system="You are a helpful no-nonsense assistant. You listen to directions carefully and follow them to the letter.",
                                messages=[{"role": "user", "content": full_message}])

            if ratings is None:
                logging.info("LLM failed to generate ratings.")
                continue

            logging.info(f"Ratings for job {index}: {ratings}")
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
                    jobs_df.at[index, 'desire_score'] = desire_score
                else:
                    logging.error("Error: Unable to split desire score.")

                experience_score_split = ratings[1].split(":")
                if len(experience_score_split) == 2:
                    experience_score = experience_score_split[1].strip()
                    jobs_df.at[index, 'experience_score'] = experience_score
                else:
                    logging.error("Error: Unable to split experience score.")

                meets_requirements_score_split = ratings[2].split(":")
                if len(meets_requirements_score_split) == 2:
                    meets_requirements_score = meets_requirements_score_split[1].strip()
                    jobs_df.at[index, 'meets_requirements_score'] = meets_requirements_score
                else:
                    logging.error("Error: Unable to split meets requirements score.")

                meets_experience_score_split = ratings[3].split(":")
                if len(meets_experience_score_split) == 2:
                    meets_experience_score = meets_experience_score_split[1].strip()
                    jobs_df.at[index, 'meets_experience_score'] = meets_experience_score
                else:
                    logging.error("Error: Unable to split meets experience score.")

                overall_job_score_split = ratings[4].split(":")
                if len(overall_job_score_split) == 2:
                    overall_job_score = overall_job_score_split[1].strip()
                    logging.info(
                        f"{index}: Adding a rating to: {row.get('title', 'N/A')} at {row.get('company', 'N/A')}: {overall_job_score}")
                    jobs_df.at[index, 'job_score'] = overall_job_score
                else:
                    logging.error("Error: Unable to split overall job score.")

                if len(guidance) > 0:
                    logging.info(
                        f"{index}: Adding guidance to: {row.get('title', 'N/A')} at {row.get('company', 'N/A')}: {guidance}")
                    jobs_df.at[index, 'guidance'] = guidance
            else:
                logging.error("Error: Ratings list does not have enough elements.")

        jobs_over_50 = jobs_df[jobs_df['job_score'].astype(float) > 50]

        logging.info('Found jobs with scores over 50: ' + str(len(jobs_over_50)))
        return jobs_over_50

    def consolidate_text(text):
        consolidated = text.replace('\r', ' ').replace('\n', ' ')
        consolidated = re.sub(' +', ' ', consolidated)
        return consolidated

    def save_jobs_to_supabase(user_id, df):
        logging.info(f"Saving {len(df)} jobs to Supabase...")

        supabase_url = os.environ.get('SUPABASE_URL', 'Specified environment variable SUPABASE_URL is not set.')
        supabase_key = os.environ.get('SUPABASE_KEY', 'Specified environment variable SUPABASE_KEY is not set.')

        opts = ClientOptions().replace(schema="jobscraper")
        supabase: Client = create_client(supabase_url, supabase_key, options=opts)

        for index, row in df.iterrows():
            try:
                job_score = int(row.get('job_score'))
            except ValueError:
                logging.info("job_score cannot be converted to an integer")
                continue

            if job_score < 50:
                continue

            job_exists = supabase.table('jobs').select('id').eq('url', row.get('job_url', '')).execute()
            if not job_exists.data:
                logging.info(f"Job with URL {row.get('job_url', 'N/A')} does not exist, creating new job...")
                result = create_new_job(supabase, row)
                job_id = result.data[0].get('id')
                if not result.data:
                    logging.info(f"Error inserting job: {result.error}")
                    continue
            else:
                job_id = job_exists.data[0].get('id')

            user_has_recommendation = (supabase.table('recent_high_score_jobs')
                                       .select('id')
                                       .eq('user_id', user_id)
                                       .eq('url', row.get('job_url', ''))
                                       .execute())

            if user_has_recommendation.data:
                logging.info(f"Job with URL {row.get('job_url', 'N/A')} already exists for user {user_id}, skipping...")
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

    # ================ Main starts ================
    client = google.cloud.logging.Client(project="project")
    client.setup_logging()

    data = context.get_json()

    user_provided_info = data.get('resume')
    user_id = data.get('user_id')

    # Find the data withing user_provided_info that's between <desired_job_titles> and </desired_job_titles>
    job_titles_csv = re.findall(r'<desired_job_titles>(.*?)</desired_job_titles>', user_provided_info, re.DOTALL)
    job_titles = [title.strip() for title in job_titles_csv[0].split(',')] if job_titles_csv else []

    if not job_titles:
        logging.info("No job titles found within user info")
        return Response(response="No job titles found within user info", status=200)
    else:
        logging.info(f"Job titles found within user info: {job_titles}")

    all_jobs = []
    job_sites = ['indeed', 'glassdoor', 'zip_recruiter', 'linkedin', 'google']
    for job_site in job_sites:
        all_jobs = get_jobs_for_user(job_site, user_id, job_titles)
        if len(all_jobs) > 0:
            break

    cleaned_jobs = clean_and_deduplicate_jobs(all_jobs, similarity_threshold=0.9)

    if len(cleaned_jobs) == 0:
        return Response(response="No jobs found after cleaning and deduplicating", status=200)

    # Just keep the top 10 jobs
    cleaned_jobs_subset = cleaned_jobs.head(10)
    # have a second df for the second 10 jobs
    cleaned_jobs_second = cleaned_jobs[~cleaned_jobs['job_url'].isin(cleaned_jobs_subset['job_url'])].head(10)

    jobs_with_derived = get_jobs_with_derived(cleaned_jobs_subset, user_provided_info)
    save_jobs_to_supabase(user_id, jobs_with_derived)

    # If jobs_with_derived doesn't have a job over 70, get the next 10 jobs
    if len(jobs_with_derived[jobs_with_derived['job_score'].astype(float) > 70]) == 0:
        logging.info("No jobs over 70 found, getting the next 10 jobs...")
        jobs_with_derived_second = get_jobs_with_derived(cleaned_jobs_second, user_provided_info)
        save_jobs_to_supabase(user_id, jobs_with_derived_second)

    return Response(response="Jobs pulled, cleaned, and saved to Supabase", status=200)

# For running locally
# if __name__ == '__main__':
#     main('event', 'context')
