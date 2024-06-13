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

from flask import Response
import google.cloud.logging
import logging

def jobs_app_scheduled(event, context):
    print(event)
    print(context)
    return "Hello world!"


def jobs_app_function(context):
    if context.method == 'POST' and 'X-CloudScheduler' in context.headers:
        jobs_app_scheduled(context.get_json(), context.context)
        return 'Scheduled job executed successfully', 200

    def find_best_job_titles(resume):
        
        full_message = "In the <resume> tag below is a candidate resume:"
        full_message += "\n<resume>\n" + resume + "\n</resume>\n"
        full_message += """
You are an expert in searching job postings. You take all the information 
given to you and come up with a list of 3 most relevant job titles. Only list the 
titles in a comma-separated list, no other information is needed.  
IMPORTANT: ONLY INCLUDE THE JOB TITLES IN A COMMA SEPARATED LIST.  DO NOT INCLUDE ANY OTHER INFORMATION.
"""

        titles = query_llm(llm="anthropic",
                           model="claude-3-opus-20240229",
                           system="You are a helpful no-nonsense assistant.  You listen to directions carefully and follow them to the letter.",
                           messages=[{"role": "user", "content": full_message}])

        titles = [title.strip() for title in titles.split(",")] if titles else []
        return titles

    def query_llm(llm, model, system, messages=[]):
        max_retries = 3
        wait_time = 3

        for attempt in range(max_retries):
            try:
                if llm == "openai":
                    # add the system message to the messages
                    messages.insert(0, {"role": "system", "content": system})
                    client = OpenAI(
                        api_key=os.environ.get("OPENAI_API_KEY", 'Specified environment variable OPENAI_API_KEY is not set.'),
                    )
                    completion = client.chat.completions.create(
                        messages=messages,
                        max_tokens=256,
                        model=model,
                        temperature=1.0
                    )
                    return completion.choices[0].message.content
                elif llm == "anthropic":
                    anthropic_api_key = os.environ.get("ANTHROPIC_API_KEY", 'Specified environment variable ANTHROPIC_API_KEY is not set.')
                    client = anthropic.Anthropic(api_key=anthropic_api_key)
                    message = client.messages.create(
                        model=model,
                        max_tokens=256,
                        temperature=1.0,
                        system=system,
                        messages=messages
                    )
                    return message.content[0].text
                elif llm == "gemini":
                    gemini.configure(api_key=os.environ.get("GEMINI_API_KEY", 'Specified environment variable GEMINI_API_KEY is not set.'))
                    model = gemini.GenerativeModel(model)  # 'gemini-1.5-flash'
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
            print(f"Searching for job titles: {','.join(job_titles)} on {job_site}...")
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
            scraped_data = scraped_data[scraped_data['is_remote'] != True & scraped_data['description'].str.contains("remote", case=False, na=False)]
        
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
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                wait_time *= 2  # Exponential backoff
                attempt += 1

        print("Max retries reached, moving on to the next job title.")
        return None

    def clean_and_deduplicate_jobs(all_jobs, similarity_threshold=0.9):
        if all_jobs.empty:
            print("No jobs found.")
            return all_jobs
        else:
            print(f"Cleaning {len(all_jobs)} jobs")

        all_jobs_cols_removed = remove_extraneous_columns(all_jobs)

        long_desc_jobs = all_jobs_cols_removed[all_jobs_cols_removed['description'].str.len() >= 1000]
        print(f"Removed jobs with short descriptions, now we have {len(long_desc_jobs)} jobs")

        deduped_by_url = remove_duplicates_by_url(long_desc_jobs, 'job_url')
        print(f"Removed duplicates by URL, now we have {len(deduped_by_url)} jobs")

        # unsimilar = remove_duplicates_by_similarity(deduped_by_url, similarity_threshold)
        # print(f"Removed duplicates by similarity, now we have {len(unsimilar)} jobs")

        return deduped_by_url

    def remove_extraneous_columns(df):
        columns_to_keep = ['site', 'job_url', 'job_url_direct', 'title', 'company', 'location', 'job_type',
                           'date_posted',
                           'interval', 'min_amount', 'max_amount', 'currency', 'is_remote', 'emails', 'description',
                           'searched_title', 'user_id']
        columns_to_drop = [col for col in df.columns if col not in columns_to_keep]
        return df.drop(columns=columns_to_drop)

    def remove_duplicates_by_url(df, column_name='job_url'):
        if df.empty:
            print("DataFrame is empty. No duplicates to remove.")
            return df
        else:
            # Keep the first occurrence of each unique value in the specified column
            return df.drop_duplicates(subset=[column_name], keep='first')

    # def remove_duplicates_by_similarity(df, similarity_threshold=0.9):
    #     if df.empty:
    #         print("DataFrame is empty. Nothing to de-duplicate.")
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

    def get_jobs_with_derived(jobs_df, resume):
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

        rated_jobs = get_job_ratings(jobs_df, resume)
        todays_jobs = add_derived_data(rated_jobs, derived_data_questions, resume=resume)

        return todays_jobs

    def add_derived_data(jobs_df, derived_data_questions=[], resume=None):
        if len(derived_data_questions) == 0:
            return jobs_df

        print("Generating derived data...")
        derived_data = pd.DataFrame(index=jobs_df.index)

        for index, row in jobs_df.iterrows():
            job_description = f"Title: {row['title']}\nCompany: {row['company']}\nLocation: {row['location']}\n" \
                              f"Description: {row['description']}\n"

            pay_info = (f"Pays between {row['min_amount']} and {row['max_amount']} on a(n) {row['interval']}'"
                        f" basis.") if len(row['interval']) > 0 else ""

            job_description += pay_info
            print(f"{index}: Processing: {row['title']} at {row['company']}")

            for column_name, question in derived_data_questions:
                full_message = build_context_for_llm(job_description, resume, question)
                full_message = consolidate_text(full_message)

                answer = query_llm(llm='openai', model='gpt-3.5-turbo',
                                system="You are a helpful no-nonsense assistant. You listen to directions carefully and follow" \
                                     " them to the letter. Only return plain text, not markdown or HTML.",
                                messages=[{"role": "user", "content": full_message}])

                derived_data.at[index, column_name] = answer

        jobs_df_updated = pd.concat([derived_data, jobs_df], axis=1)
        return jobs_df_updated

    def build_context_for_llm(job_description, resume, question):
        full_message = ""
        if resume is not None:
            full_message += "Here is the candidate's resume: <resume>" + resume + "</resume> "
        if job_description:
            full_message += "Here is some information about a job. <job>" + job_description + "</job> "
        full_message += "Now for my question: " + question + " "
        return full_message

    def get_job_ratings(jobs_df, resume):
        print(f'Getting job ratings for {len(jobs_df)} jobs...')

        resume = consolidate_text(resume)

        for index, row in jobs_df.iterrows():
            job_title = row['title']
            job_description = row['description']
            
            job_description = consolidate_text(job_description)

            full_message = f"<resume>{resume}</resume>\n" + \
                           f"<job_title>{job_title}</job_title>\n" + \
                           f"<job_description>{job_description}</job_description>\n" + \
"""
Given the user resume (resume tag), job title (job_title tag) and job description 
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

Start your answer immediately with a bulleted list and then give an explanation for why you chose those
ratings. In the explanation only use plain text paragraphs without formatting. Example output is below 
(where NN is a 2 digit number):

- Candidate desire match: NN
- Candidate experience match: NN
- Hiring manager skill match: NN
- Hiring manager experience match: NN
- Final overall match assessment: NN
- Explanation of ratings: <Your explanation about why you chose those ratings, in paragraph form>
"""

            ratings = query_llm(llm="gemini",
                                model="gemini-1.5-flash",
                                system="You are a helpful assistant, proficient in giving ratings on how well a candidate"
                                       " matches a job posting.  You think critically and consider not only the content of"
                                       " the information given to you, but also the implications and intent of the"
                                       " information.",
                                messages=[{"role": "user", "content": full_message}])

            if ratings is None:
                print("LLM failed to generate ratings.")
                continue

            print(f"Ratings for job {index}: {ratings}")
            guidance = ratings.split("Explanation of ratings:")[1].strip()
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
                    print(f"{index}: Adding a rating to: {row['title']} at {row['company']}: {overall_job_score}")
                    jobs_df.at[index, 'job_score'] = overall_job_score
                else:
                    logging.error("Error: Unable to split overall job score.")

                if len(guidance) > 0:
                    print(f"{index}: Adding guidance to: {row['title']} at {row['company']}: {guidance}")
                    jobs_df.at[index, 'guidance'] = guidance
            else:
                logging.error("Error: Ratings list does not have enough elements.")

        jobs_over_50 = jobs_df[jobs_df['job_score'].astype(float) > 50]

        print('Found jobs with scores over 50: ' + str(len(jobs_over_50)))
        return jobs_over_50

    def consolidate_text(text):
        consolidated = text.replace('\r', ' ').replace('\n', ' ')
        consolidated = re.sub(' +', ' ', consolidated)
        return consolidated

    def save_jobs_to_supabase(user_id, df):
        print(f"Saving {len(df)} jobs to Supabase...")

        supabase_url = os.environ.get('SUPABASE_URL', 'Specified environment variable SUPABASE_URL is not set.')
        supabase_key = os.environ.get('SUPABASE_KEY', 'Specified environment variable SUPABASE_KEY is not set.')

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
                'date_pulled': datetime.now().isoformat()
            }

            print(new_job)
            result = supabase.table('jobs').insert(new_job).execute()

            if result.data:
                print(f"Inserted job!")
            else:
                logging.error(f"Error inserting job: {result.error}")
                continue

            users_jobs_row = {
                'user_id': user_id,
                'job_id': result.data[0].get('id'),
                'desire_score': row.get('desire_score'),
                'experience_score': row.get('experience_score'),
                'meets_requirements_score': row.get('meets_requirements_score'),
                'meets_experience_score': row.get('meets_experience_score'),
                'score': row.get('job_score'),
                'guidance': row.get('guidance'),
                'searched_title': row.get('searched_title')
            }

            association_result = supabase.table('users_jobs').insert(users_jobs_row).execute()
            if association_result.data:
                print("Inserted user job association!")  #: {association_result.data}")
            else:
                logging.error(f"Error inserting user job association: {association_result.error}")

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

    resume = data.get('resume')
    user_id = data.get('user_id')

    llm_job_titles = find_best_job_titles(resume)
    all_jobs = []
    job_sites = ['indeed', 'glassdoor', 'zip_recruiter', 'linkedin']
    for job_site in job_sites:
        all_jobs = get_jobs_for_user(job_site, user_id, llm_job_titles)
        if len(all_jobs) > 0:
            break

    cleaned_jobs = clean_and_deduplicate_jobs(all_jobs, similarity_threshold=0.9)

    if len(cleaned_jobs) == 0:
        return Response(response = "No jobs found after cleaning and deduplicating", status = 200)

    # Just keep the top 10 jobs
    cleaned_jobs = cleaned_jobs.head(10)
    jobs_with_derived = get_jobs_with_derived(cleaned_jobs, resume)
    save_jobs_to_supabase(user_id, jobs_with_derived)

    return Response(response = "Jobs pulled, cleaned, and saved to Supabase", status = 200)

# For running locally
# if __name__ == '__main__':
#     main('event', 'context')
