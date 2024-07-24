import os
import time

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

from job_helpers import find_best_job_titles_for_user, job_meets_salary_requirements, job_matches_stop_words, \
    get_job_guidance_for_user
from job_scraper import scrape_job_data, clean_and_deduplicate_jobs, sort_job_data, add_derived_data, reorder_columns
from helpers import consolidate_text

# Logging
import logging
from pathlib import Path
import sys

from persistent_storage import save_jobs_to_supabase, get_user_configs, get_active_users_with_resume, \
    get_recent_job_urls, \
    save_titles_for_user, get_recent_jobs, add_user_job_association, get_user_by_id, get_job_by_id, \
    user_has_recommendation
from llm import query_llm
from send_emails import send_email_updates


def get_job_ratings(original_df, db_user, user_configs):
    jobs_df = original_df.copy()
    db_job_titles = [config['string_value'] for config in user_configs if config['key'] == 'job_titles']
    db_skill_words = [config['string_value'] for config in user_configs if config['key'] == 'skill_words']
    db_stop_words = [config['string_value'] for config in user_configs if config['key'] == 'stop_words']
    db_resume = db_user.get('resume')

    job_titles = db_job_titles or []
    skill_words = db_skill_words or []
    stop_words = db_stop_words or []

    resume = consolidate_text(db_resume)

    for index, row in jobs_df.iterrows():
        job_title = row.get('title', "N/A")
        job_description = row.get('description', "N/A")
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

        ratings = query_llm(llm="openai",
                            model_name="gpt-4o-mini",
                            system="You are a helpful no-nonsense assistant. You listen to directions carefully and follow them to the letter.",
                            messages=[{"role": "user", "content": full_message}])

        if ratings is None:
            print("LLM failed to generate ratings.")
            continue

        print(f"Ratings for job {index}: {ratings}")

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
                print("Error: Unable to split desire score.")

            experience_score_split = ratings[1].split(":")
            if len(experience_score_split) == 2:
                experience_score = experience_score_split[1].strip()
                jobs_df.at[index, 'experience_score'] = experience_score
            else:
                print("Error: Unable to split experience score.")

            meets_requirements_score_split = ratings[2].split(":")
            if len(meets_requirements_score_split) == 2:
                meets_requirements_score = meets_requirements_score_split[1].strip()
                jobs_df.at[index, 'meets_requirements_score'] = meets_requirements_score
            else:
                print("Error: Unable to split meets requirements score.")

            meets_experience_score_split = ratings[3].split(":")
            if len(meets_experience_score_split) == 2:
                meets_experience_score = meets_experience_score_split[1].strip()
                jobs_df.at[index, 'meets_experience_score'] = meets_experience_score
            else:
                print("Error: Unable to split meets experience score.")

            overall_job_score_split = ratings[4].split(":")
            if len(overall_job_score_split) == 2:
                overall_job_score = overall_job_score_split[1].strip()
                print(
                    f"{index}: Adding a rating to: {row.get('title', 'N/A')} at {row.get('company', 'N/A')}: {overall_job_score}")
                jobs_df.at[index, 'job_score'] = overall_job_score
            else:
                print("Error: Unable to split overall job score.")

            if len(guidance) > 0:
                print(
                    f"{index}: Adding guidance to: {row.get('title', 'N/A')} at {row.get('company', 'N/A')}: {guidance}")
                jobs_df.at[index, 'guidance'] = guidance
        else:
            print("Error: Ratings list does not have enough elements.")

    jobs_over_50 = jobs_df[jobs_df['job_score'].astype(float) > 50]

    print('Found jobs with scores over 50: ' + str(len(jobs_over_50)))
    return jobs_over_50


def get_jobs_for_user(db_user, job_titles):
    print(f"Searching for job titles: {','.join(job_titles)}")

    db_is_remote = db_user.get('remote_preference')
    db_location = db_user.get('location')
    db_distance = db_user.get('distance')

    match db_is_remote:
        case "YES":
            is_remote = True
        case "NO":
            is_remote = False
        case "ONLY":
            is_remote = True
            location = 'USA'
        case _:
            is_remote = False

    distance = db_distance if db_distance is not None else 20
    if distance < 20:
        distance = 20

    db_results_wanted = db_user.get('results_wanted')
    results_wanted = db_results_wanted if db_results_wanted is not None else 20
    scraped_data = scrape_job_data(
        user_id,
        job_titles,
        job_sites=['indeed', 'zip_recruiter', 'glassdoor', 'linkedin'],
        location=db_location,
        hours_old=24,
        results_wanted=results_wanted,
        distance=distance,
        is_remote=is_remote)

    # Filter out jobs where is_remote is True or is_remote is not specified
    if db_is_remote == "ONLY":
        scraped_data = scraped_data[scraped_data['is_remote'] != False]
        scraped_data = scraped_data[
            scraped_data['is_remote'] != True & scraped_data['description'].str.contains("remote", case=False,
                                                                                         na=False)]

    return scraped_data


def clean_up_jobs(jobs_df, user_configs):
    db_stop_words = [config['string_value'] for config in user_configs if config['key'] == 'stop_words']
    db_go_words = [config['string_value'] for config in user_configs if config['key'] == 'go_words']
    db_candidate_min_salary = user.get('min_salary')

    stop_words = db_stop_words or []
    go_words = db_go_words or []
    candidate_min_salary = db_candidate_min_salary if db_candidate_min_salary is not None else 0

    recent_job_urls = get_recent_job_urls(3)
    results_df = clean_and_deduplicate_jobs(jobs_df, recent_job_urls,
                                            stop_words, go_words, candidate_min_salary, similarity_threshold=0.9)
    return results_df


def get_jobs_with_derived(db_user, jobs_df, job_titles, user_configs):
    db_resume = db_user.get('resume')
    resume = db_resume

    # TODO : we're not using the skill words
    db_skill_words = [config['string_value'] for config in user_configs if config['key'] == 'skill_words']
    skill_words = db_skill_words or []

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

    rated_jobs = get_job_ratings(jobs_df, db_user, user_configs)
    todays_jobs = add_derived_data(rated_jobs, derived_data_questions, resume=resume, llm="chatgpt")

    return todays_jobs


def find_titles_by_similarity(target_title, job_list, similarity_threshold=0.9):
    # Extract job titles from job_list
    job_ids, job_titles = zip(*job_list)

    # Combine target_title with job_titles
    all_titles = [target_title] + list(job_titles)

    # Use TF-IDF to vectorize the titles
    vectorizer = TfidfVectorizer()
    tfidf_matrix = vectorizer.fit_transform(all_titles)

    # Compute cosine similarity
    cosine_sim = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:])

    # Find indices where similarity is above the threshold
    similar_indices = np.where(cosine_sim[0] >= similarity_threshold)[0]

    # Create list of matching jobs
    matching_jobs = [
        (job_ids[i], job_titles[i], cosine_sim[0][i])
        for i in similar_indices
    ]

    # Sort matching jobs by similarity in descending order
    matching_jobs.sort(key=lambda x: x[2], reverse=True)

    return matching_jobs


def find_existing_jobs_for_users(users):
    # Get all recent job and their title (id, title)
    recent_jobs = get_recent_jobs(days_old=1)

    if len(recent_jobs) == 0:
        print("No recent jobs found, skipping...")
        return None

    for user in users:
        # Get the users job titles
        user_id = user.get('id')
        configs = get_user_configs(user_id)
        db_job_titles = [config['string_value'] for config in configs if config['key'] == 'job_titles']
        user_titles = db_job_titles or []

        matched_jobs = []
        for title in user_titles:
            # Find jobs that are 70% similar by title
            matching_jobs = find_titles_by_similarity(title, recent_jobs, similarity_threshold=0.7)
            for job in matching_jobs:
                if user_has_recommendation(user_id, job[0]):
                    continue
                else:
                    user = get_user_by_id(user_id)
                    user_configs = get_user_configs(user_id)
                    job_id = job[0]
                    job = get_job_by_id(job_id)

                    if not job_meets_salary_requirements(user, job):
                        print(
                            f"Job with URL {job_id} does not meet salary requirements for user {user_id}, skipping...")
                        return None

                    if job_matches_stop_words(user_configs, job):
                        print(f"Job with URL {job_id} matches stop words for user {user_id}, skipping...")
                        return None

                    ratings = get_job_guidance_for_user(user, user_configs, job)

                    if int(ratings.get('overall_score', 0)) < 70:
                        print(f"Job with URL {job_id} has a score less than 70, skipping...")
                    else:
                        print(
                            f"Job with URL {job_id} has a score of {ratings.get('overall_score')}, adding association "
                            f"for user {user_id}...")

                    add_user_job_association(user_id, job_id, ratings)

    return matched_jobs


SCHEDULED = False
if __name__ == '__main__':

    if SCHEDULED:
        downloads_path = Path(os.path.join(os.path.expanduser('~'), 'Downloads'))
        todays_date = time.strftime("%Y-%m-%d")
        log_file = downloads_path / f'job_scraper_{todays_date}.log'
        logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')


        # Redirect stdout and stderr to the logging system
        class StreamToLogger:
            def __init__(self, logger, log_level):
                self.logger = logger
                self.log_level = log_level
                self.linebuf = ''

            def write(self, buf):
                for line in buf.rstrip().splitlines():
                    self.logger.log(self.log_level, line.rstrip())

            def flush(self):
                pass


        # Redirect stdout and stderr
        sys.stdout = StreamToLogger(logging.getLogger('STDOUT'), logging.INFO)
        sys.stderr = StreamToLogger(logging.getLogger('STDERR'), logging.ERROR)

    eligible_users = get_active_users_with_resume()

    for user in eligible_users:
        user_id = user.get('id')
        print(f"Processing user: {user_id} ({user.get('name')})")

        # if user_id != '7d4cdc06-7929-453d-9ab0-88a5901a22fd':
        #     continue

        if len(user.get('resume')) < 100:
            print("Resume is too short, skipping.")
            continue

        configs = get_user_configs(user_id)
        best_titles = find_best_job_titles_for_user(user, configs)
        all_jobs = get_jobs_for_user(user, best_titles)

        cleaned_jobs = clean_up_jobs(all_jobs, configs)

        if len(cleaned_jobs) == 0:
            print("No jobs found, trying the next user.")
            time.sleep(15)
            continue

        if len(cleaned_jobs) > 10:
            print(f"We've got {len(cleaned_jobs)} cleaned jobs, truncating to 10.")
            cleaned_jobs = cleaned_jobs.head(10)

        jobs_with_derived = get_jobs_with_derived(user, cleaned_jobs, best_titles, configs)
        sorted_jobs = sort_job_data(jobs_with_derived, ['job_score'], [False])

        # Save to supabase
        save_jobs_to_supabase(user_id, sorted_jobs)

    find_existing_jobs_for_users(eligible_users)
    send_email_updates()
