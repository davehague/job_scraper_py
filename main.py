import os
import time
from file_utils import save_df_to_downloads_xlsx
from job_scraper import scrape_job_data, clean_and_deduplicate_jobs, sort_job_data, add_derived_data, reorder_columns

# Logging
import logging
from pathlib import Path
import sys

from persistent_storage import save_jobs_to_supabase, get_user_configs, get_public_users, get_recent_job_urls
from llm import query_llm


def find_best_job_titles(db_user, user_configs):
    db_job_titles = [config['string_value'] for config in user_configs if config['key'] == 'job_titles']
    db_skill_words = [config['string_value'] for config in user_configs if config['key'] == 'skill_words']
    db_stop_words = [config['string_value'] for config in user_configs if config['key'] == 'stop_words']
    db_resume = db_user.get('resume')

    job_titles = db_job_titles or []
    skill_words = db_skill_words or []

    full_message = "Below is information that the candidate has provided.\n"
    full_message += "Provided Job Titles: " + ", ".join(job_titles) + "\n"
    full_message += "Desired verbiage in job description: " + ", ".join(skill_words) + "\n"

    if db_stop_words and len(db_stop_words) > 0:
        full_message += ("Candidate does not want jobs that have titles with these words: " +
                         ", ".join(db_stop_words) + "\n")

    if db_resume and db_resume[0] is not None:
        full_message += "In the <resume> tag below is the candidate resume, give extra weight to this information."
        full_message += "<resume>" + db_resume[0] + "</resume>\n"

    titles = query_llm(llm="anthropic",
                       # llm="openai",
                       # "gpt-3.5-turbo",
                       # model="gpt-4o-2024-05-13",
                       model="claude-3-opus-20240229",
                       system="You are an expert in searching job listings. You take all the information"
                              " given to you and come up with a list of 4 most relevant job titles. You do not"
                              " have to use the job titles provided by the candidate, but take them into"
                              " consideration.  Only list the titles in a comma-separated list, "
                              " no other information is needed.  IMPORTANT: ONLY INCLUDE THE JOB TITLES IN "
                              " A COMMA SEPARATED LIST.  DO NOT INCLUDE ANY OTHER INFORMATION.",
                       messages=[{"role": "user", "content": full_message}])

    if titles is None:  # Fall back if LLM failed
        titles = db_job_titles or []
    else:
        titles = [title.strip() for title in titles.split(",")] if titles else []

    return titles


def get_jobs_for_user(db_user, job_titles):
    print(f"Searching for job titles: {','.join(job_titles)}")

    db_is_remote = db_user.get('remote_preference')
    db_location = db_user.get('location')
    db_distance = db_user.get('distance')

    location = db_location if db_location is not None else 'USA'
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

    distance = db_distance[0] if db_distance and db_distance[0] is not None else 20
    if distance < 20:
        distance = 20

    db_results_wanted = db_user.get('results_wanted')
    results_wanted = db_results_wanted if db_results_wanted is not None else 20
    scraped_data = scrape_job_data(
        user_id,
        job_titles,
        job_sites=['indeed', 'zip_recruiter', 'glassdoor', 'linkedin'],
        location=location,
        hours_old=24,
        results_wanted=results_wanted,
        distance=distance,
        is_remote=is_remote)

    return scraped_data


def clean_up_jobs(jobs_df, user_configs):
    db_stop_words = [config['string_value'] for config in user_configs if config['key'] == 'stop_words']
    db_go_words = [config['string_value'] for config in user_configs if config['key'] == 'go_words']
    db_candidate_min_salary = user.get('min_salary')

    stop_words = db_stop_words or []
    go_words = db_go_words or []
    candidate_min_salary = db_candidate_min_salary[0] if (db_candidate_min_salary and
                                                          db_candidate_min_salary[0] is not None) else 0

    recent_job_urls = get_recent_job_urls(3)
    results_df = clean_and_deduplicate_jobs(jobs_df, recent_job_urls,
                                            stop_words, go_words, candidate_min_salary, similarity_threshold=0.9)
    return results_df


def get_jobs_with_derived(db_user, jobs_df, job_titles, user_configs):
    db_resume = db_user.get('resume')
    resume = db_resume[0] if db_resume and db_resume[0] is not None else None

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
                               ' each'),
                              ('job_score',
                               f'Given the information you have, how would you rate this job on a'
                               ' scale of 1-100 as a good match, given the stated job titles, stated desired'
                               ' keywords, and candidate resume (if provided)?'
                               f' Desired titles: {", ".join(job_titles)}.  '
                               f' Desired Keywords from the description: {", ".join(skill_words)}.  '
                               ' Think through this number carefully and be as fine-grained with your'
                               ' assessment as possible.  Under no circumstances should you output anything'
                               ' other than a single integer as an answer to this question.')]

    todays_jobs = add_derived_data(jobs_df, derived_data_questions, resume=resume, llm="chatgpt")
    return todays_jobs


SCHEDULED = False
if __name__ == '__main__':

    if SCHEDULED:
        downloads_path = Path(os.path.join(os.path.expanduser('~'), 'Downloads'))
        log_file = downloads_path / 'job_scraper.log'
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

    public_users = get_public_users()
    for user in public_users:
        user_id = user.get('id')
        configs = get_user_configs(user_id)

        llm_job_titles = find_best_job_titles(user, configs)
        all_jobs = get_jobs_for_user(user, llm_job_titles)

        cleaned_jobs = clean_up_jobs(all_jobs, configs)

        if len(cleaned_jobs) == 0:
            print("No jobs found, trying the next user.")
            time.sleep(15)
            continue

        jobs_with_derived = get_jobs_with_derived(user, cleaned_jobs, user_id, configs)
        sorted_jobs = sort_job_data(jobs_with_derived, ['job_score'], [False])

        # Save to supabase
        save_jobs_to_supabase(user_id, sorted_jobs)
        save_df_to_downloads_xlsx(reorder_columns(sorted_jobs), "compiled_jobs")
