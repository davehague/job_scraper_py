import os
import time
from file_utils import save_df_to_downloads, read_df_from_downloads, save_df_to_downloads_xlsx
from job_scraper import scrape_job_data, clean_and_deduplicate_jobs, sort_job_data, add_derived_data, reorder_columns
import json

from persistent_storage import save_jobs_to_supabase, get_role_configs, get_roles, get_recent_job_urls
from llm import query_llm

if __name__ == '__main__':
    roles = get_roles()
    configs = get_role_configs()
    role_config_dict = {}
    for role in roles:
        role_id = role.get('id')
        role_configs = [config for config in configs if config.get('role_id') == role_id]
        role_config_dict[role_id] = role_configs

    for role_id, role_configs in role_config_dict.items():
        # if (role_id != 4):
        #     continue

        print(f"Looking for role with ID = {role_id}")
        db_job_titles = [config['string_value'] for config in role_configs if config['key'] == 'job_titles']
        db_skill_words = [config['string_value'] for config in role_configs if config['key'] == 'skill_words']
        db_stop_words = [config['string_value'] for config in role_configs if config['key'] == 'stop_words']
        db_go_words = [config['string_value'] for config in role_configs if config['key'] == 'go_words']
        db_location = [config['string_value'] for config in role_configs if config['key'] == 'location']
        db_distance = [config['string_value'] for config in role_configs if config['key'] == 'distance']
        db_is_remote = [config['bool_value'] for config in role_configs if config['key'] == 'is_remote']
        db_candidate_min_salary = [config['int_value'] for config in role_configs if
                                   config['key'] == 'candidate_min_salary']
        db_resume = [config['string_value'] for config in role_configs if config['key'] == 'resume']
        db_candidate_requirements = [config['string_value'] for config in role_configs if
                                     config['key'] == 'candidate_requirements']
        db_results_wanted = [config['int_value'] for config in role_configs if config['key'] == 'results_wanted']

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

        llm_job_titles = query_llm(llm="anthropic",
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

        if llm_job_titles is None:  # Fall back if LLM failed
            job_titles = db_job_titles or []
        else:
            job_titles = [title.strip() for title in llm_job_titles.split(",")] if llm_job_titles else []

        print(f"Searching for job titles: {','.join(job_titles)}")
        # Get jobs
        from_file = False
        if from_file:
            all_jobs = read_df_from_downloads('scraped_jobs_2024-05-20-10-02-15.csv')
            # save_jobs_to_supabase(all_jobs)
        else:
            is_remote = db_is_remote[0] if db_is_remote and db_is_remote[0] is not None else False
            location = db_location[0] if db_location and db_location[0] is not None else 'Columbus, OH'
            distance = db_distance[0] if db_distance and db_distance[0] is not None else 20

            if distance < 20:
                distance = 20

            results_wanted = db_results_wanted[0] if db_results_wanted and db_results_wanted[0] is not None else 20
            all_jobs = scrape_job_data(
                role_id,
                job_titles,
                job_sites=['indeed', 'zip_recruiter', 'glassdoor', 'linkedin'],
                location=location,
                hours_old=24,
                results_wanted=results_wanted,
                distance=distance,
                is_remote=is_remote)

        stop_words = db_stop_words or []
        go_words = db_go_words or []
        candidate_min_salary = db_candidate_min_salary[0] if (db_candidate_min_salary and
                                                              db_candidate_min_salary[0] is not None) else 0

        recent_job_urls = get_recent_job_urls(role_id)
        cleaned_jobs = clean_and_deduplicate_jobs(all_jobs, recent_job_urls, stop_words, go_words, skill_words,
                                                  job_titles,
                                                  candidate_min_salary,
                                                  similarity_threshold=0.9)

        if len(cleaned_jobs) == 0:
            print("No jobs found, trying the next job.")
            time.sleep(30)
            continue

        generate_derived_data = True
        if generate_derived_data:
            candidate_requirements = db_candidate_requirements or []
            resume = db_resume[0] if db_resume and db_resume[0] is not None else None

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

            todays_jobs = add_derived_data(cleaned_jobs, derived_data_questions, resume=resume, llm="chatgpt")
            save_df_to_downloads(todays_jobs, "compiled_jobs_with_derived")
            sorted_jobs = sort_job_data(todays_jobs, ['job_score'], [False])
        else:
            sorted_jobs = cleaned_jobs

        # Save to supabase
        save_jobs_to_supabase(sorted_jobs)
        save_df_to_downloads_xlsx(reorder_columns(sorted_jobs), "compiled_jobs")
