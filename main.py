import time
from file_utils import save_df_to_downloads, read_df_from_downloads, save_df_to_downloads_xlsx
from job_scraper import scrape_job_data, clean_and_deduplicate_jobs, sort_job_data, add_derived_data, reorder_columns
import json

from persistent_storage import save_jobs_to_supabase, get_role_configs, get_roles, get_recent_job_urls

if __name__ == '__main__':
    roles = get_roles()
    configs = get_role_configs()
    role_config_dict = {}
    for role in roles:
        role_id = role.get('id')
        role_configs = [config for config in configs if config.get('role_id') == role_id]
        role_config_dict[role_id] = role_configs

    for role_id, role_configs in role_config_dict.items():
        print(f"Looking for role with ID =: {role_id}")
        db_job_titles = [config['string_value'] for config in role_configs if config['key'] == 'job_titles']
        db_skill_words = [config['string_value'] for config in role_configs if config['key'] == 'skill_words']
        db_stop_words = [config['string_value'] for config in role_configs if config['key'] == 'stop_words']
        db_location = [config['string_value'] for config in role_configs if config['key'] == 'location']
        db_distance = [config['string_value'] for config in role_configs if config['key'] == 'distance']
        db_is_remote = [config['bool_value'] for config in role_configs if config['key'] == 'is_remote']
        db_candidate_min_salary = [config['int_value'] for config in role_configs if
                                   config['key'] == 'candidate_min_salary']
        db_resume = [config['string_value'] for config in role_configs if config['key'] == 'resume']
        db_candidate_requirements = [config['string_value'] for config in role_configs if
                                     config['key'] == 'candidate_requirements']

        job_titles = db_job_titles or []
        skill_words = db_skill_words or []

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

            all_jobs = scrape_job_data(
                role_id,
                job_titles,
                job_sites=['indeed', 'zip_recruiter', 'glassdoor', 'linkedin'],
                location=location,
                hours_old=24,
                results_wanted=20,
                distance=distance,
                is_remote=is_remote)

        stop_words = db_stop_words or []
        candidate_min_salary = db_candidate_min_salary[0] if (db_candidate_min_salary and
                                                              db_candidate_min_salary[0] is not None) else 0

        recent_job_urls = get_recent_job_urls(role_id)
        cleaned_jobs = clean_and_deduplicate_jobs(all_jobs, recent_job_urls, stop_words, skill_words, job_titles,
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
                                       'Provide a short summary of the key job responsibilities (no more than 3 '
                                       'sentences) and benefits (no more than 3 sentences), including'
                                       'pay info if available (no more than 1 sentence)'),
                                      ('hard_requirements',
                                       'List the hard requirements, things the candidate "must have" from the description,'
                                       ' if any'),
                                      ('job_score',
                                       f'Given the job title and description, how would you rate this job on a '
                                       'scale of 1-100 as a good match, given the stated job titles, skills, '
                                       'and candidate resume (if provided)?'
                                       f' Titles: {", ".join(job_titles)}'
                                       f' Skills: {", ".join(skill_words)}'
                                       ' Be as fine-grained with your assessment as possible.  Under no circumstances'
                                       ' should you output anything other than a single integer as an answer to this'
                                       ' question.')]

            todays_jobs = add_derived_data(cleaned_jobs, derived_data_questions, resume=resume, llm="chatgpt")
            save_df_to_downloads(todays_jobs, "compiled_jobs_with_derived")
            sorted_jobs = sort_job_data(todays_jobs, ['job_score'], [False])
        else:
            sorted_jobs = cleaned_jobs

        # Save to supabase
        save_jobs_to_supabase(sorted_jobs)
        save_df_to_downloads_xlsx(reorder_columns(sorted_jobs), "compiled_jobs")
