from file_utils import save_df_to_downloads, read_df_from_downloads, save_df_to_downloads_xlsx
from job_scraper import scrape_job_data, clean_and_deduplicate_jobs, sort_job_data, add_derived_data, reorder_columns
import json

from persistent_storage import save_jobs_to_supabase

if __name__ == '__main__':
    # Set up config
    config_file_path = 'mock_configs/senior_ux.json'
    with open(config_file_path) as json_file:
        config = json.load(json_file)

    job_titles = config.get('job_titles') or []
    skill_words = config.get('skill_words') or []

    # Get jobs
    from_file = False
    if from_file:
        all_jobs = read_df_from_downloads('compiled_jobs_with_derived_2024-05-13-21-08-07.csv')
        # save_jobs_to_supabase(all_jobs)
    else:
        is_remote = config.get('is_remote') or False
        location = config.get('location') or 'Columbus, OH'

        if config.get('distance') < 20:
            distance = 20

        distance = config.get('distance') or 20

        all_jobs = scrape_job_data(job_titles,
                                   job_sites=['indeed', 'zip_recruiter', 'glassdoor', 'linkedin'],
                                   location=location,
                                   hours_old=24,
                                   results_wanted=20,
                                   distance=distance,
                                   is_remote=is_remote)

    stop_words = config.get('stop_words') or []
    candidate_min_salary = config.get('candidate_min_salary') or 0
    cleaned_jobs = clean_and_deduplicate_jobs(all_jobs, stop_words, skill_words, job_titles, candidate_min_salary,
                                              similarity_threshold=0.9)

    generate_derived_data = True
    if generate_derived_data:
        candidate_requirements = config.get('candidate_requirements') or []
        resume = config.get('resume')

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
