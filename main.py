from file_utils import save_df_to_downloads, read_df_from_downloads
from job_scraper import scrape_job_data, clean_and_deduplicate_jobs, sort_job_data, add_derived_data
import json

if __name__ == '__main__':
    # Set up config
    config_file_path = 'mock_configs/software_qa.json'
    with open(config_file_path) as json_file:
        config = json.load(json_file)

    # Get jobs
    from_file = False
    if from_file:
        all_jobs = read_df_from_downloads('compiled_jobs_2024-04-03-15-24-08.csv')
    else:
        job_titles = config.get('job_titles') or []
        is_remote = config.get('is_remote') or False
        location = config.get('location') or 'Columbus, OH'
        distance = config.get('distance') or 20

        all_jobs = scrape_job_data(job_titles,
                                   job_sites=['zip_recruiter'],  # 'indeed', 'zip_recruiter', 'glassdoor', 'linkedin'],
                                   location=location,
                                   hours_old=24,
                                   results_wanted=15,
                                   distance=distance,
                                   is_remote=is_remote)

    stop_words = config.get('stop_words') or []
    cleaned_jobs = clean_and_deduplicate_jobs(all_jobs, stop_words, similarity_threshold=0.9)

    generate_derived_data = True
    if generate_derived_data:
        skill_words = config.get('skill_words') or []
        candidate_requirements = config.get('candidate_requirements') or []
        resume = config.get('resume')

        derived_data_questions = [('min_years_exp',
                                   'If the job mentions a minimum number of years of experience, list the wording used'),
                                  ('hard_requirements',
                                   'List the hard requirements, things the candidate "must have" from the '
                                   'description, if any')]

        if skill_words and len(skill_words) > 0:
            derived_data_questions.append(
                ('mentions_skill',
                 f'Does the job mention any of the following:  {", ".join(skill_words)} or similar skills?'))

        if candidate_requirements and len(candidate_requirements) > 0:
            derived_data_questions.append(
                ('candidate_requirements',
                 f'The candidate has listed their "must have" items: things this job must have, which are: '
                 f'{", ".join(candidate_requirements)}'))

        if resume:
            print(f"Resume: {resume}")
            derived_data_questions.append(
                ('resume_match',
                 f'Act as the hiring manager for this job, would you consider this candidate based on this resume?'
                 f'{resume}'))

        todays_jobs = add_derived_data(cleaned_jobs, derived_data_questions, resume=None)
    else:
        todays_jobs = cleaned_jobs

    sorted_jobs = sort_job_data(todays_jobs, ['company', 'title', 'location'], [True, True, True])
    save_df_to_downloads(sorted_jobs, "compiled_jobs")

    # Only keep new jobs
    # yesterdays_jobs = populate_jobs_dataframe_from_file('compiled_jobs_2024-03-27-03m.csv')
    # new_jobs = get_new_rows(yesterdays_jobs, todays_jobs)  # Needs work
    # save_to_file(new_jobs, "new_jobs_today")
    # Write jobs to downloads
    # todays_jobs = populate_jobs_dataframe_from_file('compiled_jobs_2024-03-28-11-03-48.csv')
    # write_jobs_to_downloads("todays_jobs", todays_jobs)

    # Query data
    # query_data("What are the jobs in here for a CAD / CAM programmer? List the title, company, and location")
