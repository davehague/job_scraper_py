from file_utils import save_to_file, populate_jobs_dataframe_from_file
from job_scraper import scrape_job_data, clean_and_deduplicate, sort_job_data, add_derived_data
from query_with_llama import query_data

if __name__ == '__main__':
    # Get jobs
    from_file = False
    if from_file:
        all_jobs = populate_jobs_dataframe_from_file('compiled_jobs_no_derived_2024-04-01-15-58-01.csv')
    else:
        # job_list = ['CAM Engineer', 'CAM Technician', 'CNC Programmer', 'Manufacturing Engineer',
        #             'Process Control Engineer']
        job_list = ['Software Engineer', 'Software Developer', 'Web Developer', 'Full Stack Developer']
        all_jobs = scrape_job_data(job_list)

    stop_words = ["Senior", "Sr.", "III", "II", "Lead", "Internship", "Manager", "Director", "Architect"]
    cleaned_jobs = clean_and_deduplicate(all_jobs, stop_words, similarity_threshold=0.9)

    generate_derived_data = True
    if generate_derived_data:
        min_years_exp = "Does the job mention a minimum number of years of experience as a requirement?"
        mentions_skill = "Does the job mention any of the following:  Python, C#, Java, or similar skills?"

        # questions = ["Does the job mention any of the following:  CAD programming, CAM programming, PLC control "
        #              "systems, CNC programming, or similar skills?"]
        # with open("jh-resume.txt", "r") as file:
        #    resume = file.read()

        derived_data_questions = [
            ('min_years_exp', min_years_exp),
            ('mentions_skill', mentions_skill)
        ]
        todays_jobs = add_derived_data(cleaned_jobs, derived_data_questions, resume=None)
    else:
        todays_jobs = cleaned_jobs

    sorted_jobs = sort_job_data(todays_jobs, ['company', 'title', 'location'], [True, True, True])
    save_to_file(sorted_jobs, "compiled_jobs")

    # Only keep new jobs
    # yesterdays_jobs = populate_jobs_dataframe_from_file('compiled_jobs_2024-03-27-03m.csv')
    # new_jobs = get_new_rows(yesterdays_jobs, todays_jobs)  # Needs work
    # save_to_file(new_jobs, "new_jobs_today")
    # Write jobs to downloads
    # todays_jobs = populate_jobs_dataframe_from_file('compiled_jobs_2024-03-28-11-03-48.csv')
    # write_jobs_to_downloads("todays_jobs", todays_jobs)

    # Query data
    # query_data("What are the jobs in here for a CAD / CAM programmer? List the title, company, and location")
