from file_utils import save_df_to_downloads, read_df_from_downloads
from job_scraper import scrape_job_data, clean_and_deduplicate_jobs, sort_job_data, add_derived_data

if __name__ == '__main__':
    # Get jobs
    from_file = True
    if from_file:
        all_jobs = read_df_from_downloads('compiled_jobs_2024-04-03-15-24-08.csv')
    else:
        # job_titles = ['CAM Engineer', 'CAM Technician', 'CNC Programmer', 'Manufacturing Engineer',
        #       'Process Control Engineer']
        # job_titles = ['Software Engineer', 'Software Developer', 'Web Developer', 'Full Stack Developer']
        # job_titles = ['Software Quality Assurance', 'Software QA', 'Software QA Manager', 'Software QA Lead',
        #               'Software Tester']
        current_job_titles = ["Jewelry Consultant", "Sales Associate",
                              "Salesperson", "Personal Banker"]
        gpt_job_tiles = ['Patient Advocacy Coordinator', 'Medical Sales Representative', 'Healthcare Administrator',
                         'Biological Technician', 'Patient Services Coordinator']

        all_jobs = scrape_job_data(gpt_job_tiles,
                                   job_sites=['indeed', 'zip_recruiter', 'glassdoor', 'linkedin'],
                                   location='Dublin, OH',
                                   hours_old=24,
                                   results_wanted=5,
                                   distance=20,
                                   is_remote=True)

    # stop_words = ["Senior", "Sr.", "III", "II", "Lead", "Internship", "Manager", "Director", "Architect"]
    # stop_words = ['Pharmacy', 'Lab', 'Technician', 'Tech', 'Sales', 'Plant', 'Maintenance', 'Manufacturing', 'Food',
    #               'Teacher', 'Scientist']
    stop_words = ['Director']
    cleaned_jobs = clean_and_deduplicate_jobs(all_jobs, stop_words, similarity_threshold=0.9)

    # skills = ['Test cases', 'quality assurance', 'test plans', 'agile', 'automation']

    generate_derived_data = False
    if generate_derived_data:
        min_years_exp = "If the job mentions a minimum number of years of experience, list the wording used"
        hard_requirements = "List the hard requirements, things the candidate 'must have' from the description, if any"
        # mentions_skill = "Does the job mention any of the following:  Python, C#, Java, or similar skills?"

        # questions = ["Does the job mention any of the following:  CAD programming, CAM programming, PLC control "
        #              "systems, CNC programming, or similar skills?"]
        # with open("resume.txt", "r") as file:
        #    resume = file.read()

        derived_data_questions = [
            ('min_years_exp', min_years_exp),
            ('hard_requirements', hard_requirements)
        ]
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
