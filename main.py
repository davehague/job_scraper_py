from file_utils import save_to_file, populate_jobs_dataframe_from_file
from job_scraper import generate_job_data, add_derived_data, remove_duplicates_by_similarity
from query_with_llama import query_data

if __name__ == '__main__':
    # Get jobs
    from_file = True
    generate_derived_data = True

    if from_file:
        sorted_jobs = populate_jobs_dataframe_from_file('compiled_jobs_2024-03-27-03m.csv')
        todays_jobs = remove_duplicates_by_similarity(sorted_jobs)

        #todays_jobs = add_derived_data(sorted_jobs)
    else:
        todays_jobs = generate_job_data(generate_derived_data)

    save_to_file(todays_jobs, "compiled_jobs")
    # Only keep new jobs
    # yesterdays_jobs = populate_jobs_dataframe_from_file('compiled_jobs_2024-03-27-03m.csv')
    # new_jobs = get_new_rows(yesterdays_jobs, todays_jobs)  # Needs work
    # save_to_file(new_jobs, "new_jobs_today")
    # Write jobs to downloads
    # todays_jobs = populate_jobs_dataframe_from_file('compiled_jobs_2024-03-28-11-03-48.csv')
    # write_jobs_to_downloads("todays_jobs", todays_jobs)

    # Query data
    #query_data("What are the jobs in here for a CAD / CAM programmer? List the title, company, and location")