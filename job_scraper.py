import os

from jobspy import scrape_jobs
from file_utils import populate_jobs_dataframe_from_file, save_to_file
from send_jobs_to_documents import write_jobs_to_downloads
import pandas as pd

import time
import anthropic
from dotenv import load_dotenv

def get_jobs(title):
    jobs = scrape_jobs(
        site_name=["indeed", "zip_recruiter", "glassdoor", "linkedin"],
        linkedin_fetch_description=True,
        search_term=title,
        location="Columbus, OH",
        results_wanted=10,
        hours_old=24,  # (only Linkedin/Indeed is hour specific, others round up to days old)
        country_indeed='USA'  # only needed for indeed / glassdoor
    )
    jobs['Job Title'] = title  # Add a column to indicate the job title
    return jobs.dropna(axis=1, how='all') if not jobs.empty else pd.DataFrame()


def get_linkedin_jobs(title):
    jobs = scrape_jobs(
        site_name=["linkedin"],
        linkedin_fetch_description=True,
        search_term=title,
        location="Columbus, OH",
        results_wanted=10,
        hours_old=24,  # (only Linkedin/Indeed is hour specific, others round up to days old)
        country_indeed='USA'  # only needed for indeed / glassdoor
    )
    jobs['Job Title'] = title  # Add a column to indicate the job title
    return jobs.dropna(axis=1, how='all') if not jobs.empty else pd.DataFrame()


def sort_jobs(df, sort_columns, ascending_orders):
    if df.empty:
        print("DataFrame is empty. No sorting applied.")
        return df
    else:
        return df.sort_values(by=sort_columns, ascending=ascending_orders)


def remove_duplicates_by_url(df, column_name='job_url'):
    if df.empty:
        print("DataFrame is empty. No duplicates to remove.")
        return df
    else:
        # Keep the first occurrence of each unique value in the specified column
        return df.drop_duplicates(subset=[column_name], keep='first')


def populate_jobs_dataframe_from_web():
    job_list = ['CAM Engineer', 'CNC Programmer', 'Manufacturing Engineer', 'Process Improvement Engineer',
                'Automation Engineer']
    all_jobs = pd.DataFrame()  # Initialize an empty DataFrame to hold all jobs

    for job in job_list:
        job_df = get_jobs(job)
        time.sleep(60)
        # job_df = get_linkedin_jobs(job)
        if not job_df.empty:
            all_jobs = pd.concat([all_jobs, job_df], ignore_index=True)

    all_jobs_no_duplicates = remove_duplicates_by_url(all_jobs, 'job_url')
    return all_jobs_no_duplicates


def ask_claude_about_job(question, job_description=None, include_resume=False):
    load_dotenv()
    anthropic_api_key = os.environ.get("ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(
        api_key=anthropic_api_key,
    )

    messages = []
    full_message = ''
    if include_resume:
        full_message += "Here is Jonathan's resume, below\n"
        with open("jh-resume.txt", "r") as file:
            resume = file.read()
            full_message += resume + "\n\n"

    if job_description:
        full_message += "Here is some information about a job\n\n" + job_description + "\n\n"

    full_message += "Now for my questions: \n" + question

    model = "claude-3-haiku-20240307"
    # model = "claude-3-sonnet-20240229"
    message = client.messages.create(
        model=model,
        max_tokens=1000,
        temperature=0.0,
        system="You are a helpful assistant, specializing in job search.  Your goal is to find the best matching job for my user",
        messages=[
            {"role": "user", "content": full_message}
        ]
    )

    return message.content


def add_derived_data(jobs_df):
    for index, row in jobs_df.iterrows():
        # for index, row in sorted_jobs.head(5).iterrows():
        job_description = f"Title: {row['title']}\nCompany: {row['company']}\nLocation: {row['location']}\n" \
                          f"Description: {row['description']}"
        print(f"{index}: Processing: {row['title']} at {row['company']}")

        questions = """1. Does the job have a requirement for a minimum number of years experience? If so, give the exact wording\n 2. Does the job require a 
            degree, and if so, what is it?\n 3. Does the job mention CAD programming or CAM programming? Give the exact wording from the description or title. 
            mentioned.\n 4. Is this job a good fit based on the resume and the job requirements and expectations? Give an explanation why or why not. 
            \n\nFormat your answer in a list and do not include an introduction or conclusion."""

        answers = ask_claude_about_job(questions, job_description, True)
        # Split the text into lines and remove the first line (which is empty)
        lines = answers[0].text.split('\n')
        lines = [line for line in lines if line.strip()]

        # Assign each line to a separate variable, removing the opening numbered list
        min_years = lines[0][3:].strip()
        requires_degree = lines[1][3:].strip()
        mentions_cadcam = lines[2][3:].strip()

        first_line = lines[3][3:].strip()
        remaining_lines = lines[4:]
        is_good_fit = "\n".join([first_line] + remaining_lines).strip()

        # add the above 4 to the dataframe
        jobs_df.at[index, 'min_years'] = min_years
        jobs_df.at[index, 'requires_degree'] = requires_degree
        jobs_df.at[index, 'mentions_cadcam'] = mentions_cadcam
        jobs_df.at[index, 'is_good_fit'] = is_good_fit

        #time.sleep(2)

    return jobs_df


def get_new_rows(df1, df2):
    # Merge the two DataFrames, keeping all rows from both
    merged = pd.merge(df1, df2, on='job_url', how='outer', indicator=True)

    # Filter the merged DataFrame to only include rows that only appear in the second DataFrame
    new_rows = merged[merged['_merge'] == 'right_only']

    # Drop the _merge column as it's no longer needed
    new_rows = new_rows.drop(columns='_merge')

    return new_rows


def generate_job_data(generate_derived_data=False):
    all_jobs = populate_jobs_dataframe_from_web()
    sorted_jobs = sort_jobs(all_jobs, ['company', 'title', 'location'], [True, True, True])

    if not sorted_jobs.empty:
        print(f"Found {len(sorted_jobs)} jobs")

        if generate_derived_data:
            save_to_file(sorted_jobs, "compiled_jobs_no_derived") # in case deriving fails
            sorted_jobs = add_derived_data(sorted_jobs)

        return sorted_jobs
    else:
        print("No jobs found.")