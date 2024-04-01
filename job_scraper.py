import os

import numpy as np
from jobspy import scrape_jobs
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from file_utils import populate_jobs_dataframe_from_file, save_to_file
from send_jobs_to_documents import write_jobs_to_downloads
import pandas as pd

import time
import anthropic
from dotenv import load_dotenv
from requests.exceptions import HTTPError


def get_jobs_with_backoff(job_title, max_retries=5, initial_wait=5):
    """
    Attempts to fetch job data with exponential backoff.

    :param job_title: The title of the job to fetch.
    :param max_retries: Maximum number of retry attempts.
    :param initial_wait: Initial wait time in seconds before the first retry.
    :return: Job DataFrame or None if unsuccessful.
    """
    attempt = 0
    wait_time = initial_wait

    while attempt < max_retries:
        try:
            # Assuming `get_jobs` makes a request and returns a DataFrame
            job_df = get_jobs(job_title)
            return job_df
        except HTTPError as e:
            if e.response.status_code in (429, 500):
                print(f"Error {e.response.status_code} received, retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                wait_time *= 2  # Exponential backoff
                attempt += 1
            else:
                # For other HTTP errors, you might want to raise the error or handle differently
                raise
        except Exception as e:
            # For non-HTTP errors, you may want to break or handle differently
            print(f"An error occurred: {e}")
            break

    print("Max retries reached, moving on to the next job title.")
    return None


def get_jobs(title):
    jobs = scrape_jobs(
        site_name=["indeed", "zip_recruiter", "glassdoor", "linkedin"],
        distance=20,
        job_type="fulltime",
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


def remove_duplicates_by_similarity(df, similarity_threshold=0.9):
    # Fill NaN values with empty strings to ensure all data is string type
    df = df.fillna("")

    # Combine the relevant columns into a single text column for comparison
    combined_text = df['title'] + " " + df['company'] + " " + df['description']

    # Use TF-IDF to vectorize the combined text
    vectorizer = TfidfVectorizer().fit_transform(combined_text)

    # Compute cosine similarity matrix
    cosine_sim = cosine_similarity(vectorizer, vectorizer)

    # Find indices to drop (where similarity is above the threshold, excluding self-comparison)
    to_drop = np.where(
        (cosine_sim > similarity_threshold).astype(int) &
        (np.ones_like(cosine_sim) - np.eye(len(cosine_sim), dtype=bool)).astype(int)
    )

    # Unique job indices to keep (inverting the logic to keep the first occurrence and remove subsequent similar ones)
    indices_to_keep = np.setdiff1d(np.arange(len(df)), np.unique(to_drop[0]))

    # Return DataFrame without duplicates
    return df.iloc[indices_to_keep]


def populate_jobs_dataframe_from_web():
    job_list = ['CAM Engineer', 'CAM Technician', 'CNC Programmer', 'Manufacturing Engineer',
                'Process Control Engineer']
    all_jobs = pd.DataFrame()  # Initialize an empty DataFrame to hold all jobs

    for job in job_list:
        job_df = get_jobs_with_backoff(job)
        # time.sleep(20)
        # job_df = get_linkedin_jobs(job)
        if not job_df.empty:
            all_jobs = pd.concat([all_jobs, job_df], ignore_index=True)

    print(f"Found {len(all_jobs)} jobs")
    all_jobs_no_duplicates = remove_duplicates_by_url(all_jobs, 'job_url')
    print(f"Removed duplicates by URL, now we have {len(all_jobs_no_duplicates)} jobs")

    return all_jobs_no_duplicates


def ask_claude_about_job(question, job_description=None, include_resume=False):
    load_dotenv()
    anthropic_api_key = os.environ.get("ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(
        api_key=anthropic_api_key,
    )

    full_message = ''
    if include_resume:
        full_message += "Here is Jonathan's resume, below\n"
        with open("jh-resume.txt", "r") as file:
            resume = file.read()
            full_message += resume + "\n\n"

    if job_description:
        full_message += "Here is some information about a job\n\n" + job_description + "\n\n"

    full_message += "Now for my question: \n" + question

    model = "claude-3-haiku-20240307"
    # model = "claude-3-sonnet-20240229"
    message = client.messages.create(
        model=model,
        max_tokens=1000,
        temperature=0.0,
        system="You are a helpful assistant, specializing in finding the right job for a candidate",
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

        questions = ("Does the job mention any of the following:  CAD programming, CAM programming, PLC control "
                     "systems, or CNC programming?  Give a simple YES or NO answer.")

        answers = ask_claude_about_job(questions, job_description, False)
        # Split the text into lines and remove the first line (which is empty)
        answer = answers[0].text

        # add the above 4 to the dataframe
        jobs_df.at[index, 'mentions_relevant_skill'] = answer
        # time.sleep(2)

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

    if not all_jobs.empty:
        print(f"Found {len(all_jobs)} jobs")
        unsimilar = remove_duplicates_by_similarity(all_jobs, 0.9)
        print(f"Removed duplicates, now we have {len(unsimilar)} jobs")

        if generate_derived_data:
            print("Generating derived data...")
            save_to_file(unsimilar, "compiled_jobs_no_derived")  # in case deriving fails
            derived_jobs = add_derived_data(unsimilar)
            sorted_jobs = sort_jobs(derived_jobs, ['company', 'title', 'location'], [True, True, True])
        else:
            sorted_jobs = sort_jobs(unsimilar, ['company', 'title', 'location'], [True, True, True])

        return sorted_jobs
    else:
        print("No jobs found.")
