import os
import time
from file_utils import save_df_to_downloads
from dotenv import load_dotenv
from requests.exceptions import HTTPError

from jobspy import scrape_jobs  # python-jobspy package
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

import anthropic
from openai import OpenAI

system_message = ("You are a helpful assistant, highly skilled in ruthlessly distilling down information from job "
                  "descriptions, and answering questions about job descriptions in a concise and targeted manner.")


def scrape_job_data(role_id, job_titles, job_sites, location, distance, results_wanted, hours_old, is_remote):
    all_jobs = pd.DataFrame()
    for job_title in job_titles:
        job_df = get_jobs_with_backoff(role_id, job_title, job_sites, location, distance, results_wanted, hours_old,
                                       is_remote)

        if job_df is None:  # Something happened with pulling the jobs (e.g. max retries reached)
            continue

        if not job_df.empty:
            all_jobs = pd.concat([all_jobs, job_df], ignore_index=True)

    print('Job scraping completed, saving temporary file (scraped_jobs)...')
    save_df_to_downloads(all_jobs, "scraped_jobs")
    return all_jobs


def get_jobs_with_backoff(role_id, job_title, job_sites, location, distance, results_wanted, hours_old, is_remote,
                          max_retries=5, initial_wait=5):
    attempt = 0
    wait_time = initial_wait

    while attempt < max_retries:
        try:
            jobs_df = scrape_jobs(
                site_name=job_sites,
                location=location,
                distance=distance,
                is_remote=is_remote,
                job_type="fulltime",
                linkedin_fetch_description=True,
                search_term=job_title,
                results_wanted=results_wanted,
                hours_old=hours_old,  # (only Linkedin/Indeed is hour specific, others round up to days old)
                country_indeed='USA'  # only needed for indeed / glassdoor
            )

            if jobs_df is None:
                raise ValueError("scrape_jobs returned None dataframe")

            jobs_df['searched_title'] = job_title  # Add a column to indicate the job title
            jobs_df['role_id'] = role_id  # Add a column to indicate the role ID
            jobs_df = jobs_df.dropna(axis=1, how='all') if not jobs_df.empty else pd.DataFrame()
            jobs_df = jobs_df.fillna("").infer_objects(copy=False)

            return jobs_df

        except Exception as e:
            print(f"An error occurred: {e}")
            print(f"Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
            wait_time *= 2  # Exponential backoff
            attempt += 1

    print("Max retries reached, moving on to the next job title.")
    return None


def sort_job_data(all_jobs, sort_columns, ascending_orders):
    if all_jobs.empty:
        print("No jobs found.")
        return all_jobs

    return all_jobs.sort_values(by=sort_columns, ascending=ascending_orders)


def reorder_columns(df):
    # Put title and company first, then everything else
    columns = ['title', 'company']
    columns.extend([col for col in df.columns if col not in columns])
    return df[columns]


def remove_duplicates_by_url(df, column_name='job_url'):
    if df.empty:
        print("DataFrame is empty. No duplicates to remove.")
        return df
    else:
        # Keep the first occurrence of each unique value in the specified column
        return df.drop_duplicates(subset=[column_name], keep='first')


def remove_duplicates_by_similarity(df, similarity_threshold=0.9):
    if df.empty:
        print("DataFrame is empty. Nothing to de-duplicate.")
        return df

    df = df.fillna("")
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

    return df.iloc[indices_to_keep]


def remove_titles_matching_stop_words(df, stop_words):
    if df.empty:
        print("DataFrame is empty. No stop words to remove.")
        return df

    for stop_word in stop_words:
        df = df[~df['title'].str.contains(stop_word, case=False)]

    return df


def remove_titles_not_matching_go_words(df, go_words):
    def contains_go_word(title):
        return any(word in title for word in go_words)

    filtered_df = df[df['title'].apply(contains_go_word)].copy()  # Use .copy() to avoid SettingWithCopyWarning

    return filtered_df


def ask_chatgpt_about_job(question, job_description, resume=None):
    load_dotenv()

    client = OpenAI(
        api_key=os.environ.get("OPENAI_API_KEY"),
    )

    full_message = build_context_for_llm(job_description, resume, question)

    model = "gpt-3.5-turbo"
    max_retries = 5
    wait_time = 5

    for attempt in range(max_retries):
        try:
            completion = client.chat.completions.create(
                messages=[
                    {"role": "system", "content": system_message + "\nOnly return text, not markdown or HTML."},
                    {"role": "user", "content": full_message}
                ],
                model=model,
                temperature=0.0,
                max_tokens=150
            )

            return completion.choices[0].message.content

        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            time.sleep(wait_time)
            wait_time *= 2

    print("Failed to get a response after multiple retries.")
    return None


def ask_claude_about_job(question, job_description=None, resume=None):
    load_dotenv()
    anthropic_api_key = os.environ.get("ANTHROPIC_API_KEY")
    client = anthropic.Anthropic(api_key=anthropic_api_key)

    full_message = build_context_for_llm(job_description, resume, question)

    model = "claude-3-haiku-20240307"
    max_retries = 5
    wait_time = 5

    for attempt in range(max_retries):
        try:
            message = client.messages.create(
                model=model,
                max_tokens=500,
                temperature=0.0,
                system=system_message,
                messages=[
                    {"role": "user", "content": full_message}
                ]
            )
            return message.content[0].text
        except anthropic.RateLimitError:
            print(f"Rate limit exceeded, retrying in {wait_time} seconds...")
            time.sleep(wait_time)
            wait_time *= 2
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            break

    print("Failed to get a response after multiple retries.")
    return None


def build_context_for_llm(job_description, resume, question):
    """Build the full message to send to the API."""
    full_message = ''
    if resume is not None:
        full_message += "Here is the candidate's resume, below\n"
        full_message += resume + "\n\n"
    if job_description:
        full_message += ("Here is some information about a job.  I'll mark the job start and end with 3 equals signs ("
                         "===) \n===\n") + job_description + "\n===\n"
    full_message += "Now for my question: \n" + question
    return full_message


def add_derived_data(jobs_df, derived_data_questions=[], resume=None, llm="claude"):
    if len(derived_data_questions) == 0:
        return jobs_df

    print("Generating derived data...")
    save_df_to_downloads(jobs_df, "compiled_jobs_no_derived")  # in case deriving fails

    derived_data = pd.DataFrame(index=jobs_df.index)

    for index, row in jobs_df.iterrows():
        job_description = f"Title: {row['title']}\nCompany: {row['company']}\nLocation: {row['location']}\n" \
                          f"Description: {row['description']}\n"

        pay_info = (f"Pays between {row['min_amount']} and {row['max_amount']} on a(n) {row['interval']}'"
                    f" basis.") if len(row['interval']) > 0 else ""

        job_description += pay_info

        print(f"{index}: Processing: {row['title']} at {row['company']}")

        for column_name, question in derived_data_questions:
            if llm == "chatgpt":
                answer = ask_chatgpt_about_job(question, job_description, resume)
            elif llm == "claude":
                answer = ask_claude_about_job(question, job_description, resume)

            if answer is None:
                print(f"Failed to get a response from the LLM, breaking out of loop.")
                break

            derived_data.at[index, column_name] = answer

        # time.sleep(2)  # In case Anthropic is having an issue
    jobs_df_updated = pd.concat([derived_data, jobs_df], axis=1)
    return jobs_df_updated


# TODO:  Only keep new jobs (keep a running tally somewhere)
def get_new_rows(df1, df2):
    # Merge the two DataFrames, keeping all rows from both
    merged = pd.merge(df1, df2, on='job_url', how='outer', indicator=True)

    # Filter the merged DataFrame to only include rows that only appear in the second DataFrame
    new_rows = merged[merged['_merge'] == 'right_only']

    # Drop the _merge column as it's no longer needed
    new_rows = new_rows.drop(columns='_merge')

    return new_rows


def clean_and_deduplicate_jobs(all_jobs, recent_job_urls, stop_words, go_words, skill_words, job_titles,
                               candidate_min_salary,
                               similarity_threshold=0.9):
    if all_jobs.empty:
        print("No jobs found.")
        return all_jobs
    else:
        print(f"Cleaning {len(all_jobs)} jobs")

    # remove all jobs that are already in the database (based on URL)
    all_jobs = all_jobs[~all_jobs['job_url'].isin(recent_job_urls)]
    print(f"Removed jobs already in the database, now we have {len(all_jobs)} jobs")

    all_jobs_cols_removed = remove_extraneous_columns(all_jobs)

    long_desc_jobs = all_jobs_cols_removed[all_jobs_cols_removed['description'].str.len() >= 1000]
    print(f"Removed jobs with short descriptions, now we have {len(long_desc_jobs)} jobs")

    deduped_by_url = remove_duplicates_by_url(long_desc_jobs, 'job_url')
    print(f"Removed duplicates by URL, now we have {len(deduped_by_url)} jobs")
    # save_df_to_downloads(deduped_by_url, "deduped_by_url")

    unsimilar = remove_duplicates_by_similarity(deduped_by_url, similarity_threshold)
    print(f"Removed duplicates by similarity, now we have {len(unsimilar)} jobs")
    # save_df_to_downloads(unsimilar, "unsimilar")

    stop_words_removed = remove_titles_matching_stop_words(unsimilar, stop_words)
    print(f"Removed titles matching stop words, now we have {len(stop_words_removed)} jobs")
    # save_df_to_downloads(stop_words_removed, "stop_words_removed")

    non_go_words_removed = stop_words_removed if go_words == [] else remove_titles_not_matching_go_words(
        stop_words_removed, go_words)
    print(f"Removed titles not matching go words, now we have {len(non_go_words_removed)} jobs")

    # Remove all jobs where the max_amount column is less than candidate_min_salary (leave the row if max_amount is NaN)
    if 'max_amount' in non_go_words_removed.columns:
        if (non_go_words_removed.empty) or (non_go_words_removed['max_amount'].isnull().all()):
            print("No salary information available, skipping salary check.")
            return non_go_words_removed

        non_go_words_removed.loc[:, 'max_amount'] = pd.to_numeric(non_go_words_removed['max_amount'], errors='coerce')
        min_salary_removed = non_go_words_removed.loc[
            non_go_words_removed['max_amount'].isnull() |
            (non_go_words_removed['max_amount'] >= candidate_min_salary)]

        print(f"Removed jobs with max_amount less than min_salary, now we have {len(min_salary_removed)} jobs")

        return min_salary_removed
    else:
        return stop_words_removed


def remove_extraneous_columns(df):
    columns_to_keep = ['site', 'job_url', 'job_url_direct', 'title', 'company', 'location', 'job_type', 'date_posted',
                       'interval', 'min_amount', 'max_amount', 'currency', 'is_remote', 'emails', 'description',
                       'searched_title', 'role_id']
    columns_to_drop = [col for col in df.columns if col not in columns_to_keep]
    return df.drop(columns=columns_to_drop)
