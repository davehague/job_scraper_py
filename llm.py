import os
import time

import pandas as pd
from dotenv import load_dotenv

import anthropic
from openai import OpenAI
import google.generativeai as gemini

system_message = ("You are a helpful assistant, highly skilled in ruthlessly distilling down information from job "
                  "descriptions, and answering questions about job descriptions in a concise and targeted manner.")


def query_llm(llm, model_name, system, messages=[]):
    max_retries = 3
    wait_time = 3

    for attempt in range(max_retries):
        try:
            if llm == "openai":
                messages.insert(0, {"role": "system", "content": system})
                client = OpenAI(
                    api_key=os.environ.get("OPENAI_API_KEY"),
                )
                completion = client.chat.completions.create(
                    messages=messages,
                    max_tokens=256,
                    model=model_name,
                    temperature=1.0
                )
                return completion.choices[0].message.content
            elif llm == "anthropic":
                anthropic_api_key = os.environ.get("ANTHROPIC_API_KEY")
                client = anthropic.Anthropic(api_key=anthropic_api_key)
                message = client.messages.create(
                    model=model_name,
                    max_tokens=256,
                    temperature=1.0,
                    system=system,
                    messages=messages
                )
                return message.content[0].text
            elif llm == "gemini":
                safe = [
                    {
                        "category": "HARM_CATEGORY_HARASSMENT",
                        "threshold": "BLOCK_NONE",
                    },
                    {
                        "category": "HARM_CATEGORY_HATE_SPEECH",
                        "threshold": "BLOCK_NONE",
                    },
                    {
                        "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                        "threshold": "BLOCK_NONE",
                    },
                    {
                        "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                        "threshold": "BLOCK_NONE",
                    }
                ]

                gemini.configure(api_key=os.environ.get("GEMINI_API_KEY"))
                model = gemini.GenerativeModel(model_name=model_name, safety_settings=safe)  # 'gemini-1.5-flash'
                response = model.generate_content(system + " " + " ".join([msg["content"] for msg in messages]))
                return response.text
            else:
                return None

        except Exception as e:
            print(
                f"An unexpected error occurred: {e}. Attempt {attempt + 1} of {max_retries}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
            wait_time *= 2  # Exponential backoff
            if attempt == max_retries - 1:
                print(f"Failed after {max_retries} attempts.")
                return None

    return None


def add_derived_data(jobs_df, derived_data_questions=[], resume=None, llm="chatgpt"):
    if len(derived_data_questions) == 0:
        return jobs_df

    print("Generating derived data...")

    derived_data = pd.DataFrame(index=jobs_df.index)

    for index, row in jobs_df.iterrows():
        job_description = f"Title: {row.get('title', 'N/A')}\nCompany: {row.get('company', 'N/A')}\nLocation: {row.get('location', 'N/A')}\n" \
                          f"Description: {row.get('description', 'N/A')}\n"

        pay_info = (
            f"Pays between {row.get('min_amount', 'N/A')} and {row.get('max_amount', 'N/A')} on a(n) {row.get('interval', 'N/A')}'"
            f" basis.") if row.get('interval', '') else ""

        job_description += pay_info

        print(f"{index}: Processing: {row.get('title', 'N/A')} at {row.get('company', 'N/A')}")

        for column_name, question in derived_data_questions:
            if llm == "chatgpt":
                answer = ask_chatgpt_about_job(question, job_description, resume)

            if answer is None:
                print(f"Failed to get a response from the LLM, breaking out of loop.")
                break

            derived_data.at[index, column_name] = answer

        # time.sleep(2)  # In case Anthropic is having an issue
    jobs_df_updated = pd.concat([derived_data, jobs_df], axis=1)
    return jobs_df_updated


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
            )

            return completion.choices[0].message.content

        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            time.sleep(wait_time)
            wait_time *= 2

    print("Failed to get a response after multiple retries.")
    return None
