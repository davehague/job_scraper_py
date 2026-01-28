import time

from typing import List
from models import JobAssessment
from llm_config import get_openrouter_client, MODEL_FAST, MODEL_STRUCTURED

system_message = ("You are a helpful assistant, highly skilled in ruthlessly distilling down information from job "
                  "descriptions, and answering questions about job descriptions in a concise and targeted manner.")


def query_llm(model_name, system, messages=[], **kwargs):
    max_retries = 3
    wait_time = 3

    # Build messages list without mutating the caller's list
    messages_with_system = [{"role": "system", "content": system}] + messages

    for attempt in range(max_retries):
        try:
            client = get_openrouter_client()
            completion = client.chat.completions.create(
                messages=messages_with_system,
                max_tokens=256,
                model=model_name,
                temperature=1.0
            )
            return completion.choices[0].message.content

        except Exception as e:
            print(
                f"An unexpected error occurred: {e}. Attempt {attempt + 1} of {max_retries}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
            wait_time *= 2  # Exponential backoff
            if attempt == max_retries - 1:
                print(f"Failed after {max_retries} attempts.")
                return None

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


def ask_chatgpt_about_job(question, job_description, resume=None):
    client = get_openrouter_client()

    full_message = build_context_for_llm(job_description, resume, question)

    max_retries = 5
    wait_time = 5

    for attempt in range(max_retries):
        try:
            completion = client.chat.completions.create(
                messages=[
                    {"role": "system", "content": system_message + "\nOnly return text, not markdown or HTML."},
                    {"role": "user", "content": full_message}
                ],
                model=MODEL_FAST,
            )

            return completion.choices[0].message.content

        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            time.sleep(wait_time)
            wait_time *= 2

    print("Failed to get a response after multiple retries.")
    return None


def create_evaluation_prompt(job_title: str, job_description: str, resume: str,
                             job_titles: List[str], skill_words: List[str],
                             stop_words: List[str]) -> str:
    questions = {
        # Desire assessment questions
        "title_matches_preferred": f"Is the job title '{job_title}' similar to any of these preferred titles: {', '.join(job_titles)}?",
        "has_desired_skills": f"Does the job description prominently mention multiple of these desired skills: {', '.join(skill_words)}?",
        "free_from_stop_words": f"Is the job description free from these unwanted terms: {', '.join(stop_words)}?",
        "logical_career_step": "Based on the resume, does this job represent a logical next step in the candidate's career progression?",

        # Experience level questions
        "within_experience_range": "Comparing the job description's required years of experience to the resume, is the candidate within 2 years of the requirement (either direction)?",
        "seniority_matches": f"Looking at the skills {', '.join(skill_words)} from the candidate's preferences, does the role's seniority level match the candidate's expertise with these skills?",
        "responsibilities_align": "Are the role's responsibilities aligned with the candidate's current experience level shown in their resume?",
        "level_appropriate": f"Given the candidate's preferred job titles ({', '.join(job_titles)}), would this role's level be appropriate?",

        # Skill requirements questions
        "has_required_technical_skills": "Does the candidate's resume show experience with the technical skills required in the job description?",
        "has_required_domain_skills": f"Looking at the preferred skills ({', '.join(skill_words)}), does the candidate demonstrate proficiency in these areas?",
        "meets_education_requirements": "Does the candidate meet the educational requirements mentioned in the job description?",
        "has_industry_experience": f"Given the candidate's target roles ({', '.join(job_titles)}), does their industry experience align with this position?",

        # Experience requirements questions
        "meets_years_required": "Based on the resume, does the candidate meet the minimum years of experience specified in the job description?",
        "has_similar_role_history": f"Has the candidate performed roles similar to {', '.join(job_titles)} before?",
        "shows_skill_growth": f"Looking at the preferred skills ({', '.join(skill_words)}), does the candidate's experience show growth and increasing expertise in these areas?",
        "has_similar_environment": "Does the candidate's work history demonstrate success in similar company environments?"
    }

    prompt = f"""Evaluate this job opportunity for the candidate by answering YES or NO to each question.
       Be direct and definitive in your assessment.

       Job Details:
       Title: {job_title}
       Description: {job_description}

       Candidate Information:
       Resume: {resume}

       Please answer each of the following questions with YES or NO, and then provide guidance text:

       {chr(10).join(f"{key}: {question}" for key, question in questions.items())}

       Also provide guidance text for the candidate with these components:
       1. desire_reason: A single sentence explaining why they would like or dislike this job based on their preferences
       2. requirements_reason: A single sentence explaining why they would be a good or poor fit based on requirements
       3. guidance_text: A complete guidance message following this EXACT format:
          'You may <like, be lukewarm on, or dislike> this job because of the following reasons: <desire_reason>. The hiring manager may think you would be a <good, reasonable, or bad> fit for this job because of <requirements_reason>. Overall, I think <synthesis of the match assessment>.'

       Answer with structured output matching EXACTLY the expected format.
       Each assessment answer must be YES or NO only.
       Guidance fields must be complete sentences.
       """

    return prompt


def evaluate_job_match(job_title: str, job_description: str, resume: str,
                       job_titles: list[str], skill_words: list[str],
                       stop_words: list[str]) -> JobAssessment:
    """
    Evaluates job match using structured outputs via OpenRouter.
    Returns a JobAssessment object with yes/no answers.
    """

    try:
        client = get_openrouter_client()

        prompt = create_evaluation_prompt(
            job_title=job_title,
            job_description=job_description,
            resume=resume,
            job_titles=job_titles,
            skill_words=skill_words,
            stop_words=stop_words
        )

        completion = client.chat.completions.create(
            model=MODEL_STRUCTURED,
            messages=[
                {
                    "role": "system",
                    "content": """You are a job evaluation assistant.
                    Analyze jobs and resumes carefully, providing YES/NO answers to each question.
                    Be decisive and clear in your assessments."""
                },
                {"role": "user", "content": prompt}
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "job_assessment",
                    "strict": True,
                    "schema": {
                        "type": "object",
                        "properties": {
                            "title_matches_preferred": {"type": "boolean"},
                            "has_desired_skills": {"type": "boolean"},
                            "free_from_stop_words": {"type": "boolean"},
                            "logical_career_step": {"type": "boolean"},
                            "within_experience_range": {"type": "boolean"},
                            "seniority_matches": {"type": "boolean"},
                            "responsibilities_align": {"type": "boolean"},
                            "level_appropriate": {"type": "boolean"},
                            "has_required_technical_skills": {"type": "boolean"},
                            "has_required_domain_skills": {"type": "boolean"},
                            "meets_education_requirements": {"type": "boolean"},
                            "has_industry_experience": {"type": "boolean"},
                            "meets_years_required": {"type": "boolean"},
                            "has_similar_role_history": {"type": "boolean"},
                            "shows_skill_growth": {"type": "boolean"},
                            "has_similar_environment": {"type": "boolean"},
                            "desire_reason": {"type": "string"},
                            "requirements_reason": {"type": "string"},
                            "guidance_text": {"type": "string"}
                        },
                        "required": [
                            "title_matches_preferred",
                            "has_desired_skills",
                            "free_from_stop_words",
                            "logical_career_step",
                            "within_experience_range",
                            "seniority_matches",
                            "responsibilities_align",
                            "level_appropriate",
                            "has_required_technical_skills",
                            "has_required_domain_skills",
                            "meets_education_requirements",
                            "has_industry_experience",
                            "meets_years_required",
                            "has_similar_role_history",
                            "shows_skill_growth",
                            "has_similar_environment",
                            "desire_reason",
                            "requirements_reason",
                            "guidance_text"
                        ],
                        "additionalProperties": False
                    }
                }
            }
        )

        # Parse the JSON response into our Pydantic model
        response_json = completion.choices[0].message.content
        return JobAssessment.model_validate_json(response_json)

    except Exception as e:
        print(f"Error evaluating job match: {str(e)}")
        # Return an assessment with all False values in case of error
        return JobAssessment(
            title_matches_preferred=False,
            has_desired_skills=False,
            free_from_stop_words=False,
            logical_career_step=False,
            within_experience_range=False,
            seniority_matches=False,
            responsibilities_align=False,
            level_appropriate=False,
            has_required_technical_skills=False,
            has_required_domain_skills=False,
            meets_education_requirements=False,
            has_industry_experience=False,
            meets_years_required=False,
            has_similar_role_history=False,
            shows_skill_growth=False,
            has_similar_environment=False,
            desire_reason="Unable to evaluate due to an error",
            requirements_reason="Unable to evaluate due to an error",
            guidance_text="Unable to generate guidance due to an error in processing this job."
        )
