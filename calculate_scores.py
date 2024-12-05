from typing import List
from helpers import consolidate_text
from llm import evaluate_job_match
from models import JobAssessment


def get_job_ratings(original_df, db_user, user_configs):
    jobs_df = original_df.copy()
    db_job_titles = [config['string_value'] for config in user_configs if config['key'] == 'job_titles']
    db_skill_words = [config['string_value'] for config in user_configs if config['key'] == 'skill_words']
    db_stop_words = [config['string_value'] for config in user_configs if config['key'] == 'stop_words']
    db_resume = db_user.get('resume')

    job_titles = db_job_titles or []
    skill_words = db_skill_words or []
    stop_words = db_stop_words or []

    resume = consolidate_text(db_resume)

    for index, row in jobs_df.iterrows():
        job_title = row.get('title', "N/A")
        job_description = row.get('description', "N/A")
        job_description = consolidate_text(job_description)

        # Check for stop words first to avoid unnecessary API calls
        if any(stop_word.lower() in job_title.lower() or
               stop_word.lower() in job_description.lower()
               for stop_word in stop_words):
            jobs_df.at[index, 'desire_score'] = 0
            jobs_df.at[index, 'experience_score'] = 0
            jobs_df.at[index, 'meets_requirements_score'] = 0
            jobs_df.at[index, 'meets_experience_score'] = 0
            jobs_df.at[index, 'job_score'] = 0
            continue

        # Get yes/no answers from LLM
        assessment = evaluate_job_match(
            job_title=job_title,
            job_description=job_description,
            resume=resume,
            job_titles=job_titles,
            skill_words=skill_words,
            stop_words=stop_words
        )

        # Calculate scores based on the yes/no responses
        desire_score = calculate_desire_score(assessment)
        experience_score = calculate_experience_score(assessment)
        requirements_score = calculate_requirements_score(assessment)
        experience_req_score = calculate_experience_requirements_score(assessment)

        overall_score = calculate_overall_score(
            desire_score,
            experience_score,
            requirements_score,
            experience_req_score
        )

        # Update DataFrame with scores
        jobs_df.at[index, 'desire_score'] = desire_score
        jobs_df.at[index, 'experience_score'] = experience_score
        jobs_df.at[index, 'meets_requirements_score'] = requirements_score
        jobs_df.at[index, 'meets_experience_score'] = experience_req_score
        jobs_df.at[index, 'job_score'] = overall_score

        print(f"{index}: Adding a rating to: {job_title} at {row.get('company', 'N/A')}: {overall_score}")

    return jobs_df


def create_evaluation_prompt(job_title: str, job_description: str, resume: str,
                             job_titles: List[str], skill_words: List[str],
                             stop_words: List[str]) -> str:
    return f"""Evaluate this job opportunity for the candidate by answering YES or NO to each question.
    Be direct and definitive in your assessment.

    Job Details:
    Title: {job_title}
    Description: {job_description}

    Candidate Information:
    Resume: {resume}
    Preferred Job Titles: {', '.join(job_titles)}
    Desired Skills: {', '.join(skill_words)}
    Terms to Avoid: {', '.join(stop_words)}

    Answer each question with only YES or NO in the structured output.
    """


def calculate_desire_score(assessment: JobAssessment) -> int:
    """Calculate desire score based on weighted criteria"""
    weights = {
        'title_matches_preferred': 3,
        'has_desired_skills': 3,
        'free_from_stop_words': 3,
        'logical_career_step': 2
    }

    score = 0
    total_weight = sum(weights.values())

    if assessment.title_matches_preferred:
        score += weights['title_matches_preferred']
    if assessment.has_desired_skills:
        score += weights['has_desired_skills']
    if assessment.free_from_stop_words:
        score += weights['free_from_stop_words']
    if assessment.logical_career_step:
        score += weights['logical_career_step']

    # Use integer division
    return (score * 100) // total_weight


# Similar changes for other calculation functions:
def calculate_experience_score(assessment: JobAssessment) -> int:
    weights = {
        'within_experience_range': 3,
        'seniority_matches': 3,
        'responsibilities_align': 2,
        'level_appropriate': 2
    }

    score = 0
    total_weight = sum(weights.values())

    if assessment.within_experience_range:
        score += weights['within_experience_range']
    if assessment.seniority_matches:
        score += weights['seniority_matches']
    if assessment.responsibilities_align:
        score += weights['responsibilities_align']
    if assessment.level_appropriate:
        score += weights['level_appropriate']

    return (score * 100) // total_weight


def calculate_requirements_score(assessment: JobAssessment) -> int:
    weights = {
        'has_required_technical_skills': 3,
        'has_required_domain_skills': 3,
        'meets_education_requirements': 2,
        'has_industry_experience': 2
    }

    score = 0
    total_weight = sum(weights.values())

    if assessment.has_required_technical_skills:
        score += weights['has_required_technical_skills']
    if assessment.has_required_domain_skills:
        score += weights['has_required_domain_skills']
    if assessment.meets_education_requirements:
        score += weights['meets_education_requirements']
    if assessment.has_industry_experience:
        score += weights['has_industry_experience']

    return (score * 100) // total_weight


def calculate_experience_requirements_score(assessment: JobAssessment) -> int:
    weights = {
        'meets_years_required': 3,
        'has_similar_role_history': 3,
        'shows_skill_growth': 2,
        'has_similar_environment': 2
    }

    score = 0
    total_weight = sum(weights.values())

    if assessment.meets_years_required:
        score += weights['meets_years_required']
    if assessment.has_similar_role_history:
        score += weights['has_similar_role_history']
    if assessment.shows_skill_growth:
        score += weights['shows_skill_growth']
    if assessment.has_similar_environment:
        score += weights['has_similar_environment']

    return (score * 100) // total_weight


def calculate_overall_score(desire_score: int, experience_score: int,
                            requirements_score: int, experience_req_score: int) -> int:
    weights = {
        'desire_score': 0.25,
        'experience_score': 0.25,
        'requirements_score': 0.25,
        'experience_req_score': 0.25
    }

    overall_score = int(
        desire_score * weights['desire_score'] +
        experience_score * weights['experience_score'] +
        requirements_score * weights['requirements_score'] +
        experience_req_score * weights['experience_req_score']
    )

    return overall_score
