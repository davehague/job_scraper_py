from helpers import consolidate_text
from llm import query_llm


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

        # Calculate each score using binary questions
        desire_score = calculate_desire_score(job_title, job_description, resume, job_titles, skill_words, stop_words)
        experience_level_score = calculate_experience_level_score(job_description, resume)
        meets_requirements_score = calculate_meets_requirements_score(job_description, resume)
        meets_experience_score = calculate_meets_experience_score(job_description, resume)

        # Calculate overall score
        overall_score = calculate_overall_score(
            desire_score,
            experience_level_score,
            meets_requirements_score,
            meets_experience_score
        )

        # Update DataFrame
        jobs_df.at[index, 'desire_score'] = desire_score
        jobs_df.at[index, 'experience_score'] = experience_level_score
        jobs_df.at[index, 'meets_requirements_score'] = meets_requirements_score
        jobs_df.at[index, 'meets_experience_score'] = meets_experience_score
        jobs_df.at[index, 'job_score'] = overall_score

        print(f"{index}: Adding a rating to: {job_title} at {row.get('company', 'N/A')}: {overall_score}")

    return jobs_df


def calculate_desire_score(job_title, job_description, resume, job_titles, skill_words, stop_words):
    """Calculate how well the job matches candidate's desires"""

    # Pre-check for stop words
    for stop_word in stop_words:
        if stop_word.lower() in job_title.lower() or stop_word.lower() in job_description.lower():
            return 0  # Immediate rejection if stop words are present

    questions = [
        {
            "text": f"Is the job title similar to any of these preferred titles: {', '.join(job_titles)}?",
            "weight": 3
        },
        {
            "text": f"Does the job description prominently mention multiple of these skills: {', '.join(skill_words)}?",
            "weight": 3
        },
        {
            "text": f"Is the job description free from these unwanted terms: {', '.join(stop_words)}?",
            "weight": 3
        },
        {
            "text": "Based on the resume, does this job represent a logical next step in the candidate's career progression?",
            "weight": 2
        }
    ]
    return ask_weighted_questions(questions, job_title, job_description, resume)


def calculate_experience_level_score(job_description, resume, skill_words, job_titles):
    """Calculate if the experience level is appropriate"""
    questions = [
        {
            "text": "Comparing the job description's required years of experience to the resume, is the candidate within 2 years of the requirement (either direction)?",
            "weight": 3
        },
        {
            "text": f"Looking at the skills {', '.join(skill_words)} from the candidate's preferences, does the role's seniority level match the candidate's expertise with these skills?",
            "weight": 3
        },
        {
            "text": "Are the role's responsibilities aligned with the candidate's current experience level shown in their resume?",
            "weight": 2
        },
        {
            "text": f"Given the candidate's preferred job titles ({', '.join(job_titles)}), would this role's level be appropriate?",
            "weight": 2
        }
    ]
    return ask_weighted_questions(questions, "", job_description, resume)


def calculate_meets_requirements_score(job_description, resume, skill_words, job_titles):
    """Calculate how well candidate meets skill requirements"""
    questions = [
        {
            "text": f"Looking at the preferred skills ({', '.join(skill_words)}), does the candidate demonstrate proficiency in these areas?",
            "weight": 3
        },
        {
            "text": "Does the candidate's resume show experience with the technical skills required in the job description?",
            "weight": 3
        },
        {
            "text": "Does the candidate meet the educational requirements mentioned in the job description?",
            "weight": 2
        },
        {
            "text": f"Given the candidate's target roles ({', '.join(job_titles)}), does their industry experience align with this position?",
            "weight": 2
        }
    ]
    return ask_weighted_questions(questions, "", job_description, resume)


def calculate_meets_experience_score(job_description, resume, job_titles, skill_words):
    """Calculate how well candidate meets experience requirements"""
    questions = [
        {
            "text": "Based on the resume, does the candidate meet the minimum years of experience specified in the job description?",
            "weight": 3
        },
        {
            "text": f"Has the candidate performed roles similar to {', '.join(job_titles)} before?",
            "weight": 3
        },
        {
            "text": f"Looking at the preferred skills ({', '.join(skill_words)}), does the candidate's experience show growth and increasing expertise in these areas?",
            "weight": 2
        },
        {
            "text": "Does the candidate's work history demonstrate success in similar company environments?",
            "weight": 2
        }
    ]
    return ask_weighted_questions(questions, "", job_description, resume)


def ask_weighted_questions(questions, job_title, job_description, resume, llm):
    """Ask questions and calculate weighted score"""
    total_weight = sum(q["weight"] for q in questions)
    current_score = 0

    for question in questions:
        prompt = f"""
        Based on the following job details and resume, please answer YES or NO:

        Job Title: {job_title}
        Job Description: {job_description}

        Resume: {resume}

        Question: {question['text']}

        Consider the question carefully and answer with ONLY 'YES' or 'NO'.
        """

        response = llm(prompt).strip().upper()
        if response == "YES":
            current_score += question["weight"]

    # Convert to 0-100 scale
    return round((current_score / total_weight) * 100)


def calculate_overall_score(desire_score, experience_level_score, meets_requirements_score, meets_experience_score):
    """Calculate overall score with weighted components"""
    weights = {
        'desire_score': 0.25,
        'experience_level_score': 0.25,
        'meets_requirements_score': 0.25,
        'meets_experience_score': 0.25
    }

    overall_score = (
            desire_score * weights['desire_score'] +
            experience_level_score * weights['experience_level_score'] +
            meets_requirements_score * weights['meets_requirements_score'] +
            meets_experience_score * weights['meets_experience_score']
    )

    return round(overall_score)
