from llm import query_llm
from helpers import consolidate_text


def job_matches_stop_words(user_configs, job):
    db_stop_words = [config['string_value'] for config in user_configs if config['key'] == 'stop_words']
    job_title = job.get('title')
    job_description = job.get('description')

    stop_words = db_stop_words or []

    job_title = consolidate_text(job_title)
    job_description = consolidate_text(job_description)

    for stop_word in stop_words:
        if stop_word in job_title or stop_word in job_description:
            return True

    return False


def job_meets_salary_requirements(user, job):
    candidate_min_salary = user.get('min_salary')
    job_max_salary = job.get('max_amount') or 0
    return job_max_salary >= candidate_min_salary


def get_job_guidance_for_user(db_user, user_configs, job):
    job_description = job.get('description')
    job_title = job.get('title')

    db_job_titles = [config['string_value'] for config in user_configs if config['key'] == 'job_titles']
    db_skill_words = [config['string_value'] for config in user_configs if config['key'] == 'skill_words']
    db_stop_words = [config['string_value'] for config in user_configs if config['key'] == 'stop_words']
    db_resume = db_user.get('resume')

    job_titles = db_job_titles or []
    skill_words = db_skill_words or []
    stop_words = db_stop_words or []

    resume = consolidate_text(db_resume)

    job_description = consolidate_text(job_description)
    full_message = f"<job_titles>{', '.join(job_titles)}</job_titles>\n" + \
                   f"<desired_words>{', '.join(skill_words)}</desired_words>\n" + \
                   f"<undesirable_words>{', '.join(stop_words)}</undesirable_words>\n" + \
                   f"<resume>{resume}</resume>\n" + \
                   f"<job_title>{job_title}</job_title>\n" + \
                   f"<job_description>{job_description}</job_description>\n" + \
                   """
                   Given the job titles (job_titles tag), desired words (desired_words tag), undesired words 
                   (undesirable_words tag), resume (resume tag), job title (job_title tag) and job description 
                   (job_description tag), make the following ratings:

                   1) How the candidate would rate this job on a scale from 1 to 100 in terms of how well it 
                   matches their experience and the type of job they desire.
                   2) How the candidate would rate this job on a scale from 1 to 100 as a match for their 
                   experience level (they aren't underqualified or overqualified).
                   3) How a hiring manager for this job would rate the candidate on a scale from 1 to 100 on how 
                   well the candidate meets the skill requirements for this job.
                   4) How a hiring manager for this job would rate the candidate on a scale from 1 to 100 on how 
                   well the candidate meets the experience requirements for this job.
                   5) Consider the results from steps 1 through 5 then give a final assessment from 1 to 100,
                   where 1 is very little chance of this being a good match for the candidate and hiring manager, 
                   and 100 being a perfect match where the candidate will have a great chance to succeed in 
                   this role.

                   For experience level, look for cues in the jobs description that list years of experience, 
                   then compare that to the level of experience you believe the candidate to have (make an 
                   assessment based on year in directly applicable fields of work).

                   Start your answer immediately with a bulleted list as shown in the example below. Always include 
                   the left side prefix from the template below in your answer (including for the explanation). 
                   Address the candidate directly, closely following the template set in the example. NN should be 
                   replaced in the template with a 2 digit number each time.
                   """
    full_message = consolidate_text(full_message)
    full_message += \
        """
        - Candidate desire match: NN
        - Candidate experience match: NN
        - Hiring manager skill match: NN
        - Hiring manager experience match: NN
        - Final overall match assessment: NN
        - Explanation of ratings: 
        You may <like, be lukewarm on, or dislike> this job because of the following reasons: <reasons in one sentence>. The hiring manager may think you would be a <good, reasonable, or bad> fit for this job because of <reasons, in one sentence>. Overall, I think <your overall thoughts about the match between the user and the job in one sentence>.
        """

    ratings = query_llm(llm="gemini",
                        model_name="gemini-1.5-flash",
                        system="You are a helpful no-nonsense assistant. You listen to directions carefully and follow them to the letter.",
                        messages=[{"role": "user", "content": full_message}])

    if ratings is None:
        print("LLM failed to generate ratings.")
        return None

    print(f"Ratings for job: {ratings}")

    guidance_split = ratings.split("Explanation of ratings:")
    if len(guidance_split) == 2:
        guidance = guidance_split[1].strip()
    else:
        guidance = ""

    ratings = ratings.split("\n")

    if len(ratings) >= 6:
        desire_score_split = ratings[0].split(":")
        if len(desire_score_split) == 2:
            desire_score = desire_score_split[1].strip()
        else:
            print("Error: Unable to split desire score.")

        experience_score_split = ratings[1].split(":")
        if len(experience_score_split) == 2:
            experience_score = experience_score_split[1].strip()
        else:
            print("Error: Unable to split experience score.")

        meets_requirements_score_split = ratings[2].split(":")
        if len(meets_requirements_score_split) == 2:
            meets_requirements_score = meets_requirements_score_split[1].strip()
        else:
            print("Error: Unable to split meets requirements score.")

        meets_experience_score_split = ratings[3].split(":")
        if len(meets_experience_score_split) == 2:
            meets_experience_score = meets_experience_score_split[1].strip()
        else:
            print("Error: Unable to split meets experience score.")

        overall_job_score_split = ratings[4].split(":")
        if len(overall_job_score_split) == 2:
            overall_job_score = overall_job_score_split[1].strip()
        else:
            print("Error: Unable to split overall job score.")
    else:
        print("Error: Ratings list does not have enough elements.")

    score_dict = {
        'desire_score': desire_score,
        'experience_score': experience_score,
        'meets_requirements_score': meets_requirements_score,
        'meets_experience_score': meets_experience_score,
        'overall_score': overall_job_score,
        'guidance': guidance
    }

    return score_dict


def find_best_job_titles_for_user(user, user_configs):
    user_id = user.get('id')
    db_job_titles = [config['string_value'] for config in user_configs if config['key'] == 'job_titles']
    db_skill_words = [config['string_value'] for config in user_configs if config['key'] == 'skill_words']
    db_stop_words = [config['string_value'] for config in user_configs if config['key'] == 'stop_words']
    db_resume = user.get('resume')

    job_titles = db_job_titles or []
    skill_words = db_skill_words or []

    full_message = "Below is information that the candidate has provided.\n"
    full_message += "Provided Job Titles: " + ", ".join(job_titles) + "\n"
    full_message += "Desired verbiage in job description: " + ", ".join(skill_words) + "\n"

    if db_stop_words and len(db_stop_words) > 0:
        full_message += ("Candidate does not want jobs that have titles with these words: " +
                         ", ".join(db_stop_words) + "\n")

    if db_resume is not None:
        db_resume = consolidate_text(db_resume)
        full_message += "In the <resume> tag below is the candidate resume, give extra weight to this information."
        full_message += "\n<resume>\n" + db_resume + "\n</resume>\n"

    if not db_job_titles:
        print("No job titles found in the database, using LLM to find job titles.")
        titles = query_llm(llm="anthropic",
                           model_name="claude-3-opus-20240229",
                           system="You are an expert in searching job listings. You take all the information"
                                  " given to you and come up with a list of 3 most relevant job titles. You do not"
                                  " have to use the job titles provided by the candidate, but take them into"
                                  " consideration.  Only list the titles in a comma-separated list, "
                                  " no other information is needed.  IMPORTANT: ONLY INCLUDE THE JOB TITLES IN "
                                  " A COMMA SEPARATED LIST.  DO NOT INCLUDE ANY OTHER INFORMATION.",
                           messages=[{"role": "user", "content": full_message}])
        if titles is None:  # Fall back if LLM failed
            titles = []
    else:
        titles = db_job_titles

    return titles
