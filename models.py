# models.py
from pydantic import BaseModel


class JobAssessment(BaseModel):
    # Desire assessment
    title_matches_preferred: bool
    has_desired_skills: bool
    free_from_stop_words: bool
    logical_career_step: bool

    # Experience level fit
    within_experience_range: bool
    seniority_matches: bool
    responsibilities_align: bool
    level_appropriate: bool

    # Skill requirements
    has_required_technical_skills: bool
    has_required_domain_skills: bool
    meets_education_requirements: bool
    has_industry_experience: bool

    # Experience requirements
    meets_years_required: bool
    has_similar_role_history: bool
    shows_skill_growth: bool
    has_similar_environment: bool

    # Guidance fields
    desire_reason: str
    requirements_reason: str
    guidance_text: str
