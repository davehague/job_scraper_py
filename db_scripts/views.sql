-- DROP VIEW jobscraper.recent_high_score_jobs
CREATE OR REPLACE VIEW jobscraper.recent_high_score_jobs AS
SELECT
    uj.user_id,
    uj.score,
    uj.desire_score,
    uj.experience_score,
    uj.meets_requirements_score,
    uj.meets_experience_score,
    uj.interested,
    uj.has_applied,
    uj.email_sent,
    uj.guidance,
    j.id,
    j.created_at,
    j.title,
    j.company,
    j.short_summary,
    j.hard_requirements,
    j.job_site,
    j.url,
    j.location,
    j.date_posted,
    j.comp_interval,
    j.comp_min,
    j.comp_max,
    j.comp_currency,
    j.emails,
    j.date_pulled
FROM
    jobscraper.users_jobs uj
JOIN
    jobscraper.jobs j ON uj.job_id = j.id
WHERE
    (j.date_posted > CURRENT_DATE - interval '7 days' OR j.date_pulled > CURRENT_DATE - interval '7 days')
    AND uj.score::INT >= 50;