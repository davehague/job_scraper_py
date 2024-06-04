CREATE OR REPLACE VIEW jobscraper.recent_high_score_jobs AS
SELECT
    uj.user_id,
    uj.score,
    uj.interested,
    j.*
FROM
    jobscraper.users_jobs uj
JOIN
    jobscraper.jobs j ON uj.job_id = j.id
WHERE
    (j.date_posted > CURRENT_DATE - interval '7 days' OR j.date_pulled > CURRENT_DATE - interval '7 days')
    AND uj.score::INT > 75;