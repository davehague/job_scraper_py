--DROP FUNCTION get_active_users_with_resume()
CREATE OR REPLACE FUNCTION jobscraper.get_active_users_with_resume()
RETURNS SETOF jobscraper.users as $$
BEGIN
    RETURN QUERY
    SELECT *
    FROM jobscraper.users
    WHERE jobscraper.users.resume IS NOT NULL
      AND jobscraper.users.resume <> ''
      AND jobscraper.users.last_login >= (CURRENT_TIMESTAMP - INTERVAL '30 days');
END;
$$ LANGUAGE plpgsql;

--select count(string_value) from (
--select distinct string_value from jobscraper.user_configs where key = 'job_titles'
--and user_id in (SELECT id FROM jobscraper.get_active_users_with_resume())
--) as unique_titles