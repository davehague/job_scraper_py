-- Functions
CREATE OR REPLACE FUNCTION jobscraper.is_admin()
RETURNS BOOLEAN AS $$
BEGIN
  RETURN (SELECT is_admin FROM jobscraper.users WHERE id = auth.uid());
EXCEPTION
  WHEN OTHERS THEN
    RETURN FALSE;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Users
CREATE POLICY user_access_policy ON jobscraper.users
FOR ALL
USING (
  auth.uid() = id OR jobscraper.is_admin()
);

CREATE POLICY user_update_policy ON jobscraper.users
FOR UPDATE
USING (
  auth.uid() = id OR jobscraper.is_admin()
)
WITH CHECK (
  (auth.uid() = id AND is_admin = (SELECT is_admin FROM jobscraper.users WHERE id = auth.uid()))
  OR
  jobscraper.is_admin()
);


CREATE POLICY user_insert_policy ON jobscraper.users
FOR INSERT
WITH CHECK (
  auth.uid() = id
  AND
  is_admin = FALSE
);

-- User_Configs
CREATE POLICY user_access_policy ON jobscraper.user_configs
FOR ALL
USING (
  auth.uid() = user_id OR jobscraper.is_admin()
);

-- Users_Jobs
CREATE POLICY users_jobs_select_policy ON jobscraper.users_jobs
FOR SELECT
USING (
  select auth.uid() = user_id
);

CREATE POLICY users_jobs_update_policy ON jobscraper.users_jobs
FOR UPDATE
using ((select auth.uid()) = user_id)
with check ((select auth.uid()) = user_id)
