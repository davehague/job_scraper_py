-- SCHEMA: jobscraper

-- DROP SCHEMA IF EXISTS jobscraper ;

CREATE SCHEMA IF NOT EXISTS jobscraper
    AUTHORIZATION postgres;

GRANT USAGE ON SCHEMA jobscraper TO anon;

GRANT USAGE ON SCHEMA jobscraper TO authenticated;

GRANT ALL ON SCHEMA jobscraper TO postgres;

GRANT USAGE ON SCHEMA jobscraper TO service_role;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA jobscraper
GRANT ALL ON TABLES TO anon;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA jobscraper
GRANT ALL ON TABLES TO authenticated;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA jobscraper
GRANT ALL ON TABLES TO service_role;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA jobscraper
GRANT ALL ON SEQUENCES TO anon;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA jobscraper
GRANT ALL ON SEQUENCES TO authenticated;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA jobscraper
GRANT ALL ON SEQUENCES TO service_role;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA jobscraper
GRANT EXECUTE ON FUNCTIONS TO anon;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA jobscraper
GRANT EXECUTE ON FUNCTIONS TO authenticated;

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA jobscraper
GRANT EXECUTE ON FUNCTIONS TO service_role;