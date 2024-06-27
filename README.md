# Job Scraper Application

## Overview

This is a Python-based application that pulls job listings from various online job boards. It fetches job
listings based on specified criteria, adds derived data such as skills or qualifications required, and saves
the listings to Supabase. From there a [web frontend](https://github.com/davehague/job_scraper_web) can
display the listings.

*Built using Python 3.11.2*

## Getting Started

To run the Job Scraper, ensure you have Python 3.11 installed and set up on your system.

Next, set up a virtual environment in the project directory:

```bash
# Confirm you're using the correct version of Python
python --version

# Install virtualenv if you don't have it, then create the venv
pip install virtualenv
python -m venv venv

# Activate the virtual environment
venv\Scripts\activate

# CD to your project directory
cd path\to\job-scraper

# Install the required dependencies:
pip install -r requirements.txt
```

Create a `.env` file to store your LLM keys

```bash
ANTHROPIC_API_KEY=sk-your-key-here
GEMINI_API_KEY=AIz-your-key-here
OPENAI_API_KEY=sk-your-key-here
SUPABASE_URL=https://<project_url>.supabase.co
# Supabase Service Account (will bypass RLS)
SUPABASE_KEY=eyJhb-your-key-here
# Mailjet keys
MJ_APIKEY_PUBLIC=dd2-your-key-here
MJ_APIKEY_PRIVATE=e3e-your-key-here
```

## Database

Schemas, tables, and views are created in Supabase. You can view the DDL under `db_scripts`

## Configuration

Configuration is controlled via database settings. The table is called `user_configs`. This is a sparsely
populated table that can contain the following keys:

| key                    | description                                                     |
|------------------------|-----------------------------------------------------------------|
| job_titles             | Comma-separated list of job titles to search for                |
| skill_words            | Comma-separated list of skills to search for                    |
| go_words               | Comma-separated list of words to search for in job descriptions |
| is_remote              | Boolean indicating whether to search for remote jobs            |
| distance               | Integer indicating the distance from the location to search     |
| candidate_requirements | Comma-separated list of words to search for in job descriptions |
| stop_words             | Comma-separated list of words to exclude from job titles        |
| results_wanted         | Integer indicating the number of results to return              |
| location               | String indicating the location to search                        |
| candidate_min_salary   | Integer indicating the minimum salary to search for             |

## Dev Guidance

Code formatting:  Use Pycharm's built-in formatter to ensure consistent code style. Configure it by going
to `File > Settings > Tools > Actions on Save` and checking `Reformat code`.

## Scheduling (Windows)

1. Set the environment variable `JOB_SCRAPER_PROJECT_DIR` to the downloaded location of this project.
2. Create a new task in Task Scheduler. Set Actions -> Program/Script to the `.bat` file in the root of this project.