# Job Scraper Application

## Overview

The Job Scraper is a Python-based application designed to automate the process of finding job listings from various
online job boards including Indeed, ZipRecruiter, Glassdoor, and LinkedIn. It fetches job listings based on specified
criteria, adds derived data such as skills or qualifications required, and saves the listings both as CSV files and as
Markdown documents for easy viewing.

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
OPENAI_API_KEY=sk-your-key-here
ANTHROPIC_API_KEY=sk-your-key-here
SUPABASE_URL=https://<project_url>.supabase.co
SUPABASE_KEY=eyJhbGci...
```

## Configuration

A number of config files can be found in the `mock_configs` directory. This makes it easier to swap out different
testing scenarios.

## Dev Guidance

Code formatting:  Use Pycharm's built-in formatter to ensure consistent code style. Configure it by going
to `File > Settings > Tools > Actions on Save` and checking `Reformat code`.
