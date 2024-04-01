# Job Scraper Application

## Overview
The Job Scraper is a Python-based application designed to automate the process of finding job listings from various online job boards including Indeed, ZipRecruiter, Glassdoor, and LinkedIn. It fetches job listings based on specified criteria, adds derived data such as skills or qualifications required, and saves the listings both as CSV files and as Markdown documents for easy viewing.

*Built using Python 3.11.2*

## Features
- **Scrape Job Listings:** Fetch job listings from multiple sources.
- **Derived Data:** Enhance listings with derived information like required skills.
- **Save Listings:** Save job listings to CSV and Markdown files.
- **Query Listings:** Use LLaMA and Anthropic's Claude model to query job data with natural language.

## Modules
- `file_utils.py`: Utilities for file operations, including reading from and saving to CSV files.
- `job_scraper.py`: Core functionality for scraping job listings and processing them.
- `query_with_llama.py`: Leveraging LLaMA for querying job listings with natural language.
- `send_jobs_to_documents.py`: Writing job listings to Markdown files for easy access and readability.
- `main.py`: The main entry point for the application, orchestrating the job scraping, data processing, and file operations.

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
```

Execute the main script to start the application:
`python main.py`

## Configuration
You can customize the job scraping process by editing parameters in job_scraper.py, such as job titles, locations, and the number of results.

