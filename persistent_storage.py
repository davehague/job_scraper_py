def save_jobs_to_supabase(df):
    import os
    from pathlib import Path
    from dotenv import load_dotenv
    from supabase import create_client, Client
    from supabase.lib.client_options import ClientOptions
    import pandas as pd

    # Load environment variables
    env_path = Path('.') / '.env'
    load_dotenv(dotenv_path=env_path)

    # Get Supabase URL and key from environment variables
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')

    # Initialize Supabase client
    opts = ClientOptions().replace(schema="jobscraper")
    supabase: Client = create_client(supabase_url, supabase_key, options=opts)

    for index, row in df.iterrows():
        new_job = {
            'title': row['title'],
            'company': row['company'],
            'short_summary': row['short_summary'],
            'hard_requirements': row['hard_requirements'],
            'score': row['job_score'],
            'job_site': row['site'],
            'url': row['job_url'],
            'location': None if pd.isna(row['location']) else row['location'],
            'date_posted': row['date_posted'],
            'comp_interval': None if pd.isna(row['interval']) else row['interval'],
            'comp_min': None if pd.isna(row['min_amount']) else int(row['min_amount']),
            'comp_max': None if pd.isna(row['max_amount']) else int(row['max_amount']),
            'comp_currency': None if pd.isna(row['currency']) else row['currency'],
            'emails': None if pd.isna(row['emails']) else row['emails'],
            'description': row['description'],
            'searched_title': row['searched_title'],
        }

        print(new_job)
        result = supabase.table('jobs').insert(new_job).execute()

        if result.data:
            print(f"Inserted job: {result.data}")
        else:
            print(f"Error inserting job: {result.error}")

    # data, count = supabase.table('countries')
    #   .insert({"id": 1, "name": "Denmark"})
    #   .execute()
