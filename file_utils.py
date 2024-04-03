import csv
import os
from datetime import datetime
from pathlib import Path
import pandas as pd


def get_downloads_folder():
    home = str(Path.home())
    return os.path.join(home, 'Downloads')


def read_df_from_downloads(filename):
    downloads_folder = get_downloads_folder()
    jobs_csv_path = os.path.join(downloads_folder, filename)
    all_jobs = pd.read_csv(jobs_csv_path)
    return all_jobs


def save_df_to_downloads(df, filename):
    today = datetime.today().strftime('%Y-%m-%d-%H-%M-%S')
    filename = f"{filename}_{today}.csv"
    downloads_folder = get_downloads_folder()
    jobs_csv_path = os.path.join(downloads_folder, filename)

    print(f"Saving jobs to {jobs_csv_path}")
    df.to_csv(jobs_csv_path, quoting=csv.QUOTE_NONNUMERIC, escapechar="\\", index=False)