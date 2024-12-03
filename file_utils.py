import os
from pathlib import Path


def get_downloads_folder():
    home = str(Path.home())
    return os.path.join(home, 'Downloads')


def write_jobs_to_downloads(filename, jobs_df):
    downloads_folder = get_downloads_folder()
    file_path = os.path.join(downloads_folder, f"{filename}.csv")
    jobs_df.to_csv(file_path, index=False)
