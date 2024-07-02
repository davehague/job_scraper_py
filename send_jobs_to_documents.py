import csv

from file_utils import get_downloads_folder
import os


def clean_filename(filename):
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    return filename


def write_jobs_to_downloads(folder_name, jobs_df):
    downloads_folder = get_downloads_folder()
    folder_path = os.path.join(downloads_folder, folder_name)

    # Create the folder if it doesn't exist
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    # Iterate over each row in the dataframe
    for index, row in jobs_df.iterrows():
        # Create a markdown formatted string
        markdown_text = f"# {row.get('title', 'N/A')}\n\n"
        for column in jobs_df.columns:
            if column != 'title':
                markdown_text += f"**{column}**\n{row[column]}\n\n"

        # Save the markdown formatted string to a file
        filename = f"{clean_filename(row.get('title', 'N/A'))}-{clean_filename(row.get('company'), 'N/A')}-{row.get('location', 'N/A')}.md"
        file_path = os.path.join(folder_path, filename)
        print(f"Writing file: {file_path}")  # Print the filename
        try:
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(markdown_text)
        except Exception as e:
            print(f"Error writing file: {file_path}. Error: {e}")  # Print any errors that occur
