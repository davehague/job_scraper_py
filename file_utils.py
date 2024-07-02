import os
from pathlib import Path


def get_downloads_folder():
    home = str(Path.home())
    return os.path.join(home, 'Downloads')
