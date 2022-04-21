"""
Module for managing files and folders
"""

import errno
import os
from sys import platform
from typing import List


def get_content_list(folder: str = ".") -> List[str]:
    """Get list of contents in input folder

    :param folder: input folder to list contents. Default is ``'.'``
    :return: list of folder contents
    """
    return os.listdir(folder)


def get_folder_list(folder: str = ".") -> List[str]:
    """Get list of sub-folders contained in input folder

    :param folder: input folder to list sub-folders. Default is ``'.'``
    :return: list of sub-folders
    """
    dir_list = get_content_list(folder)
    return [f for f in dir_list if not os.path.isfile(os.path.join(folder, f))]


def get_file_list(folder: str = ".") -> List[str]:
    """Get list of files contained in input folder

    :param folder: input folder to list files only. Default is ``'.'``
    :return: list of files
    """
    dir_list = get_content_list(folder)
    return [f for f in dir_list if os.path.isfile(os.path.join(folder, f))]


def create_parent_folder(filename: str) -> None:
    """Create parent folder for input filename recursively

    :param filename: input filename
    :raises: error if folder cannot be created
    """
    path = os.path.dirname(filename)
    if path != "":
        make_folder(path)


def make_folder(path: str) -> None:
    """Create folder at input path recursively

    Create a folder specified by input path if one
    does not exist already

    :param path: input path to folder to be created
    :raises: os.error if folder cannot be created
    """
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise ValueError(
                    f"Specified folder is not writable: {path}\nPlease check permissions or set a new valid folder."
                ) from exception


def rename(old_path: str, new_path: str, edit_folders: bool = True) -> None:
    """Rename files or folders

    :param old_path: name of file or folder to rename
    :param new_path: name of new file or folder
    :param edit_folders: flag to allow recursive renaming of folders. Default is `True`
    """
    if edit_folders:
        os.renames(old_path, new_path)
    else:
        os.rename(old_path, new_path)


def size(pathname: str) -> int:
    """Returns size of a file or folder in Bytes

    :param pathname: path to file or folder to be sized
    :return: size of file or folder in Bytes
    :raises: os.error if file is not accessible
    """
    if os.path.isfile(pathname):
        return os.path.getsize(pathname)
    return sum([size(f"{pathname}/{name}") for name in get_content_list(pathname)])


def sys_is_windows() -> bool:
    """Check if user is running the code on Windows machine

    :return: `True` if OS is Windows and `False` otherwise
    """
    return platform.lower().startswith("win")
