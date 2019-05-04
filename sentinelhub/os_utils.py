"""
Module for managing files and folders
"""

import os
import errno
from sys import platform


def get_content_list(folder='.'):
    """ Get list of contents in input folder

    :param folder: input folder to list contents. Default is ``'.'``
    :type folder: str
    :return: list of folder contents
    :rtype: list(str)
    """
    return os.listdir(folder)


def get_folder_list(folder='.'):
    """ Get list of sub-folders contained in input folder

    :param folder: input folder to list sub-folders. Default is ``'.'``
    :type folder: str
    :return: list of sub-folders
    :rtype: list(str)
    """
    dir_list = get_content_list(folder)
    return [f for f in dir_list if not os.path.isfile(os.path.join(folder, f))]


def get_file_list(folder='.'):
    """ Get list of files contained in input folder

    :param folder: input folder to list files only. Default is ``'.'``
    :type folder: str
    :return: list of files
    :rtype: list(str)
    """
    dir_list = get_content_list(folder)
    return [f for f in dir_list if os.path.isfile(os.path.join(folder, f))]


def create_parent_folder(filename):
    """ Create parent folder for input filename recursively

    :param filename: input filename
    :type filename: str
    :raises: error if folder cannot be created
    """
    path = os.path.dirname(filename)
    if path != '':
        make_folder(path)


def make_folder(path):
    """ Create folder at input path recursively

    Create a folder specified by input path if one
    does not exist already

    :param path: input path to folder to be created
    :type path: str
    :raises: os.error if folder cannot be created
    """
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise ValueError('Specified folder is not writable: %s'
                                 '\nPlease check permissions or set a new valid folder.' % path)


def rename(old_path, new_path, edit_folders=True):
    """ Rename files or folders

    :param old_path: name of file or folder to rename
    :param new_path: name of new file or folder
    :param edit_folders: flag to allow recursive renaming of folders. Default is `True`
    :type old_path: str
    :type new_path: str
    :type edit_folders: bool
    """
    if edit_folders:
        os.renames(old_path, new_path)
    else:
        os.rename(old_path, new_path)


def size(pathname):
    """ Returns size of a file or folder in Bytes

    :param pathname: path to file or folder to be sized
    :type pathname: str
    :return: size of file or folder in Bytes
    :rtype: int
    :raises: os.error if file is not accessible
    """
    if os.path.isfile(pathname):
        return os.path.getsize(pathname)
    return sum([size('{}/{}'.format(pathname, name)) for name in get_content_list(pathname)])


def sys_is_windows():
    """ Check if user is running the code on Windows machine

    :return: `True` if OS is Windows and `False` otherwise
    :rtype: bool
    """
    return platform.lower().startswith('win')
