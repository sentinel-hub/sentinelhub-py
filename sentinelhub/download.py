"""
Script for downloading data
"""
from __future__ import print_function

import os
import time
import threading
import requests

try:
    import queue
except ImportError:
    import Queue as queue

REDOWNLOAD = False
THREADED_DOWNLOAD = False
RETURN_DATA = False

MAX_THREAD_NUMBER = 8
MAX_NUMBER_OF_DOWNLOAD_TRIES = 2 # Due to bad connection some requests might fail and need to be repeated
SLEEP_TIME = 5

SUCCESS_STATUS_CODE_INTERVAL = (200, 203)

VERBOSE = True

class DownloadError(Exception):
    pass

class MyThread(threading.Thread):
    def __init__(self, my_queue=None):
        threading.Thread.__init__(self)
        self.my_queue = my_queue

    def run(self):
        while True:
            request = self.my_queue.get()
            if request is None:
                break
            make_request(request[0], request[1])
            self.my_queue.task_done()

# Public function
def download_data(request_list, redownload=REDOWNLOAD, threaded_download=THREADED_DOWNLOAD):
    if not isinstance(request_list, list): # in case only one request would be given
        return download_data([request_list], redownload, threaded_download)

    if not redownload:
        request_list = remove_requests_of_existing_files(request_list)

    for request in request_list:
        set_file_location(request[1])

    if not threaded_download:
        for url, filename in request_list:
            make_request(url, filename)
    else:
        my_queue = queue.Queue()
        threads = []
        for i in range(MAX_THREAD_NUMBER):
            threads.append(MyThread(my_queue=my_queue))
            threads[-1].start()

        for request in request_list:
            my_queue.put(request)

        my_queue.join() # waits until all threads are done
        for i in range(MAX_THREAD_NUMBER):
            my_queue.put(None)
        for thread in threads:
            thread.join()

def make_request(url, filename=None, return_data=RETURN_DATA, verbose=VERBOSE):
    try_num = MAX_NUMBER_OF_DOWNLOAD_TRIES
    response = None
    while try_num > 0:
        try:
            response = requests.get(url)
            if SUCCESS_STATUS_CODE_INTERVAL[0] <= response.status_code <= SUCCESS_STATUS_CODE_INTERVAL[1]:
                try_num = 0
                if verbose:
                    print('Downloaded from %s' % url)
            else:
                raise
        except:
            try_num -= 1
            if try_num > 0:
                if verbose:
                    print('Unsuccessful download from %s ... will retry in %ds' % (url, SLEEP_TIME))
                time.sleep(SLEEP_TIME)
            else:
                if verbose:
                    print('Failed to download from %s' % url)
                return response
    if filename is not None:
        with open(filename, 'wb') as f:
            f.write(response.content)
    if return_data:
        return response

def remove_requests_of_existing_files(request_list):
    return [request for request in request_list if not os.path.exists(request[1])]

def get_json(url):
    response = make_request(url, return_data=True, verbose=False)
    try:
        return response.json()
    except:
        if response is None:
            raise DownloadError('No internet connection')
        else:
            raise DownloadError('Invalid url request %s' % url)

def set_file_location(filename):
    path = '/'.join(filename.split('/')[:-1])
    if path == '':
        path = '.'
    make_folder(path)

def make_folder(path):
    if not os.path.exists(path):
        os.makedirs(path)


if __name__ == '__main__':
    pass
    # Example:
    #download_data([('http://sentinel-s2-l1c.s3.amazonaws.com/tiles/54/H/VH/2017/4/14/0/metadata.xml', 'example.xml')], redownload=True, threaded_download=True)
