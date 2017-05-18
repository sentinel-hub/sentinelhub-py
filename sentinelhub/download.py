"""
Script for downloading data
"""

import os
import requests
import time
import threading


REDOWNLOAD = False
THREADED_DOWNLOAD = True
RETURN_DATA = False

# Due to bad connection some requests might fail and need to be repeated
MAX_NUMBER_OF_DOWNLOAD_TRIES = 3
SLEEP_TIME = 5

SUCCESS_STATUS_CODE = 200

VERBOSE = True

# Threaded download class
class MyThread (threading.Thread):
    def __init__(self, url, filename):
        threading.Thread.__init__(self)
        self.url = url
        self.filename = filename

    def run(self):
        make_request(self.url, self.filename)


def download_data(requestList, redownload=REDOWNLOAD, threadedDownload=THREADED_DOWNLOAD):
    if not isinstance(requestList, list): # in case only one request would be given
        return download_data([requestList], redownload, threadedDownload)

    if not redownload:
        requestList = reduce_requests(requestList)

    for request in requestList:
        set_file_location(request[1])

    if not threadedDownload:
        for url, filename in requestList:
            make_request(url, filename)
    else:
        threads = []
        for url, filename in requestList:
            threads.append(MyThread(url, filename))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

def make_request(url, filename=None, returnData=RETURN_DATA, verbose=VERBOSE):
    tryNum = MAX_NUMBER_OF_DOWNLOAD_TRIES
    while tryNum > 0:
        try:
            response = requests.get(url)
            if response.status_code == SUCCESS_STATUS_CODE:
                tryNum = 0
                if verbose:
                    print('Downloaded from ' + url + '\n', end = '')
            else:
                raise
        except:
            tryNum -= 1
            if tryNum > 0:
                if verbose:
                    print('Unsuccessful download from ' + url + '... will retry in ' + str(SLEEP_TIME) + 's\n', end = '')
                time.sleep(SLEEP_TIME)
            else:
                if verbose:
                    print('Failed to download from ' + url + '\n', end = '')
    if filename is not None:
        with open(filename, 'wb') as f:
            f.write(response.content)
    if returnData:
        return response

def reduce_requests(requestList):
    return [request for request in requestList if not os.path.exists(request[1])]

def get_json(url):
    return make_request(url, returnData=True, verbose=False).json()

def set_file_location(filename):
    path = '/'.join(filename.split('/')[:-1])
    if path == '':
        path = '.'
    set_folder(path)

def set_folder(path):
    if not os.path.exists(path):
        os.makedirs(path)


if __name__ == '__main__':
    download_data([('http://sentinel-s2-l1c.s3.amazonaws.com/tiles/54/H/VH/2017/4/14/0/metadata.xml', 'example.xml')], redownload=True, threadedDownload=True)
