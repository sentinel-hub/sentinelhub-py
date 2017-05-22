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

SUCCESS_STATUS_CODE = 200

VERBOSE = True

class DownloadError(Exception):
    pass

class MyThread(threading.Thread):
    def __init__(self, myQueue=None):
        threading.Thread.__init__(self)
        self.myQueue = myQueue

    def run(self):
        while True:
            request = self.myQueue.get()
            if request is None:
                break
            make_request(request[0], request[1])
            self.myQueue.task_done()

# Public function
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
        myQueue = queue.Queue()
        threads = []
        for i in range (MAX_THREAD_NUMBER):
            threads.append(MyThread(myQueue=myQueue))
            threads[-1].start()

        for request in requestList:
            myQueue.put(request)

        myQueue.join() # waits until all threads are done
        for i in range (MAX_THREAD_NUMBER):
            myQueue.put(None)
        for thread in threads:
            thread.join()

def make_request(url, filename=None, returnData=RETURN_DATA, verbose=VERBOSE):
    tryNum = MAX_NUMBER_OF_DOWNLOAD_TRIES
    response = None
    while tryNum > 0:
        try:
            response = requests.get(url)
            if response.status_code == SUCCESS_STATUS_CODE:
                tryNum = 0
                if verbose:
                    print('Downloaded from %s' % url)
            else:
                raise
        except:
            tryNum -= 1
            if tryNum > 0:
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
    if returnData:
        return response

def reduce_requests(requestList):
    return [request for request in requestList if not os.path.exists(request[1])]

def get_json(url):
    response = make_request(url, returnData=True, verbose=False)
    try:
        return response.json()
    except:
        if response is not None:
            raise DownloadError('Invalid url request %s' % url)
        else:
            raise DownloadError('No internet connection')

def set_file_location(filename):
    path = '/'.join(filename.split('/')[:-1])
    if path == '':
        path = '.'
    set_folder(path)

def set_folder(path):
    if not os.path.exists(path):
        os.makedirs(path)


if __name__ == '__main__':
    pass
    # Example:
    #download_data([('http://sentinel-s2-l1c.s3.amazonaws.com/tiles/54/H/VH/2017/4/14/0/metadata.xml', 'example.xml')], redownload=True, threadedDownload=True)
