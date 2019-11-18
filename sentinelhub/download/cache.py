""" Caching logic
"""
import hashlib
import json


def hash_request(url, payload):
    """ Joins url and payload into a dictionary, dumps it as json and hashes it with md5 algorithm
    """
    if not isinstance(url, str):
        raise ValueError('Expected the url argument of type str, got {}.'.format(type(url)))
    if not isinstance(payload, dict):
        raise ValueError('Expected the payload argument of type dict, got {}.'.format(type(payload)))

    hashable = dict(url=url, payload=payload)
    hashable = json.dumps(hashable, indent=4)
    hashed = hashlib.md5(hashable.encode('utf-8')).hexdigest()

    return hashed, hashable
