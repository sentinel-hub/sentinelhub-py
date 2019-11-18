import unittest

from sentinelhub import SentinelHubSession


class TestSession(unittest.TestCase):

    def test_session(self):

        session = SentinelHubSession()

        token = session.token
        headers = session.session_headers

        for item in [token, headers]:
            self.assertTrue(isinstance(item, dict))

        for key in ['access_token', 'expires_in', 'expires_at']:
            self.assertTrue(key in token, msg="Key '{}' was not found in a token".format(key))


if __name__ == '__main__':
    unittest.main()
