# Script to simplify SLURM API calls by mimicking the SLURM CLI

import requests

class SLURMAPIConnection(endpoint=None, token=None, headers=None):
    def init(endpoint=None, token=None):
        try:
            assert endpoint is not None
        except AssertionError as ae:
            raise AssertionError('endpoint (str) must be provided.')
        except Error as e:
            raise Error('Problem interpreting endpoint string')
        try:
            assert token is not None
        except AssertionError as ae:
            raise AssertionError('token (str) must be provided.')
        except Error as e:
            raise Error('Problem interpreting token string')
        if endpoint:
            self.endpoint = endpoint
        else:
            raise ValueError('Cannot assign endpoint')
        if token:
            self.token = token
        else:
            raise ValueError('Cannot assign token')

        self.headers = {"X-SLURM-USER-TOKEN": self.token}

    def diag(self):
        response = requests.get(f"{self.endpoint}/diag", headers=self.headers)
        return response.json()