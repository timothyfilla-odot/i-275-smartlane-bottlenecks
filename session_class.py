import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry


class SessionClass():
    def __init__(self) -> None:
        self.session: requests.sessions.Session = requests.sessions.Session()

        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[302, 429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries = retry_strategy)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)
        self.session.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36 Edg/105.0.1343.27'