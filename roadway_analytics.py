import os
from typing import Optional, List
import requests
import requests.adapters
from datetime import date, datetime, timedelta
from dateutil import parser
import pytz
import time
from loguru import logger


def _get_map_version_string(map_version: float) -> str:
    """
    Converts a map version float to an API-compatible string.
    :param map_version: the map version as a float
    :return: an API-compatible string
    """
    map_str = str(map_version)
    return map_str.replace('.', '0')


class RoadwayAnalyticsResponse:
    """
    Contains response data from the Roadway Analytics-based API request.
    """

    def __init__(self, response_type, result):
        self.response_type = response_type
        self.result = result


class _RoadwayAnalytics:
    """
    RoadwayAnalytics is an abstract class and should not be instantiated.
    """
    _session: Optional[requests.Session] = None
    _token: str = ""
    _token_expiry: datetime = datetime.utcnow().astimezone(pytz.utc)

    def __init__(self, output_dir=os.getcwd()):
        self.output_dir = output_dir

    def _create_session_if_not_exists(self) -> None:
        """
        If a session does not already exist, create one.
        :return: None
        """
        # if the session already exists, do nothing
        if self._session is not None:
            return
        self._session = requests.Session()
        return

    def _get_data_from_api(self, **kwargs) -> RoadwayAnalyticsResponse:
        pass

    def _refresh_token(self) -> None:
        """
        Refreshes the authorization token if necessary.
        :return: None
        """
        assert self._session is not None, "A session must be created before refreshing the token"

        # if we're within one minute of token expiry, get a new one
        if (datetime.utcnow().astimezone(pytz.utc) + timedelta(minutes=1)) > self._token_expiry:
            r = requests.get("https://uas-api.inrix.com/v1/appToken?"
                             "appId=7de7c637-b215-459d-a5c1-ad733bea18b8"
                             "&hashToken=54f96a0ee3f216566bd25eadac4ab7f31a0bbc2c")
            if not r.ok:
                r.raise_for_status()

            res = r.json()
            self._token = res['result']['token']
            self._token_expiry = parser.parse(res['result']['expiry'])

            # update the Authorization header on the token
            self._session.headers = {"Authorization": f"Bearer {self._token}"}

    def get_data(self, **kwargs) -> RoadwayAnalyticsResponse:
        """
        Retrieves data from the Roadway Analytics API.
        :return: a RoadwayAnalyticsResponse variable.
        """

        self._create_session_if_not_exists()
        self._refresh_token()

        return self._get_data_from_api(**kwargs)


class BottleneckAnalytics(_RoadwayAnalytics):
    """
    Carried over from TOAST. Not used here.
    """

    def __init__(self, output_dir=os.getcwd()):
        super().__init__()


class BottleneckAnalyticsForSegments(_RoadwayAnalytics):
    """
    Adapted from the TOAST Bottlenecks script.
    """

    def __init__(self, output_dir=os.getcwd()):
        super().__init__(output_dir)

    def _download_data(self, report_id, api_args, start_date, end_date) -> RoadwayAnalyticsResponse:
        """
        Waits for the report to be generated and downloads the data.
        :param report_id: the report id of the running report
        :return: a string containing the filename of the saved file
        """
        data_avail = False
        error = False
        time_start = datetime.now()

        end_date = end_date + timedelta(days=-1)

        url = "https://roadway-analytics-api.inrix.com/v1/bottlenecksReport/" \
              f"download/{report_id}" \
              "?includeBottleneckOccurrences=true"

        while (not data_avail) \
                and (not error) \
                and (datetime.now() - time_start).total_seconds() <= 1800:
            req = self._session.get(url)

            if req.status_code not in (200, 404):
                # we had an unknown error
                err_msg = 'Unknown error with status code '+ str(req.status_code)
                logger.log('ERROR', err_msg)
                return RoadwayAnalyticsResponse('error', err_msg)
            elif req.status_code == 404:
                # let's wait a bit before trying again
                time.sleep(1)
            elif req.status_code == 200:
                # let's save the file
                logger.log('INFO', 'Successfully retrieved data. Writing to zip file...')
                filename = f'bottlenecks_{start_date.strftime("%Y-%m-%d")}' \
                           f'_{end_date.strftime("%Y-%m-%d")}.zip'
                with open(os.path.join(self.output_dir, filename), 'wb') as file:
                    for chunk in req.iter_content():
                        file.write(chunk)
                return RoadwayAnalyticsResponse(str, filename)

        err_msg = f'Report for {start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}' \
                  ' took too long to run.'
        logger.log('ERROR', err_msg)
        return RoadwayAnalyticsResponse('error', err_msg)

    @staticmethod
    def _prepare_payload(api_args: dict) -> dict:
        """
        Prepares the payload to be sent to the API endpoint.
        :param kwargs: arguments to include
        :return: a payload dictionary
        """
        data = api_args.copy()

        data['reportName'] = 'odot_275_phase1_bottlenecks'
        data['userName'] = 'test_user_beta'
        data['userId'] = '7de7c637-b215-459d-a5c1-ad733bea18b8'
        data['unit'] = 'IMPERIAL'
        data['startDate'] = data['startDate'].isoformat()
        data['endDate'] = data['endDate'].isoformat()
        data['mapVersion'] = _get_map_version_string(data['mapVersion'])

        return data

    def _get_data_from_api(self, **kwargs) -> RoadwayAnalyticsResponse:
        """

        :param start_date:
        :param end_date:
        :param map_version:
        :param kwargs:
        :return:
        """
        """
        Example request:
        {  
            "reportName":"Sample Report",
            "userName":"userA",
            "mapVersion":"2102",
            "unit":"IMPERIAL",
            "segmentIds":[
                132582324,132403471,132912967,133431482,133433590,133534361,133729944,133730345,133632076,133831443,132807234,133636717,133638456,133638557,133638753,133445039,133446838,133836523,132698484,133931496,133450944,133553967,133565317,133568173,133849076,133851047,132724002,133859519,133642541,133939125,133940404,133941765,133941886,133457377,133457780,132744348,132863725,133869535,133648382,133649630,133944229,133945054,133946003,133948862,133875647,133883170,133883344,134021202,134023769,133749997
            ],
            "startDate":"2021-08-01T00:00:00.000Z",
            "endDate":"2021-08-02T00:00:00.000Z"
        }

        """

        data = self._prepare_payload(kwargs['api_args'])

        req = self._session.post("https://roadway-analytics-api.inrix.com/v1/"
                                 "bottlenecksReport",
                                 json=data)
        if not req.ok:
            print(data)
            err_msg = 'Bad request (Code ' + str(req.status_code) + ')'
            logger.log('ERROR', err_msg)
            return RoadwayAnalyticsResponse('error', err_msg)

        res = req.json()
        report_id = res['reportId']

        filename = self._download_data(report_id,
                                       api_args=kwargs['api_args'],
                                       start_date=kwargs['api_args']['startDate'],
                                       end_date=kwargs['api_args']['endDate'])

        return RoadwayAnalyticsResponse(str, filename)

    def get_data(self, start_date: datetime, end_date: datetime,
                 map_version: float, segments: List[int]) -> RoadwayAnalyticsResponse:
        """
        Retrieves data from the Roadway Analytics API.
        :param start_date: the time-zone aware start date to request data
        :param end_date: the time-zone aware exclusive end date to request data
        :param map_version: the INRIX map version
        :return: a list of dictionaries containing the data
        """
        # verify parameters
        assert type(start_date) == datetime, "start_date must be a datetime"
        assert type(end_date) == datetime, "end_date must be a datetime"
        assert type(map_version) == float, "map_version must be a float"
        # assert start_date.tzinfo is not None, "start_date must be localized to a tz"
        # assert end_date.tzinfo is not None, "end_date must be localized to a tz"
        assert type(segments) == list, "segments must be a list"
        assert len(segments) > 0, "segments must not be empty"
        assert sum([type(x) == int for x in segments]) == len(segments), "segments must contain only integers"

        api_args = {
            'startDate': start_date,
            'endDate': end_date,
            'mapVersion': map_version,
            'segmentIds': segments
        }

        return super().get_data(api_args=api_args)
