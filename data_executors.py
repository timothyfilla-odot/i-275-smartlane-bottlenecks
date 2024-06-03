from datetime import date, datetime, timedelta
import pandas as pd
from roadway_analytics_factory import RoadwayAnalyticsFactory, RoadwayAnalyticsMethod
from util import get_db_engine


class _DataExecutor:
    def __init__(self, start_date: date, end_date: date, output_dir: str,
                 method: RoadwayAnalyticsMethod, map_version: float) -> None:
        self.start_date = start_date
        self.end_date = end_date
        self.output_dir = output_dir
        self.method = method
        self.map_version = map_version

    def download(self):
        pass


class StatewideDataExecutor(_DataExecutor):
    """
    Left empty because this is not used for this dashboard; carried over from TOAST.
    """
    def __init__(self, start_date: datetime, end_date: datetime, output_dir: str,
                 method: RoadwayAnalyticsMethod, map_version: float) -> None:
        super().__init__(start_date, end_date, output_dir, method, map_version)


class CorridorsDataExecutor(_DataExecutor):

    def __init__(self, start_date: datetime, end_date: datetime, output_dir: str,
                 method: RoadwayAnalyticsMethod, map_version: float) -> None:
        super().__init__(start_date, end_date + timedelta(days=1), output_dir, method, map_version)
        self.api = RoadwayAnalyticsFactory.get_downloader(RoadwayAnalyticsMethod.CORRIDORS, self.output_dir)

    def _get_segments(self) -> list:
        tsmo_dw = get_db_engine()

        sql = """
        SELECT XDSegID
        FROM NlfId_Inrix
        WHERE
	        InrixMapVersion = ? AND
	        NLFID = 'SHAMIR00275**C' AND
	        StationDirection = 'Downstation' AND
	        ((CAST(CTL_End AS float) >= 28.92 AND CAST(CTL_End AS float) <= 35.3) OR (CAST(CTL_Beg AS float) <= 35.3 AND CAST(CTL_Beg AS float) >= 28.92))
        """

        segments_df = pd.read_sql(sql, con=tsmo_dw, params=[self.map_version])
        segment_list = segments_df['XDSegID'].tolist()
        segment_list = [int(i) for i in segment_list]
        return segment_list

    def _get_data_for_corridors(self):
        self.api.get_data(self.start_date, self.end_date, self.map_version, self._get_segments())

    def download(self):
        self._get_data_for_corridors()
