from datetime import date
from roadway_analytics_factory import RoadwayAnalyticsMethod
from data_executors import StatewideDataExecutor, _DataExecutor, CorridorsDataExecutor


class DataExecutorFactory:
    __instances = {
        RoadwayAnalyticsMethod.STATEWIDE: StatewideDataExecutor,
        RoadwayAnalyticsMethod.CORRIDORS: CorridorsDataExecutor
    }

    @staticmethod
    def get_data_executor(start_date: date, end_date: date, output_dir: str, method: RoadwayAnalyticsMethod,
                          map_version: float) -> _DataExecutor:
        return DataExecutorFactory.__instances[method](start_date, end_date, output_dir, method, map_version)
