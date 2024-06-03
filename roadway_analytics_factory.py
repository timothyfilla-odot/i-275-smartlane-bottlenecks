from enum import Enum
from roadway_analytics import _RoadwayAnalytics, BottleneckAnalytics, BottleneckAnalyticsForSegments


class RoadwayAnalyticsMethod(Enum):
    STATEWIDE = 1
    CORRIDORS = 2


class RoadwayAnalyticsFactory:
    __instances = {
        RoadwayAnalyticsMethod.STATEWIDE: BottleneckAnalytics,
        RoadwayAnalyticsMethod.CORRIDORS: BottleneckAnalyticsForSegments
    }

    @staticmethod
    def get_downloader(method: RoadwayAnalyticsMethod, output_dir: str) -> _RoadwayAnalytics:
        return RoadwayAnalyticsFactory.__instances[method](output_dir)
