import shutil
import pandas as pd
from tqdm import tqdm
from session_class import SessionClass
import os
from zipfile import ZipFile
from loguru import logger
from util import deduplicate


class BottleneckProcessor:
    """
    A base class that every Bottleneck Processor should implement.
    """

    @staticmethod
    def _move_completed_file(file_path: str) -> None:
        shutil.move(file_path, os.getcwd() + '\\processed_zips')

    def process_bottlenecks(self):
        pass


class OriginationBottlenecksProcessor(BottleneckProcessor):
    tqdm.pandas()

    def __init__(self) -> None:
        super().__init__()
        self._session = SessionClass()

    @staticmethod
    def _hour_creator(df) -> pd.DataFrame:

        def _fill_hours(row):
            zero_hours = list(range(0, 24))
            zero_hours = list(map(str, zero_hours))
            hour_diff = row['EndHourET'] - row['StartHourET']
            # Case 1: Bottleneck occurs within a single hour
            if hour_diff == 0:
                row[str(row['StartHourET'])] = row['TotalImpactFactor']
                zero_hours.remove(str(int(row['StartHourET'])))
            # Case 2: Bottleneck split over multiple hours
            elif hour_diff >= 1:
                middle_hours = range(row['StartHourET'] + 1, row['EndHourET'])
                full_hour_portion = 60 / row['MaxDurationMinutes']
                for hour in middle_hours:
                    row[str(hour)] = full_hour_portion * row['TotalImpactFactor']
                    zero_hours.remove(str(hour))
                first_pct = (60 - row['StartMinuteET']) / row['MaxDurationMinutes']
                last_pct = row['EndMinuteET'] / row['MaxDurationMinutes']
                row[str(int(row['StartHourET']))] = row['TotalImpactFactor'] * first_pct
                row[str(int(row['EndHourET']))] = row['TotalImpactFactor'] * last_pct
                zero_hours.remove(str(int(row['StartHourET'])))
                zero_hours.remove(str(int(row['EndHourET'])))

            # Fill in all other hours with 0
            for zh in zero_hours:
                row[zh] = 0

            return row

        # Create hour columns
        df = df.apply(lambda x: _fill_hours(x), axis=1)
        df = df[['BottleneckSummaryIndex', 'Id', 'StartDateUTC', 'EndDateUTC', 'FromIntersectionName',
                 'ToIntersectionName', 'Direction', 'MaxDurationMinutes', 'StartSegmentOffsetMiles',
                 'EndSegmentOffsetMiles', 'MaxLengthMiles', 'FromPointLat', 'FromPointLon', 'ToPointLat', 'ToPointLon',
                 'StartDateET', 'EndDateET', 'StartHourET', 'StartMinuteET', 'EndHourET', 'EndMinuteET',
                 'TotalImpactFactor', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14',
                 '15', '16', '17', '18', '19', '20', '21', '22', '23']]
        return df

    def _run_bottleneck_file(self, bn_data, csv_name):
        df = pd.read_csv(bn_data)

        # Create/alter necessary columns
        df['StartDate'] = pd.to_datetime(df['StartDate'])
        df['EndDate'] = pd.to_datetime(df['EndDate'])
        df.rename(columns={'StartDate': 'StartDateUTC', 'EndDate': 'EndDateUTC'}, inplace=True)
        df['StartDateET'] = df['StartDateUTC'].dt.tz_convert('US/Eastern')
        df['EndDateET'] = df['EndDateUTC'].dt.tz_convert('US/Eastern')
        df['StartDateUTC'] = (df['StartDateUTC'].astype(str)).str[:19]
        df['EndDateUTC'] = (df['EndDateUTC'].astype(str)).str[:19]
        df['StartHourET'] = df.apply(lambda x: int(x['StartDateET'].hour), axis=1)
        df['StartMinuteET'] = df.apply(lambda y: int(y['StartDateET'].minute), axis=1)
        df['EndHourET'] = df.apply(lambda x: int(x['EndDateET'].hour), axis=1)
        df['EndMinuteET'] = df.apply(lambda y: int(y['EndDateET'].minute), axis=1)

        df['TotalImpactFactor'] = df['MaxDurationMinutes'] * df['MaxLengthMiles']
        df = self._hour_creator(df)

        logger.log('INFO', 'De-duplicating...')
        df = deduplicate(df, 'duplicates')
        df.to_csv(os.getcwd() + '\\ready_for_warehouse\\' + csv_name + '.csv', index=False)

    def _process_zip(self, new_data):
        for file in new_data:
            full_file = (os.getcwd() + '\\Zip\\' + file)
            try:
                with ZipFile(full_file, 'r') as bn_z:
                    logger.log('INFO', 'Processing file ' + file + '...')
                    with bn_z.open('BottleneckOccurrences.csv') as bn_csv:
                        self._run_bottleneck_file(bn_data=bn_csv, csv_name=file[:-4])
                logger.log('INFO', 'Processed file ' + file)
                self._move_completed_file(full_file)
            except FileNotFoundError:
                logger.log('ERROR', file + ' is not a zip file. Skipping.')

    def process_bottlenecks(self):
        super().process_bottlenecks()
        self._process_zip(os.listdir(os.getcwd() + '\\Zip\\'))
