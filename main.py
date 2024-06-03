from util import *
from write_to_warehouse import *
from roadway_analytics_factory import RoadwayAnalyticsMethod
from data_executors_factory import DataExecutorFactory
from bottleneck_processors import OriginationBottlenecksProcessor
from loguru import logger

if __name__ == '__main__':
    # 1. Get the proper parameters for the Inrix API request.
    logger.log('INFO', 'Compiling parameters...')
    # date_range = find_date_range('weekly')  # Enter 'manual' and provide start_date and end_date, or 'weekly'
    date_range = find_date_range('manual', '2024-03-01', '2024-05-31')
    map_version = get_map_version()

    # 2. Request data from the Inrix API. Save zip file to 'Zip' folder.
    logger.log('INFO', 'Creating INRIX request...')
    data_executor = DataExecutorFactory.get_data_executor(start_date=date_range[0], end_date=date_range[1],
                                                          output_dir='Zip',
                                                          method=RoadwayAnalyticsMethod['CORRIDORS'],
                                                          map_version=map_version)
    logger.log('INFO', 'Requesting data from INRIX...')
    data_executor.download()

    # 3. Take new zipfile, unzip it, and manipulate spreadsheet.
    logger.log('INFO', 'Processing bottlenecks file...')
    OriginationBottlenecksProcessor().process_bottlenecks()

    # 4. Write contents of spreadsheet to data warehouse tables.

    # 4a. Write wide version to warehouse.
    for file in os.listdir(os.getcwd() + '\\ready_for_warehouse'):
        logger.log('INFO', 'Writing ' + file + ' wide version to warehouse...')
        wide_to_wh(os.getcwd() + '\\ready_for_warehouse\\' + file)

    # 4b. Write counts to warehouse.
    logger.log('INFO', 'Writing counts to warehouse...')
    day_sum_to_wh('counts')

    # 4c. Write impact factor to warehouse.
    logger.log('INFO', 'Writing impact factor to warehouse...')
    day_sum_to_wh('impact_factor')

    logger.log('INFO', 'All data written to warehouse. Bottlenecks process complete. :)')
