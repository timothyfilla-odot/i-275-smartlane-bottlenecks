import pandas as pd
from datetime import datetime, timedelta, date
from loguru import logger
import sys
import pytz
import os
from tsmo_db.db_factory import DbFactory, DbTypes, VaultConnectionType
import sqlalchemy as sa


def get_db_engine() -> sa.engine.Engine:
    return DbFactory.get_provider(DbTypes.AZURE_SYNAPSE, VaultConnectionType.SQL_SERVER).get_engine()


def find_date_range(method, start_date='', end_date='') -> tuple:
    if method == 'manual':
        if not start_date or not end_date:
            logger.log('ERROR', 'Provide start_date and end_date parameters.')
            sys.exit('Insufficient parameters supplied.')
        date_tup = (date.fromisoformat(start_date), date.fromisoformat(end_date))
    elif method == 'weekly':
        date_tup = ((datetime.now() - timedelta(days=9)).date(), (datetime.now() - timedelta(days=3)).date())
    else:
        logger.log('ERROR',
                   "Invalid request method specified. Use 'manual' with two dates as following arguments, or "
                   "'weekly' to run last week.")
        sys.exit('Invalid parameters.')
    tz = pytz.timezone('America/New_York')
    datetime_tup = (datetime(year=date_tup[0].year, month=date_tup[0].month, day=date_tup[0].day).replace(tzinfo=tz),
                    datetime(year=date_tup[1].year, month=date_tup[1].month, day=date_tup[1].day).replace(tzinfo=tz))
    return datetime_tup


def get_map_version() -> float:
    now = datetime.today().date().isoformat()
    sql = f"SELECT InrixMapVersion " \
          f"FROM NlfId_Inrix " \
          f"WHERE StartDate <= '{now}' AND (EndDate IS NULL OR EndDate >= '{now}') " \
          f"GROUP BY InrixMapVersion " \
          f"ORDER BY InrixMapVersion DESC "
    df = pd.read_sql(sql, get_db_engine())
    return float(df.iloc[0][0])


def deduplicate(data: pd.DataFrame, duplicate_folder: str):
    duplicate_ids = []
    original_count = len(data)
    duplicate_dir = os.path.join(os.getcwd(), duplicate_folder)

    for filename in os.listdir(duplicate_dir):
        file = os.path.join(duplicate_dir, filename)
        df = pd.read_csv(file)
        df = df[df['label'] == 'No']
        dupes = list(df['Id'])
        duplicate_ids.extend(dupes)

    data = data[~data['Id'].isin(duplicate_ids)]
    logger.log('INFO', 'Removed ' + str(original_count - len(data)) + ' duplicate bottlenecks.')

    return data
