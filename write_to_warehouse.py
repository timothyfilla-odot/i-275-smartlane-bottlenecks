from util import deduplicate, get_db_engine
import pandas as pd
from loguru import logger
from tqdm import tqdm
from math import ceil
import json
import os
import sys
import shutil

engine = get_db_engine()


def wide_to_wh(file):
    df = pd.read_csv(file)
    df['StartDateET'] = df.apply(lambda x: x['StartDateET'][:-6], axis=1)
    df['EndDateET'] = df.apply(lambda y: y['EndDateET'][:-6], axis=1)
    with engine.begin() as con:
        for i in tqdm(range(ceil(df.shape[0] / 100) + 1)):
            json_string = json.dumps(df.iloc[i * 100: (i + 1) * 100].to_dict('records'))
            con.execute(f"""
                        INSERT INTO [dbo].[275Bottlenecks_Wide]
                        SELECT *
                        FROM OPENJSON('{json_string}')
                            WITH (
                                BottleneckSummaryIndex int '$.BottleneckSummaryIndex',
                                Id varchar(36) '$.Id',
                                StartDateUTC datetime '$.StartDateUTC', 
                                EndDateUTC datetime '$.EndDateUTC',
                                FromIntersectionName varchar(50) '$.FromIntersectionName',
                                ToIntersectionName varchar(50) '$.ToIntersectionName',
                                Direction varchar(15) '$.Direction',
                                MaxDurationMinutes int '$.MaxDurationMinutes',
                                StartSegmentOffsetMiles float '$.StartSegmentOffsetMiles',
                                EndSegmentOffsetMiles float '$.EndSegmentOffsetMiles',
                                MaxLengthMiles float '$.MaxLengthMiles',
                                FromPointLat float '$.FromPointLat',
                                FromPointLon float '$.FromPointLon',
                                ToPointLat float '$.ToPointLat',
                                ToPointLon float '$.ToPointLon',
                                StartDateET datetime '$.StartDateET',
                                EndDateET datetime '$.EndDateET',
                                StartHourET int '$.StartHourET',
                                StartMinuteET int '$.StartMinuteET',
                                EndHourET int '$.EndHourET',
                                EndMinuteET int '$.EndMinuteET',
                                TotalImpactFactor float '$.TotalImpactFactor',
                                "0" float '$."0"',
                                "1" float '$."1"',
                                "2" float '$."2"',
                                "3" float '$."3"',
                                "4" float '$."4"',
                                "5" float '$."5"',
                                "6" float '$."6"',
                                "7" float '$."7"',
                                "8" float '$."8"',
                                "9" float '$."9"',
                                "10" float '$."10"',
                                "11" float '$."11"',
                                "12" float '$."12"',
                                "13" float '$."13"',
                                "14" float '$."14"',
                                "15" float '$."15"',
                                "16" float '$."16"',
                                "17" float '$."17"',
                                "18" float '$."18"',
                                "19" float '$."19"',
                                "20" float '$."20"',
                                "21" float '$."21"',
                                "22" float '$."22"',
                                "23" float '$."23"'
                            )
                        """)
    shutil.move(file, os.getcwd() + '\\processed_csvs')


def convert_to_long_format(df: pd.DataFrame, value_name: str) -> pd.DataFrame:
    """Returns the given dataframe with the hour columns converted to long format"""
    return df.melt(id_vars=[c for c in df.columns if c not in [str(i) for i in range(0, 24)]],
                   value_vars=[str(i) for i in range(0, 24)], var_name='Hour', value_name=value_name)


def day_sum_to_wh(mode):
    if mode == 'counts':
        db_table = '[dbo].[275Bottlenecks_Count]'
        sql_file = 'get_bn_counts.sql'
        day_column = 'TotalCount'
        hour_column = 'Count'
        value_type = 'int'
    elif mode == 'impact_factor':
        db_table = '[dbo].[275Bottlenecks_ImpactFactor]'
        sql_file = 'get_if_totals.sql'
        day_column = 'TotalImpactFactor'
        hour_column = 'ImpactFactor'
        value_type = 'float'
    else:
        logger.log('ERROR', "Invalid mode entered for day_sum_to_wh. Enter 'counts' or 'impact_factor'.")
        sys.exit()

    def _sum_hours(row):
        hour_sum = 0
        for hour in range(0, 24):
            hour_sum += row[str(hour)]
        return hour_sum

    engine.execute(f'DELETE FROM {db_table}')

    with open(os.getcwd() + '\\sql\\' + sql_file, 'r') as query:
        counts = pd.read_sql_query(query.read(), engine)
    counts[day_column] = counts.apply(lambda x: _sum_hours(x), axis=1)
    counts_long = convert_to_long_format(counts, hour_column)
    counts_long['DateET'] = counts_long.apply(lambda y: y['DateET'].isoformat(), axis=1)

    with engine.begin() as con:
        for i in tqdm(range(ceil(counts_long.shape[0] / 1000) + 1)):
            json_string = json.dumps(counts_long.iloc[i * 1000: (i + 1) * 1000].to_dict('records'))
            con.execute(f"""
                        INSERT INTO {db_table}
                        SELECT *
                        FROM OPENJSON('{json_string}')
                            WITH (
                                DateET date '$.DateET',
                                {day_column} {value_type} '$.{day_column}',
                                Hour int '$.Hour',
                                {hour_column} {value_type} '$.{hour_column}'
                            )
                        """)
