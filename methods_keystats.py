# -*- coding: UTF-8 -*-
from __future__ import print_function
import mysql.connector
from collections import Counter
from datetime import datetime, timedelta
import dateutil
import numpy as np
import pandas
import re
import config
from langdetect import DetectorFactory
DetectorFactory.seed = 0

def get_keywords(query):
    '''
    Create lsit of keywords from text field (keywords must be seperated with comma and space.
    :param query: keywords from text field
    :return: list of keywords
    '''
    return [w.strip() for w in query.split(', ')]

def get_statistics(query, date_from, date_to, soc_media):
    '''
    Get statistics for selected keywords, posts between particular dates, from particular social media.
    :param query: search keyword
    :param date_from: date to search posts from
    :param date_to: date to search posts to
    :param soc_media: selected social media
    :return: statistics (number of posts, users and comments, inserted in the database for particular keywords
    '''
    stats = {'post':[], 'user':[], 'comment':[]}
    for stat in stats.keys():
        stats[stat] = get_counts(stat, query, date_from, date_to, soc_media)
    return stats

def get_counts(stat, query, date_from, date_to, soc_media):
    '''
    Get data from teh database,
    :param stat: post, user or comment
    :param query: search keyword
    :param date_from: date to search posts from
    :param date_to: date to search posts to
    :param soc_media: selected social media
    :return: statistics for given keyword, from to date
    '''
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()
    media = get_soc_media(soc_media, cursor)
    if stat == 'post':
        sql_query = "SELECT "+config.database+".`social_media`.name, count(*) " \
                    "FROM "+config.database+".`"+stat+"` " \
                    "LEFT JOIN "+config.database+".`social_media` ON "+config.database+".`"+stat+"`.`soc_media`="+config.database+".`social_media`.`id` " \
                    "WHERE "+config.database+".`"+stat+"`.`soc_media` IN (%s) AND "+config.database+".`"+stat+"`.`creation_time` >= %s AND "+config.database+".`"+stat+"`.`creation_time` <= %s "
        results = get_results(cursor, sql_query, stat, query, media, date_from, date_to, group_by=' GROUP BY '+config.database+'.`social_media`.name')
    elif stat == 'comment':
        sql_query = "SELECT "+config.database+".`social_media`.name, count(*) " \
                    "FROM "+config.database+".`comment` " \
                    "LEFT JOIN "+config.database+".`social_media` ON "+config.database+".`comment`.`soc_media`="+config.database+".`social_media`.`id` " \
                    "WHERE "+config.database+".`comment`.`soc_media` IN (%s) AND "+config.database+".`comment`.`creation_time` >= %s AND "+config.database+".`comment`.`creation_time` <= %s "
        results = get_results(cursor, sql_query, stat, query, media, date_from, date_to, group_by=' GROUP BY '+config.database+'.`social_media`.name')
    else:
        sql_query = "SELECT "+config.database+".`user`.`id`, min("+config.database+".`comment`.`creation_time`), "+config.database+".`social_media`.name " \
                    "FROM "+config.database+".`user` " \
                    "LEFT JOIN "+config.database+".`comment` ON "+config.database+".`comment`.`user`="+config.database+".`user`.`id` " \
                    "LEFT JOIN "+config.database+".`social_media` ON "+config.database+".`user`.`soc_media`="+config.database+".`social_media`.`id` " \
                    "WHERE "+config.database+".`user`.`soc_media` IN (%s) AND "+config.database+".`comment`.`creation_time` >= %s AND "+config.database+".`comment`.`creation_time` <= %s "
        results1 = get_results(cursor, sql_query, stat, query, media, date_from, date_to, group_by=' GROUP BY '+config.database+'.`user`.id')

        sql_query = "SELECT "+config.database+".`user`.`id`, min("+config.database+".`post`.`creation_time`), "+config.database+".`social_media`.name " \
                    "FROM "+config.database+".`user` " \
                    "LEFT JOIN "+config.database+".`post` ON "+config.database+".`post`.`user`="+config.database+".`user`.`id` " \
                    "LEFT JOIN "+config.database+".`social_media` ON "+config.database+".`user`.`soc_media`="+config.database+".`social_media`.`id` " \
                    "WHERE "+config.database+".`user`.`soc_media` IN (%s) AND "+config.database+".`post`.`creation_time` >= %s AND "+config.database+".`post`.`creation_time` <= %s "
        results2 = get_results(cursor, sql_query, stat, query, media, date_from, date_to, group_by=' GROUP BY '+config.database+'.`user`.id')
        results = dict(Counter(results1) + Counter(results2))

    cursor.close()
    cnx.close()
    return results

def get_results(cursor, sql_query, stat, query, media, date_from, date_to, group_by):
    '''
    Create and execute sql query and get data from the database.
    :param cursor: connection to the database
    :param sql_query: SQL query to add desired where conditions
    :param stat: post, user or comment
    :param query: search keyword
    :param media: selected social media
    :param date_from: date to search posts from
    :param date_to: date to search posts to
    :param group_by: qroup by condition for sql query
    :return: dicitionary of results from the database
    '''
    sql_query_final, like_params = create_andor_query(sql_query, stat, query)
    sql_query_final += group_by
    in_media = ', '.join(map(lambda x: '%s', media))
    t = [in_media, '%s', '%s']
    for i in range(len(like_params)):
        t.append('%s')
    sql_query_final = sql_query_final % tuple(t)
    params = []
    params.extend(media)
    params.append(date_from)
    params.append(date_to)
    params.extend(like_params)
    if stat == 'post' or stat == 'comment':
        cursor.execute(sql_query_final, tuple(params))
        results = cursor.fetchall()
    else:
        new_sql_query_final = "SELECT nt1.name, count(*) as summ FROM (" + sql_query_final + ") as nt1 GROUP BY nt1.name";
        cursor.execute(new_sql_query_final, tuple(params))
        results = cursor.fetchall()
    return dict(results)

def create_andor_query(sql_query, stat, keywords):
    '''
    Update SQL query statement in where clause with multiple number of keywords separated with AND or OR
    (example: where keyword='keyword1' AND keyword='keyword2' OR keyword='keyword3').
    :param sql_query: SQL query to add desired where conditions
    :param stat: the name of SQL table
    :param keywords: keywords, separated with AND or OR
    :return: updated SQL query
    '''
    query = 'AND ' + keywords
    delimiters = ['OR', 'AND']
    values = re.split('|'.join(delimiters), query)
    values.pop(0)
    keys = re.findall('|'.join(delimiters), query)
    splitted = {'AND': [], 'OR': []}
    for k, v in zip(keys, values):
        splitted[k].append(v.strip())

    params = []
    t = 0
    for k in splitted:
        for i in splitted[k]:
            params.append(i)
            if t > 0:
                sql_query += k + ' '+config.database+'.`'+stat+'`.`keyword` = %s '
            else:
                sql_query += k + ' ('+config.database+'.`'+stat+'`.`keyword` = %s '
                t += 1
    sql_query += ')'
    return sql_query, params

def get_soc_media(soc_media, cursor):
    '''
    Get media consecutive number from its name.
    :param soc_media: selected media
    :param cursor: connection to the database
    :return: media numbers
    '''
    media = []
    for s in soc_media:
        cursor.execute(
            '''SELECT id FROM '''+config.database+'''.social_media WHERE name = %s''', (s,))
        m = cursor.fetchall()[0][0]
        media.append(int(m))
    return media

def chart_frequency(soc_media, query, date):
    '''
    Keyword frequency in posts from selected media for last 7, 14 and 30 days.
    :param soc_media: selected media
    :param query: selected keyword
    :return: posts
    '''
    date7 = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    date14 = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")
    dateM = (datetime.now() - dateutil.relativedelta.relativedelta(months=1)).strftime("%Y-%m-%d")

    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()
    media = get_soc_media(soc_media, cursor)
    sql_query = "SELECT "+config.database+".`post`.`id`, "+config.database+".`post`.`creation_time` " \
                "FROM "+config.database+".`post` " \
                "WHERE "+config.database+".`post`.`soc_media` IN (%s) AND "+config.database+".`post`.`creation_time` >= %s "
    sql_query_final, like_params = create_andor_query(sql_query, 'post', query)
    in_media = ', '.join(map(lambda x: '%s', media))
    t = [in_media, '%s']
    for i in range(len(like_params)):
        t.append('%s')
    sql_query_final = sql_query_final % tuple(t)
    params = []
    params.extend(media)
    params.append(date7)
    params.extend(like_params)
    cursor.execute(sql_query_final, tuple(params))
    results7 = cursor.fetchall()

    params = []
    params.extend(media)
    params.append(date14)
    params.extend(like_params)
    cursor.execute(sql_query_final, tuple(params))
    results14 = cursor.fetchall()

    params = []
    params.extend(media)
    params.append(dateM)
    params.extend(like_params)
    cursor.execute(sql_query_final, tuple(params))
    resultsM = cursor.fetchall()

    df7 = pandas.DataFrame(results7)
    data7, counts7 = get_counts_data(df7)
    df14 = pandas.DataFrame(results14)
    data14, counts14 = get_counts_data(df14)
    dfM = pandas.DataFrame(resultsM)
    dataM, countsM = get_counts_data(dfM)


    sql_query = "SELECT "+config.database+".`post`.`id`, "+config.database+".`post`.`creation_time` " \
                "FROM "+config.database+".`post` " \
                "WHERE creation_time IS NOT NULL AND " \
                ""+config.database+".`post`.`soc_media` IN (%s) AND "+config.database+".`post`.`creation_time` >= %s "
    sql_query_final, like_params = create_andor_query(sql_query, 'post', query)
    in_media = ', '.join(map(lambda x: '%s', media))
    t = [in_media, '%s']
    for i in range(len(like_params)):
        t.append('%s')
    sql_query_final = sql_query_final % tuple(t)
    params = []
    params.extend(media)
    params.append(date)
    params.extend(like_params)
    cursor.execute(sql_query_final, tuple(params))
    results = cursor.fetchall()
    cursor.close()
    cnx.close()

    frequencies = {}
    if results:
        df = pandas.DataFrame(results)
        df.columns = ['id', 'date']

        df_week = df.groupby([df['date'].map(lambda x: x.weekday())]).count()
        del df_week['id']
        countsW = df_week.date.values
        dataW = df_week.index.values

        df_hour = df.groupby(df['date'].map(lambda x: x.hour)).count()
        del df_hour['id']
        countsH = df_hour.date.values
        dataH = df_hour.index.values
        frequencies = {'week':[list(data7), counts7.tolist()], 'twoweeks':[list(data14), counts14.tolist()],
                       'month':[list(dataM), countsM.tolist()], 'day':[[str(d) for d in dataW], countsW.tolist()],
                       'hour':[[str(d) for d in dataH], countsH.tolist()]}
    return frequencies

def get_counts_data(df):
    '''
    Auxiliary function for computing keyword frequency in posts.
    :param df: dataframe to count for
    :return: data counts
    '''
    counts = np.ndarray(shape=(0, 0))
    data = []
    if len(df) > 0:
        df.columns = ['id', 'date']
        df['date'] = pandas.to_datetime(df['date']).dt.date
        df_week = df.groupby([df['date'].map(lambda x: x)]).count()
        del df_week['id']
        counts = df_week.date.values
        data = [d.strftime("%Y-%m-%d") for d in df_week.index.values]
    return data, counts

