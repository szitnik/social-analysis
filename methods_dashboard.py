# -*- coding: UTF-8 -*-
from __future__ import print_function
import mysql.connector
from collections import Counter
from pytagcloud import create_tag_image, make_tags
from pytagcloud.lang.counter import get_tag_counts
from pytagcloud.colors import COLOR_SCHEMES
from datetime import datetime, timedelta
import dateutil
from bs4 import BeautifulSoup
import numpy as np
import pandas
import re
import json
import langdetect
from langdetect import DetectorFactory
DetectorFactory.seed = 0
from nltk.corpus import stopwords
import config
import countries

# get latest post for print
def get_latest(soc_media, query, limit, limit_comm):
    '''
    Get last 10, 15 or 20 posts from the database. Posts must include selected keyword and appears in selected media.
    :param soc_media: selected media
    :param query: selected keyword
    :param limit: limit for number of posts to print
    :param limit_comm: limit the number of comments to print
    :return: posts
    '''
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()
    cursor.execute(
        '''SELECT id FROM '''+config.database+'''.social_media WHERE name = %s''', (soc_media,))
    media = cursor.fetchall()[0][0]

    sql_query = "SELECT post.id, post.text, post.creation_time, user.name " \
                "FROM "+config.database+".post LEFT JOIN "+config.database+".user on post.user = user.id " \
                "WHERE post.soc_media = %s "
    sql_query_final, like_params = create_andor_query(sql_query, 'post', query)
    t = ['%s']
    for i in range(len(like_params)):
        t.append('%s')
    sql_query_final = sql_query_final % tuple(t)
    sql_query_final += " ORDER BY post.creation_time DESC LIMIT %s "
    params = []
    params.extend(str(media))
    params.extend(like_params)
    params.append(limit)
    cursor.execute(sql_query_final, tuple(params))
    raw_results = cursor.fetchall()

    results = []
    for r in raw_results:
        cleantext = BeautifulSoup(r[1], 'lxml').text
        date = json.dumps(r[2].replace(microsecond=0).isoformat(' '))
        cursor.execute(
            '''SELECT comment.text, comment.creation_time, user.name
            FROM '''+config.database+'''.comment
            LEFT JOIN '''+config.database+'''.user on comment.user = user.id
            WHERE comment.post = %s ORDER BY comment.creation_time DESC LIMIT %s''', (r[0], limit_comm))
        comm = cursor.fetchall()
        results.append((r[0], cleantext, date, r[3], [[t, json.dumps(d.replace(microsecond=0).isoformat(' ')), a] for t, d, a in comm]))
    cursor.close()
    cnx.close()
    return results

def chart_soc_media(soc_media, query, date_from, date_to):
    '''
    Get data from the database for chart of posts per media. Posts must include selected keyword and appear in selected media between particular dates.
    :param soc_media: media
    :param query: search keyword
    :param date_from: date to search posts from
    :param date_to: date to search posts to
    :return: posts
    '''
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()
    media = get_soc_media(soc_media, cursor)
    sql_query = "SELECT .`post`.`id`, "+config.database+".`social_media`.`name` " \
                "FROM "+config.database+".`post` " \
                "LEFT JOIN "+config.database+".`social_media` ON "+config.database+".`post`.`soc_media`="+config.database+".`social_media`.`id` " \
                "WHERE "+config.database+".`social_media`.`id` IN (%s) AND "+config.database+".`post`.`creation_time` >= %s AND "+config.database+".`post`.`creation_time` <= %s "
    sql_query_final, like_params = create_andor_query(sql_query, 'post', query)
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
    cursor.execute(sql_query_final, tuple(params))
    results = cursor.fetchall()
    cursor.close()
    cnx.close()

    data = []
    counts = np.ndarray(shape=(0,0))
    if len(results) > 0:
        df = pandas.DataFrame(results)
        df.columns = ['post_id', 'soc_media']
        df_media = df.groupby('soc_media', as_index=True).count()
        counts = df_media.values
        data = df_media.index.values
    return data, counts

def chart_frequency(query, soc_media):
    '''
    Keyword frequency in posts from selected media for last 7, 14 and 30 days.
    :param query: selected keyword
    :param soc_media: selected media
    :return: posts
    '''
    date7 = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    date14 = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d")
    dateM = (datetime.now() - dateutil.relativedelta.relativedelta(months=1)).strftime("%Y-%m-%d")

    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()
    media = get_soc_media(soc_media, cursor)
    sql_query = "SELECT "+config.database+".`post`.`id`, "+config.database+".`post`.`creation_time`" \
                " FROM "+config.database+".`post`" \
                " WHERE "+config.database+".`post`.`soc_media` IN (%s) " \
                "AND "+config.database+".`post`.`creation_time` IS NOT NULL AND "+config.database+".`post`.`creation_time` >= %s "
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
    cursor.close()
    cnx.close()

    df7 = pandas.DataFrame(results7)
    data7, counts7 = get_counts_data(df7)
    df14 = pandas.DataFrame(results14)
    data14, counts14 = get_counts_data(df14)
    dfM = pandas.DataFrame(resultsM)
    dataM, countsM = get_counts_data(dfM)
    return data7, counts7, data14, counts14, dataM, countsM

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

def get_best_users(query, soc_media, date_from, date_to):
    '''
    Get the most active users, which published posts with selected keyword in selected media from between particular dates.
    :param query: selected keyword
    :param soc_media: selected media
    :param date_from: date to search posts from
    :param date_to: date to search posts to
    :return: top 20 users
    '''
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()

    media = get_soc_media(soc_media, cursor)
    sql_query = "SELECT `post`.`id`, `post`.`user`, CONCAT(`user`.`name`, ', ', `social_media`.`name`) AS name " \
               "FROM "+config.database+".`post` " \
               "LEFT JOIN "+config.database+".`user` ON "+config.database+".`post`.`user` = "+config.database+".`user`.`id` " \
               "LEFT JOIN "+config.database+".`social_media` ON "+config.database+".`post`.`soc_media` = "+config.database+".`social_media`.`id` " \
               "WHERE "+config.database+".`social_media`.`id` IN (%s) AND "+config.database+".`post`.`creation_time` >= %s AND "+config.database+".`post`.`creation_time` <= %s" \
                "AND `post`.`user` IS NOT NULL "
    sql_query_final, like_params = create_andor_query(sql_query, 'post', query)
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
    cursor.execute(sql_query_final, tuple(params))
    results = cursor.fetchall()
    cursor.close()
    cnx.close()

    data = []
    counts = np.ndarray(shape=(0, 0))
    if len(results) > 0:
        df = pandas.DataFrame(results)
        df.columns = ['post_id', 'user_id', 'user_name']
        del df['user_id']
        df_users = df.groupby('user_name', as_index=True).count().sort_values(['post_id'], ascending=False).head(20)
        counts = df_users.values
        data = df_users.index.values
    return data, counts

def get_posts_coordinates(query, soc_media, date_from, date_to):
    '''
    Connects with database and get posts, which have available coordinates to show them on map. Tweets are separated
    on the ones with traffic info and others. Posts must include selected keyword and appear in selected media between particular dates.
    :param query: selected keyword
    :param soc_media: selected media
    :param date_from: date to search posts from
    :param date_to: date to search posts to
    :return: map with icons for each post
    '''
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()
    media = get_soc_media(soc_media, cursor)
    sql_query = "SELECT "+config.database+".`post`.`id`, "+config.database+".`post`.`text`, "+config.database+".`post`.`creation_time`, "+config.database+".`post`.`coordinates` " \
                "FROM "+config.database+".`post` " \
                "WHERE "+config.database+".`post`.`soc_media` IN (%s) AND "+config.database+".`post`.`creation_time` >= %s AND "+config.database+".`post`.`creation_time` <= %s" \
                "AND `post`.`coordinates` IS NOT NULL "
    sql_query_final, like_params = create_andor_query(sql_query, 'post', query)
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
    cursor.execute(sql_query_final, tuple(params))
    results = cursor.fetchall()
    cursor.close()
    cnx.close()

    icon_green = 'http://maps.google.com/mapfiles/ms/icons/green-dot.png'
    icon_red = 'http://maps.google.com/mapfiles/ms/icons/red-dot.png'
    coordinates = []
    points = []
    for r in results:
        id, text, date, latlong = r
        lat, long = latlong.split(",")
        if lat < long:
            temp = lat
            lat = long
            long = temp
        words_re = re.compile("|".join(['nesreca', 'kolona', 'dars', 'zastoj']), re.IGNORECASE)
        if words_re.search(text):
            icon = icon_red
        else:
            icon = icon_green
        published = date.strftime("%Y-%m-%d %H:%M")
        coordinates.append({'icon': icon, 'lat': lat, 'lng': long, 'infobox': str(date)+': '+str(text.encode('utf-8'))})
        points.append([float(lat), float(long)])

    center_lat, center_long = 0, 0
    if len(points) > 0:
        x, y = zip(*points)
        center_lat = np.median(x)
        center_long = np.median(y)
    return coordinates, center_lat, center_long

def get_events(date):
    '''
    Connects with database and get facebook events, which have available coordinates to show them on map and
    which starts from selected date.
    :param date: selected from date
    :return: map with icons for each event
    '''
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()
    cursor.execute(
        '''SELECT id, name, start_time, place, coordinates
        FROM '''+config.database+'''.event WHERE coordinates IS NOT NULL AND start_time >= %s''', (date,))
    results = cursor.fetchall()
    cursor.close()
    cnx.close()

    icon = 'http://maps.google.com/mapfiles/ms/icons/blue-dot.png'
    coordinates = []
    for r in results:
        id, name, start_time, place, latlong = r
        lat, long = latlong.split(",")
        if lat < long:
            temp = lat
            lat = long
            long = temp
        text = name + ': ' + start_time.strftime("%Y-%m-%d %H:%M") + ': ' + place
        coordinates.append({'icon': icon, 'lat': lat, 'lng': long, 'infobox': str(text.encode('utf-8'))})
    return coordinates

def get_venues():
    '''
    Connects with database and get foursquare and tripadvisor venues, which have available coordinates to show them on map.
    :return: map with icons for each venue
    '''
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()
    cursor.execute(
        '''SELECT id, name, categories, coordinates FROM '''+config.database+'''.venue WHERE coordinates IS NOT NULL;''')
    results = cursor.fetchall()
    cursor.close()
    cnx.close()

    icon = 'http://maps.google.com/mapfiles/ms/icons/yellow-dot.png'
    coordinates = []
    points = []
    for r in results:
        id, name, categories, latlong = r
        lat, long = latlong.split(",")
        if lat < long:
            temp = lat
            lat = long
            long = temp
        coordinates.append({'icon': icon, 'lat': lat, 'lng': long, 'infobox': name})
        points.append([float(lat), float(long)])

    center_lat, center_long = 0, 0
    if len(points) > 0:
        x, y = zip(*points)
        center_lat = np.median(x)
        center_long = np.median(y)
    return coordinates, center_lat, center_long

def get_nr_countries(soc_media, query, date_from, date_to):
    '''
    Get countries, where the posts with selected keyword appear (in selected media between particular dates).
    :param query: selected keyword
    :param soc_media: selected media
    :param date_from: date to search posts from
    :param date_to: date to search posts to
    :return: top 10 countries
    '''
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()
    media = get_soc_media(soc_media, cursor)

    sql_query = "SELECT "+config.database+".`post`.`id`, "+config.database+".`location`.`name` " \
                "FROM "+config.database+".`post` " \
                "LEFT JOIN "+config.database+".`user` ON "+config.database+".`post`.`user`="+config.database+".`user`.`id` " \
                "LEFT JOIN "+config.database+".`location` ON "+config.database+".`user`.`location`="+config.database+".`location`.`id` " \
                "WHERE "+config.database+".`user`.`location` is not null AND "+config.database+".`post`.`soc_media` IN (%s) " \
                "AND "+config.database+".`post`.`creation_time` >= %s AND "+config.database+".`post`.`creation_time` <= %s "
    sql_query_final, like_params = create_andor_query(sql_query, 'post', query)
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
    cursor.execute(sql_query_final, tuple(params))
    results = cursor.fetchall()

    sql_query = "SELECT COUNT(*) " \
                "FROM "+config.database+".`post` " \
                "LEFT JOIN "+config.database+".`user` ON "+config.database+".`post`.`user`="+config.database+".`user`.`id` " \
                "LEFT JOIN "+config.database+".`location` ON "+config.database+".`user`.`location`="+config.database+".`location`.`id` " \
                "WHERE "+config.database+".`user`.`location` is not null AND "+config.database+".`post`.`soc_media` IN (%s) AND "+config.database+".`post`.`creation_time` <= %s " \
                "AND "+config.database+".`post`.`creation_time` >= %s "
    sql_query_final, like_params = create_andor_query(sql_query, 'post', query)
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
    cursor.execute(sql_query_final, tuple(params))
    counted = cursor.fetchall()

    sql_query = "SELECT COUNT(*) " \
                "FROM "+config.database+".`post` " \
                "LEFT JOIN "+config.database+".`user` ON "+config.database+".`post`.`user`="+config.database+".`user`.`id` " \
                "LEFT JOIN "+config.database+".`location` ON "+config.database+".`user`.`location`="+config.database+".`location`.`id` " \
                "WHERE "+config.database+".`post`.`soc_media` IN (%s) AND "+config.database+".`post`.`creation_time` >= %s AND "+config.database+".`post`.`creation_time` <= %s "
    sql_query_final, like_params = create_andor_query(sql_query, 'post', query)
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
    cursor.execute(sql_query_final, tuple(params))
    all = cursor.fetchall()
    cursor.close()
    cnx.close()

    per_country = 0
    if (counted[0][0] > 0) and (all[0][0]):
        per_country = round(float(counted[0][0]) / float(all[0][0]) * 100, 2)

    results = [[k.lower() for k in i] for i in results]

    country = map(lambda x: x.lower(), countries.countries)
    country.extend(countries.countries_map.keys())
    for r in results:
        for c in country:
            if c.decode('utf-8') in r[1]:
                ct = c
                if ct in countries.countries_map.keys():
                    ct = countries.countries_map[c]
                r[1] = ct

    df = pandas.DataFrame(results)
    counts = np.ndarray(shape=(0, 0))
    data = []
    if len(df) > 0:
        df.columns = ['post', 'country']
        df_country = df.groupby([df['country'].map(lambda x: x)]).count()
        del df_country['post']
        df_country_sorted = df_country.sort_values('country', ascending=False).head(10)
        counts = df_country_sorted.values
        data = df_country_sorted.index.values
    return data, counts, per_country

def create_word_cloud(soc_media, query, filename, date_from, date_to):
    '''
    Get wordcloud of words, which appear in posts together with selected keyword most frequently.
    :param query: selected keyword
    :param soc_media: selected media
    :param filename: filename of image to save wordcloud in
    :param date_from: date to search posts from
    :param date_to: date to search posts to
    '''
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()
    cursor.execute('SET NAMES utf8mb4')
    cursor.execute("SET CHARACTER SET utf8mb4")
    cursor.execute("SET character_set_connection=utf8mb4")
    media = get_soc_media(soc_media, cursor)
    sql_query = "SELECT "+config.database+".`post`.`text` " \
                "FROM "+config.database+".`post` " \
                "WHERE "+config.database+".`post`.`soc_media` IN (%s) AND "+config.database+".`post`.`creation_time` >= %s AND "+config.database+".`post`.`creation_time` <= %s"
    sql_query_final, like_params = create_andor_query(sql_query, 'post', query)
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
    cursor.execute(sql_query_final, tuple(params))
    results = cursor.fetchall()
    cursor.close()
    cnx.close()

    stopwords_set_english = set(stopwords.words("english"))
    stopwords_set_slovene = [word.replace(' ', '') for word in set(stopwords.words("slovenian"))]

    # slo_stops = [unicode(s, 'utf-8') for s in stops.slovene_stop_words]
    text_raw = ', '.join([r[0] for r in results])
    text = BeautifulSoup(text_raw.encode('utf-8', 'ignore'), 'lxml').text
    words = text.split()
    digits = re.compile(r"([\d.]*\d+)")
    urls = re.compile(r'(?:(?:http|https):\/\/)?([-a-zA-Z0-9.]{1,256}\.[a-z]{2,4})\b(?:\/[-a-zA-Z0-9@:%_\+.~#?&//=]*)?')
    meaningful_words = [
        w.lower().replace("#", "").replace("\"", "").replace("*", "").replace('&', '').replace('-', '').replace('(', '')
            .replace(')', '').replace('@', '').replace('.', '').replace(',', '').replace(':', '').replace('\u2026', '')
        for w in words if ((not w.lower() in stopwords_set_slovene) and (not w.lower() in stopwords_set_english) and
                           (not digits.match(w)) and (not urls.match(w.lower())) and len(w) > 2)]

    word_counts = Counter(meaningful_words)
    text = ''
    top20 = dict(word_counts.most_common(20))
    for i in top20.keys():
        text = text + (' ' + i) * top20[i]

    tags = make_tags(get_tag_counts(text), maxsize=50, minsize=10, colors=COLOR_SCHEMES['goldfish'])
    create_tag_image(tags, "C:/Users/Neli/Documents/socioPowerApps/static/images/" + filename, size=(650, 400), fontname='IM Fell DW Pica', layout='LAYOUT_HORIZONTAL', rectangular=True)
    # possible fonts: IM Fell DW Pica, Coustard, Josefin Sans, Neuton, Inconsolata, Cantarell, Reenie Beanie, Cuprum, Lobster, Crimson Text

def get_languages(query, soc_media, date_from, date_to):
    '''
    Get languages, where the posts with selected keyword appear (in selected media between particular dates).
    :param query: selected keyword
    :param soc_media: selected media
    :param date_from: date to search posts from
    :param date_to: date to search posts to
    :return: top 10 languages
    '''
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()
    media = get_soc_media(soc_media, cursor)

    sql_query = "SELECT " + config.database + ".`post`.`text` " \
                "FROM " + config.database + ".`post` " \
                "WHERE " + config.database + ".`post`.`soc_media` IN (%s) AND " + config.database + ".`post`.`creation_time` >= %s AND "+config.database+".`post`.`creation_time` <= %s"
    sql_query_final, like_params = create_andor_query(sql_query, 'post', query)
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
    cursor.execute(sql_query_final, tuple(params))
    results = cursor.fetchall()

    det = []
    for text in results:
        if len(text[0]) > 0:
            ld = language_detection(text[0])
            if len(ld) > 0:
                det.append(ld)

    det_ser = pandas.Series(det).value_counts().head(10)
    counts = det_ser.values
    data = det_ser.index.values
    return data, counts

def language_detection(text, sens=0.999):
    '''
    Language detection of given text.
    :param text: text
    :param sens: sensitivity for language detection, default is 0.999
    :return: determined language for given text
    '''
    try:
        ld = langdetect.detect_langs(text)
        ld_splitted = str(ld[0]).split(':')
        if float(ld_splitted[1]) > sens:
            return ld_splitted[0]
        else:
            return ''
    except:
        print('LangDetectException')
        return ''

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
