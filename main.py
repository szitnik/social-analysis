# -*- coding: UTF-8 -*-
from __future__ import print_function
import mysql.connector
from flask import Flask
from flask import redirect
from flask import url_for
from flask import request
from flask import render_template
from flask import send_from_directory
from flask_googlemaps import GoogleMaps
from flask_googlemaps import Map
from collections import OrderedDict
from datetime import datetime, timedelta
import dateutil
import random
import time
from langdetect import DetectorFactory
DetectorFactory.seed = 0
import json_search
import json_search_location
import search
import config
import re
import methods_filldata
import methods_dashboard
import methods_createjson
import methods_keystats
import lang_sent_gend

app = Flask(__name__)
GoogleMaps(app)

@app.route('/')
def query_form():
    '''
    App for database fill, creating json file with posts, dashboard and statistics..
    '''
    return render_template("main.html")

@app.route('/', methods=['POST'])
def query_form_post():
    '''
    Basic selection form: fill database, analyze from database, create json, see dashboard and statistics.
    '''
    choose = request.form['choose']
    if choose == 'fill':
        return redirect(url_for('filldata'))
    elif choose == 'lsg':
        return redirect(url_for('analyze_lsg'))
    elif choose == 'json':
        return redirect(url_for('createjson'))
    elif choose == 'dashboard':
        return redirect(url_for('dashboard'))
    else:
        return redirect(url_for('stats'))

@app.route('/analyze_lsg/', methods=['GET', 'POST'])
def analyze_lsg():
    '''
    Language, sentiment and gender detection for posts from database.
    '''
    if request.method == 'POST':
        query = request.form['query']
        from_date = request.form['date_from']
        to_date = str(request.form['date_to'])+' 23:59:59'
        media = request.form['media']

        lang_sens = float(request.form['lang_sens'])
        sent_sens = float(request.form['sent_sens'])
        gender_sens = float(request.form['gend_sens'])
        number_of_predictions = float(request.form['gend_nr_predictions'])
        percent_of_same = float(request.form['gend_pr_predictions'])

        lang_sent_gend.set_lsg(query, media, from_date, to_date, lang_sens, sent_sens, gender_sens, number_of_predictions, percent_of_same)

        return 'Analiza jezika, sentimenta in spola za "'+query+'" v mediju "'+str(media)+ '" med datumoma '+str(from_date)+' in ' + str(to_date)+'.'
    else:
        dateWeek = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        dateToday = datetime.now().strftime("%Y-%m-%d")
        return render_template('lsg_form.html', dateWeek=dateWeek, dateToday=dateToday)

@app.route('/createjson/', methods=['GET', 'POST'])
def createjson():
    '''
    Basic selection form for creating json file of posts.
    '''
    if request.method == 'POST':
        return_files_path = config.return_files_path
        text = request.form['query']
        date_from = request.form['date_from']
        date_to = request.form['date_to']
        text1 = text.encode('utf-8')
        search_method = request.form['search']
        r = dict(request.form)
        soc_media = r.get('soc_media')
        files = request.form['write']
        lang_sens = float(request.form['lang_sens'])
        sent_sens = float(request.form['sent_sens'])
        name_sens = float(request.form['gend_sens'])
        text_sens = float(request.form['gend_sens'])
        number_of_predictions = float(request.form['gend_nr_predictions'])
        percent_of_same = float(request.form['gend_pr_predictions'])

        posts_length = []
        if files == 'one_file':
            file = return_files_path + text1 + '-' + str(random.randint(1, 100)) + '.json'
            file_to_return = file
        else:
            rnd = random.randint(1, 100)
            file = [return_files_path + sm + '-' + text1 + '-' + str(rnd) + '.json' for sm in soc_media]
            file += [return_files_path + '' + text1 + '-' + str(rnd) + '.zip']
            file_to_return = file[-1]

        if search_method == 'search_location':
            location = request.form['location']
            lat, long, distance = location.split(', ')
            for i in range(len(soc_media)):
                sm = soc_media[i]
                first, last = 0, 0
                if i == 0:
                    first = 1
                if i == len(soc_media) - 1:
                    last = 1
                posts_length.append((methods_createjson.srch_location_json(text1, lat + ',' + long, distance, sm, file, date_from, date_to,
                                    first, last, lang_sens, sent_sens, name_sens, text_sens, number_of_predictions, percent_of_same), sm))
            lang_detected = json_search_location.lang_detect
            sentiment_detected = json_search_location.sentiment_detect
            gender_detect = len(json_search_location.users_with_gender)
        else:
            for i in range(len(soc_media)):
                sm = soc_media[i]
                first, last = 0, 0
                if i == 0:
                    first = 1
                if i == len(soc_media) - 1:
                    last = 1
                posts_length.append((methods_createjson.srch_json(text1, sm, file, date_from, date_to, first, last, lang_sens, sent_sens,
                                    name_sens, text_sens, number_of_predictions, percent_of_same), sm))
            lang_detected = json_search.lang_detect
            sentiment_detected = json_search.sentiment_detect
            gender_detect = len(json_search.users_with_gender)

        all_posts = sum([n for n, m in posts_length])
        lang_deteted_percent = round(float(lang_detected) / all_posts, 3) * 100
        sent_deteted_percent = round(float(sentiment_detected) / all_posts, 3) * 100
        gend_detected_percent = round(float(gender_detect) / all_posts, 3) * 100

        iskanje = 'navadno iskanje' if search_method == 'search' else 'iskanje po lokaciji'
        objave = ''
        for i, m in posts_length:
            objave += "%s objav na %s <br/>" % (str(i), m)
        final_message = 'Za ' + iskanje + ' po kljucni besedi \"' + text + '\" med ' + date_from + ' in ' + date_to + ' je bilo najdenih:<br/>' + objave + '' \
            'Jezik je bil dolocen ' + str(lang_detected) + ' objavam od ' + str(all_posts) + ' (' + str(lang_deteted_percent) + '%) pri obcutljivosti ' + str(lang_sens) + '. <br/> ' \
            'Sentiment je bil dolocen ' + str(sentiment_detected) + ' objavam od ' + str(all_posts) + ' (' + str(sent_deteted_percent) + '%) pri obcutljivosti ' + str(sent_sens) + '. <br/> ' \
            'Spol je bil dolocen ' + str(gender_detect) + ' objavam od ' + str(all_posts) + ' (' + str(gend_detected_percent) + '%) pri obcutljivosti ' + str(name_sens) + '/' \
             + str(number_of_predictions) + '/' + str(percent_of_same) + '. <br/> <br/> ' \
              'Podatki so shranjeni v datoteki ' + file_to_return.split('/')[-1] + '. Prenos datoteke: '

        json_search.lang_detect, json_search.sentiment_detect = 0, 0
        return render_template('json_download.html', text=final_message, file_to_return=file_to_return.split('/')[-1])
    else:
        dateWeek = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        dateToday = (datetime.now()).strftime("%Y-%m-%d")
        return render_template("json_form.html", dateWeek=dateWeek, dateToday=dateToday)

@app.route('/return-files/<path:filename>', methods=['GET'])
def download(filename):
    '''
    Download json file.
    '''
    return_files_path = config.return_files_path
    return send_from_directory(return_files_path, filename)

@app.route('/dashboard/', methods=['GET', 'POST'])
def dashboard():
    '''
    Basic selection form for dashboard.
    '''
    if request.method == 'POST':
        query = request.form['query']
        from_date = request.form['date_from']
        to_date = str(request.form['date_to'])+' 23:59:59'

        # izpis zadnjih 20 postov za izbran query iz izbranih medijev (potem je na voljo izbira 10, 15 ali 20)
        # print last 20 posts for selected query from selected media (possible choices are last 10, 15 or 20 posts)
        limit = 20  # request.form['limit']
        comm_limit = 5
        r = dict(request.form)
        media = r.get('soc_media')
        results = {}
        start_time_topresults = time.clock()
        for m in media:
            res_media = methods_dashboard.get_latest(m, query, int(limit), comm_limit)
            if len(res_media) > 0:
                results[m.upper()] = methods_dashboard.get_latest(m, query, int(limit), comm_limit)
        results1 = OrderedDict(sorted(results.items(), key=lambda t: t[0]))
        finish_time_topresults = time.clock() - start_time_topresults

        # graf števila postov po socialnih medijih od izbranega datuma
        # post number chart over selected media from selected date
        start_time_chartsocmedia = time.clock()
        labels_pie, values_pie = methods_dashboard.chart_soc_media(media, query, from_date, to_date)
        finish_time_chartsocmedia = time.clock() - start_time_chartsocmedia
        colors = ['#F7464A', '#46BFBD', '#FDB45C', '#FEDCBA', '#ABCDEF', '#DDDDDD', '#ABCABC', '#ABBCAC']
        picked_colors = random.sample(set(colors), len(media))
        submits = [query, limit, media]

        # graf pojavljanja izbrane ključne besede po izbranih medijih za zadnji teden, 14 dni ali mesec
        # keyword chart over selected media for last week, two weeks or last month
        start_time_chartovertime = time.clock()
        data7, counts7, data14, counts14, dataM, countsM = methods_dashboard.chart_frequency(query, media)
        finish_time_chartovertime = time.clock() - start_time_chartovertime

        # graf najbolj dejavnih uporabnikov za objave z izbrano ključno besedo po izbranih medijih od izbranega datuma
        # most active users for selected keyword over selected media from selected date
        start_time_topusers = time.clock()
        userData, userCounts = methods_dashboard.get_best_users(query, media, from_date, to_date)
        finish_time_topusers = time.clock() - start_time_topusers

        # reach in engagement za objave z izbrano ključno besedo od izbranega datuma po izbranih medijih
        # reach and engagement form posts with selected keyword from selected date
        start_time_reach = time.clock()
        reach = get_query_reach(media, query, from_date, to_date)
        finish_time_reach = time.clock() - start_time_reach

        start_time_engagement = time.clock()
        engagement = get_query_engagement(media, query, from_date, to_date)
        finish_time_engagement = time.clock() - start_time_engagement

        # zemljevid za objave z izbrano ključno besedo od izbranega datuma po izbranih medijih, VSEH prihajajočih dogodkov ter VSEH lokacij, ki so v bazi
        # map of posts with selected keyword from selected date for selected media, ALL comming events and ALL location
        date_today = datetime.now().strftime("%Y-%m-%d")
        start_time_map = time.clock()
        coordinates, center_lat, center_long = methods_dashboard.get_posts_coordinates(query, media, from_date, to_date)
        events = methods_dashboard.get_events(date_today)
        venues, center_lat_venues, center_long_venues = methods_dashboard.get_venues()
        if center_lat == 0:
            center_lat = center_lat_venues
            center_long = center_long_venues
        merged_coordinates = coordinates + events + venues
        sndmap = Map(
            identifier="sndmap",
            lat=center_lat,
            lng=center_long,
            style="height:290px;width:520px;margin:0;",
            markers=merged_coordinates
        )
        finish_time_map = time.clock() - start_time_map

        # wordcloud za besede, ki se v objavah od izbranega datuma pojavljajo ob izbrani ključni besedi
        # wordcloud of words, which appear with selected keyword from selected date in posts from selected media
        nr = random.randint(1, 1000)
        filename = 'wordcloud-' + query + '-' + str(nr) + '.png'
        start_time_wordcloud = time.clock()
        methods_dashboard.create_word_cloud(media, query, filename, from_date, to_date)
        finish_time_wordcloud = time.clock() - start_time_wordcloud

        # graf po državah za objave z izbrano ključno besedo v izbranih socialnih medijih in objavljene od izbranega datuma
        # country chart for posts with selected keyword from selected date in posts from selected media
        start_time_country = time.clock()
        countryData, countryCounts, per_country = methods_dashboard.get_nr_countries(media, query, from_date, to_date)
        finish_time_country = time.clock() - start_time_country
        colorsCountry = ['#F7464A', '#46BFBD', '#FDB45C', '#FEDCBA', '#ABCDEF', '#DDDDDD', '#ABCABC', '#ABBCAC',
                         '#b7464A', '#C6BFBD']

        # graf po jezikih za objave z izbrano ključno besedo v izbranih socialnih medijih in objavljene od izbranega datuma
        # language chart for posts with selected keyword from selected date in posts from selected media
        start_time_language = time.clock()
        languageData, languageCounts = methods_dashboard.get_languages(query, media, from_date, to_date)
        finish_time_language = time.clock() - start_time_language

        print("Time chart over time: " + str(finish_time_chartovertime) + "\n"
                "Time chart social media: " + str(finish_time_chartsocmedia) + "\n"
                "Time top results: " + str(finish_time_topresults) + "\n"
                "Time top users: " + str(finish_time_topusers) + "\n"
                "Time reach: " + str(finish_time_reach) + "\n"
                "Time engagement: " + str(finish_time_engagement) + "\n"
                "Time map: " + str(finish_time_map) + "\n"
                "Time wordcloud: " + str(finish_time_wordcloud) + "\n"
                "Time chart country: " + str(finish_time_country) + "\n"
                "Time chart languages: " + str(finish_time_language) + "\n"
                 "Total mentions: " + str(sum([sum(x) for x in values_pie.tolist()])))

        return render_template('dashboard.html', values=values_pie.tolist(), labels=list(labels_pie), colors=picked_colors,
                               results=results1, submits=submits, columns=len(results), limit=10,
                               values7=counts7.tolist(), labels7=list(data7),
                               values14=counts14.tolist(), labels14=list(data14),
                               valuesM=countsM.tolist(), labelsM=list(dataM),
                               valuesUser=userCounts.tolist(), labelsUser=list(userData),
                               reach=reach, engagement=engagement,
                               mention=sum([sum(x) for x in values_pie.tolist()]), sndmap=sndmap, filename=filename,
                               valuesCountry=countryCounts.tolist(), labelsCountry=list(countryData),
                               colorsCountry=colorsCountry, per_country=per_country,
                               valuesLanguage=languageCounts.tolist(), labelsLanguage=list(languageData))
    else:
        dateMonth = (datetime.now() - dateutil.relativedelta.relativedelta(months=1)).strftime("%Y-%m-%d")
        dateToday = datetime.now().strftime("%Y-%m-%d")
        return render_template('dashboard_form.html', dateMonth=dateMonth, dateToday=dateToday)

@app.route('/stats/', methods=['GET', 'POST'])
def stats():
    '''
    Keyword analysis/statistics for selected keywords between particular dates.
    '''
    if request.method == 'POST':
        query = request.form['query']
        from_date = request.form['date_from']
        to_date = str(request.form['date_to'])+' 23:59:59'
        r = dict(request.form)
        media = r.get('soc_media')

        keyword_list = methods_keystats.get_keywords(query)
        stats = {key: {} for key in keyword_list}
        for keyword in keyword_list:
            print("racunanje statistike: "+ str(keyword))
            stats[keyword] = methods_keystats.get_statistics(keyword, from_date, to_date, media)

        keyword_sum = {key: 0 for key in stats.keys()}
        for key in stats.keys():
            summary = 0
            for key1 in stats[key].keys():
                summary += sum(stats[key][key1].values())
            keyword_sum[key] = summary

        statistics = ['post', 'user', 'comment']
        media_sum = {key: {s: 0 for s in statistics} for key in media}
        for key in stats.keys():
            for s in statistics:
                for m in media:
                    if (m in stats[key][s].keys()):
                        media_sum[m][s] += stats[key][s][m]

        keyword_list = methods_keystats.get_keywords(query)
        reach_engage = {key: [] for key in keyword_list}
        for keyword in keyword_list:
            print("racunanje reach in engagement: "+ str(keyword))
            reach_engage[keyword].append(get_query_reach(media, keyword, from_date, to_date))
            reach_engage[keyword].append(get_query_engagement(media, keyword, from_date, to_date))

        frequency = {key: [] for key in keyword_list}
        for keyword in keyword_list:
            print("racunanje chart frequency: "+ str(keyword))
            frequency[keyword] = methods_keystats.chart_frequency(media, keyword, from_date)

        nr_key = dict(zip(keyword_list, range(len(keyword_list))))

        return render_template('keystats.html', stats=stats, media=media, keyword_sum=keyword_sum,
                               media_sum=media_sum, reach=reach_engage, frequency=frequency, nr_key=nr_key)
    else:
        dateWeek = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        dateToday = datetime.now().strftime("%Y-%m-%d")

        return render_template('keystats_form.html', dateWeek=dateWeek, dateToday=dateToday)

@app.route('/filldata/', methods=['GET', 'POST'])
def filldata():
    '''
    Form for database fill: get data from social media APIs and fill database
    (basic search, user search and location search).
    '''
    if request.method == 'POST':
        if request.form['blogger'] == '':
            text = request.form['query']
            date_from = request.form['date']
            text1 = text.encode('utf-8')
            search_method = request.form['search']
            r = dict(request.form)
            soc_media = r.get('soc_media')
            if search_method == 'search_user':
                methods_filldata.srch_user(text, soc_media)
            elif search_method == 'search_location':
                location = request.form['location']
                lat, long, distance = location.split(', ')
                methods_filldata.srch_location(text, lat+','+long, distance, soc_media)
            else:
                methods_filldata.srch(text1, soc_media, date_from)
        else:
            blogger_url = request.form['blogger']
            date_from = request.form['date']
            search.blogger_mysql(blogger_url, date_from)
            text = blogger_url
            search_method = 'all posts from blog'
            soc_media = 'blogger'
        return text+' '+search_method+' '+str(soc_media)
    else:
        dateWeek = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        return render_template('filldata_form.html', dateWeek=dateWeek)

if __name__ == '__main__':
    app.run()

def get_query_engagement(soc_media, query, date_from, date_to):
    '''
    Engagement for posts with selected keyword in selected media between particular dates. Engagement is computed with adding up
    likes, dislikes, shared and comments of posts.
    :param soc_media: selected media
    :param query: selected keyword
    :param date_from: the date to search from in format 'Y-M-D'
    :param date_to: the date to search to in format 'Y-M-D'
    :return: engagement number
    '''
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()
    media = get_soc_media(soc_media, cursor)

    sql_query = "SELECT coalesce(SUM("+config.database+".`post`.`like_count`), 0)+coalesce(SUM("+config.database+".`post`.`dislike_count`), 0)+ " \
                "coalesce(SUM("+config.database+".`post`.`share_count`), 0)+coalesce(SUM("+config.database+".`post`.`comment_count`), 0)" \
                "FROM "+config.database+".`post` " \
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
    result = cursor.fetchall()
    cursor.close()
    cnx.close()

    engagement = 0
    if result[0][0] is not None:
        engagement = int(result[0][0])
    return engagement

def get_query_reach(soc_media, query, date_from, date_to):
    '''
    Reach for posts with selected keyword in selected media between particular dates. Reach is computed with adding up
    followers and friends of users, which published posts with selected keyword.
    :param soc_media: selected media
    :param query: selected keyword
    :param date_from: the date to search from in format 'Y-M-D'
    :param date_to: the date to search to in format 'Y-M-D'
    :return: reach number
    '''
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()
    twitter_reach = [[0]]
    youtube_reach = [[0]]
    facebook_reach = [[0]]
    if 'twitter' in soc_media:
        media = [2]
        sql_query = "SELECT SUM("+config.database+".`user`.`follower_count`)+SUM("+config.database+".`user`.`friend_count`) " \
                    "FROM "+config.database+".`post` " \
                    "LEFT JOIN "+config.database+".`user` ON "+config.database+".`post`.`user`="+config.database+".`user`.`id` " \
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
        params.append(str(date_to) + ' 23:59:59')
        params.extend(like_params)
        cursor.execute(sql_query_final, tuple(params))
        twitter_reach = cursor.fetchall()
    if 'facebook' in soc_media:
        media = [1]
        sql_query = "SELECT SUM("+config.database+".`group`.`fan_count`) " \
                    "FROM "+config.database+".`post` " \
                    "LEFT JOIN "+config.database+".`group` ON "+config.database+".`post`.`group_id`="+config.database+".`group`.`id` " \
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
        params.append(str(date_to) + ' 23:59:59')
        params.extend(like_params)
        cursor.execute(sql_query_final, tuple(params))
        facebook_reach = cursor.fetchall()
    if 'youtube' in soc_media:
        media = [5]
        sql_query = "SELECT SUM("+config.database+".`post`.`view_count`) " \
                    "FROM "+config.database+".`post` " \
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
        params.append(str(date_to) + ' 23:59:59')
        params.extend(like_params)
        cursor.execute(sql_query_final, tuple(params))
        youtube_reach = cursor.fetchall()
    cursor.close()
    cnx.close()
    reach = int(twitter_reach[0][0] or 0) + int(facebook_reach[0][0] or 0) + int(youtube_reach[0][0] or 0)
    return reach

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
