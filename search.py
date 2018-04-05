# -*- coding: utf-8 -*-
from __future__ import print_function
import mysql.connector
from googleapiclient.errors import HttpError
import methods
import pytz
from datetime import datetime
import re
import config
from bs4 import BeautifulSoup

import tweepy

def facebook_mysql(results, query, date):
    '''
    Facebook insert search results in mysql tables location, event, group, post, user and comment.
    :param results: the results from method 'methods.facebook_search'
    :param  query: search query keyword
    '''
    (events, groups, pages) = results
    soc_media = 1
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database, use_unicode=True)
    cursor = cnx.cursor()
    cursor.execute('SET NAMES utf8mb4')
    cursor.execute("SET CHARACTER SET utf8mb4")
    cursor.execute("SET character_set_connection=utf8mb4")

    # events
    fb_type = 'event'
    for event in events.get('data'):
        event_id = int(event.get('id'))

        event_url = 'http://www.facebook.com/'+str(event_id)
        event_name = event.get('name')
        event_description = event.get('description')
        event_start_time = event.get('start_time')
        event_end_time = event.get('end_time')
        if event.get('place') != None:
            event_place = event.get('place').get('name')
            if event.get('place').get('location') != None:
                event_coordinates = None
                event_city = None
                event_country = None
                if event.get('place').get('location').get('city') != None:
                    event_city = event.get('place').get('location').get('city')
                    id_location = None
                if event.get('place').get('location').get('country') != None:
                    event_country = event.get('place').get('location').get('country')
                    id_location = None
                if event.get('place').get('location').get('latitude') != None:
                    event_coordinates = str(event.get('place').get('location').get('latitude'))+','+str(event.get('place').get('location').get('longitude'))
                    id_location = None
                if event.get('place').get('location').get('city') != None and event.get('place').get('location').get('country') != None:
                    cursor.execute('''SELECT MAX(id) FROM location''')
                    id_location = cursor.fetchall()[0][0]
                    if not id_location:
                        id_location = 0
                    cursor.execute('''SELECT id FROM location WHERE city = %s AND country = %s''', (event_city, event_country))
                    location_already = cursor.fetchall()
                    if location_already:
                        id_location = location_already[0][0]
                    else:
                        id_location = str(int(id_location)+1)
                    cursor.execute('''
                                    INSERT IGNORE INTO location (id, name, city, country, keyword)
                                    VALUES (%s, %s, %s, %s, %s)''', (id_location, event_city+', '+event_country, event_city, event_country, query))
                    cnx.commit()
            else:
                id_location = None
                event_coordinates = None
        else:
            event_place = None
            id_location = None
            event_coordinates = None
        cursor.execute('''
                        INSERT IGNORE INTO event (id, name, description, start_time, end_time, place, coordinates, location, soc_media, keyword, url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                        (event_id, event_name, event_description, event_start_time, event_end_time, event_place, event_coordinates, id_location, soc_media, query, event_url))
        cnx.commit()
        posts = methods.facebook_get_egp_posts_since(event_id, since_date=date)
        facebook_post_insertion(posts, event_id, cnx, cursor, query, fb_type)

    # groups
    fb_type = 'group'
    for group in groups.get('data'):
        # group insertion
        group_id = int(group.get('id'))
        group_url = 'http://www.facebook.com/' + str(group_id)
        group_name = group.get('name')
        group_data = methods.facebook_get_group_info(group_id)
        group_about = group_data.get('about')
        group_privacy = group.get('privacy')
        cursor.execute('''
                        INSERT IGNORE INTO '''+config.database+'''.group (id, name, privacy, description, type, soc_media, keyword, url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE description = VALUES(description)''', (group_id, group_name, group_privacy, group_about, fb_type, soc_media, query, group_url))
        cnx.commit()
        # post and comment insertion
        posts = methods.facebook_get_egp_posts_since(group_id, since_date=date)
        facebook_post_insertion(posts, group_id, cnx, cursor, query, fb_type)

    # pages
    fb_type = 'page'
    for page in pages.get('data'):
        page_id = int(page.get('id'))
        page_url = 'http://www.facebook.com/' + str(page_id)
        page_name = page.get('name')
        page_fans = methods.facebook_get_page_fans(page_id)
        page_data = methods.facebook_get_page_info(page_id)
        page_description = page_data.get('description')
        page_privacy = page.get('privacy')
        cursor.execute('''
                        INSERT INTO '''+config.database+'''.group (id, name, privacy, description, type, fan_count, soc_media, keyword, url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE fan_count = VALUES(fan_count), description = VALUES(description)''', (page_id, page_name, page_privacy, page_description, fb_type, page_fans, soc_media, query, page_url))
        cnx.commit()
        # post and comment insertion
        posts = methods.facebook_get_egp_posts_since(page_id, since_date=date)
        facebook_post_insertion(posts, page_id, cnx, cursor, query, fb_type)

    cursor.close()
    cnx.close()

def facebook_post_insertion(posts, egp_id, cnx, cursor, keyword, fb_type):
    '''
    Facebook insert data about posts in mysql tables user, post and comment.
    :param posts: the list of posts from method 'facebook_mysql'
    :param egp_id: the identitifacion number of event/group/page
    :param cnx: mysql connector
    :param cursor: cnx cursor
    :param  keyword: search query keyword
    '''
    soc_media = 1
    type_fb = 'facebook post'
    for post in posts:
        # post insertion
        post_id = post.get('id')
        post_url = 'http://www.facebook.com/' + str(post_id)
        post_text = post.get('message')
        if post.get('created_time'):
            post_published = datetime.strptime(post.get('created_time'), '%Y-%m-%dT%H:%M:%S+0000').replace(tzinfo=pytz.UTC)
        else:
            post_published = datetime.strptime(post.get('updated_time'), '%Y-%m-%dT%H:%M:%S+0000').replace( tzinfo=pytz.UTC)
        cursor.execute('''
                    INSERT IGNORE INTO post (id, text, creation_time, soc_media, group_id, type, keyword, url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)''', (post_id, post_text, post_published, soc_media, egp_id, type_fb, keyword, post_url))

        cnx.commit()

        # comment insertion
        comments = methods.facebook_get_post_comments(post.get('id'))
        if comments:
            for comment in comments.get('data'):
                comment_user_id = comment.get('from').get('id')
                comment_user_name = comment.get('from').get('name')
                cursor.execute('''
                                        INSERT IGNORE INTO user (id, name, soc_media, keyword)
                                        VALUES (%s, %s, %s, %s)''', (comment_user_id, comment_user_name, soc_media, keyword))
                cnx.commit()
                comment_id = comment.get('id')
                comment_url = 'http://www.facebook.com/' + str(comment_id)
                comment_text = comment.get('message')
                comment_published = datetime.strptime(comment.get('created_time'), '%Y-%m-%dT%H:%M:%S+0000').replace(tzinfo=pytz.UTC)
                cursor.execute('''
                                    INSERT IGNORE INTO comment (id, text, creation_time, user, post, keyword, soc_media, url)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)''', (comment_id, comment_text, comment_published, comment_user_id, post_id, keyword, soc_media, comment_url))
                cnx.commit()

def twt_after_date(query, date, number_of_tweets=100):
    '''
    Twitter search and insert search results in mysql database.
    :param query: search keyword
    :param date: date to search after
    :param number_of_tweets: the number of maximal tweets per request, default is 100
    '''
    auth = tweepy.OAuthHandler(config.twitter_consumer_key, config.twitter_consumer_secret)
    auth.set_access_token(config.twitter_access_key, config.twitter_access_secret)
    api = tweepy.API(auth)

    results = []
    try:
        tweets = api.search(q=query, count=number_of_tweets, since=date)
    except tweepy.RateLimitError:
        print(query + ': Rate limit exceeded. Waiting.')
    results += tweets
    twitter_mysql(results, query)

    if len(tweets) > 0:
        before_id = int(tweets[-1]._json.get('id') - 1)
        contin = True
        while contin:
            results = []
            tweets = [1]
            while tweets:
                try:
                    tweets = api.search(q=query, count=number_of_tweets, max_id=int(before_id), since=date)
                    if len(tweets) == 0:
                        contin = False
                except tweepy.RateLimitError:
                    print(query + ': Rate limit exceeded. Waiting.')
                    tweets = []
                if len(tweets) > 0:
                    before_id = int(tweets[-1]._json.get('id') - 1)
                    results += tweets
            twitter_mysql(results, query)

def twitter_user_insertion(r, cnx, cursor, keyword):
    '''
    Twitter insert data about users in mysql table user and location.
    :param r: the user from method 'twitter_mysql'
    :param cnx: mysql connector
    :param cursor: cnx cursor
    :param  keyword: search query keyword
    '''
    soc_media = 2
    screen_name = r.get('screen_name')
    user_url = 'http://www.twitter.com/' + str(screen_name)
    name = r.get('name')
    user_id = r.get('id')
    followers = r.get('followers_count')
    tweets = r.get('statuses_count')
    description = r.get('description')
    friends = r.get('friends_count')
    created_at = datetime.strptime(r.get('created_at'),'%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC)
    time_zone = r.get('time_zone')
    language = r.get('lang')
    if r.get('location'):
        cursor.execute('''SELECT MAX(id) FROM location''')
        id_location = cursor.fetchall()[0][0]
        if not id_location:
            id_location = str(0)
        location = r.get('location')
        cursor.execute('''SELECT id FROM location WHERE name = %s''', (location,))
        location_already = cursor.fetchall()
        if location_already:
            id_location = location_already[0][0]
        else:
            id_location = str(int(id_location)+1)
        cursor.execute('''
                        INSERT IGNORE INTO location (id, name, keyword)
                        VALUES (%s, %s, %s)''', (id_location, location, keyword))
        cnx.commit()
    else:
        id_location = None
    cursor.execute('''
                    INSERT IGNORE INTO user (id, name, location, soc_media, screen_name, description, follower_count, post_count, friend_count, created_at, time_zone, language, keyword, url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', (user_id, name, id_location, soc_media,
                                                                                 screen_name, description, followers, tweets, friends, created_at, time_zone, language, keyword, user_url))
    cnx.commit()

def twitter_mysql(results, keyword):
    '''
    Twitter insert search results in mysql tables location, post, user and comment.
    :param results: the results from method 'methods.twitter_search'
    :param  keyword: search query keyword
    '''
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()

    soc_media = 2
    type_tw = 'tweet'
    for r in results:
        # user insertion
        twitter_user_insertion(r._json.get('user'), cnx, cursor, keyword)
        user_id = r._json.get('user').get('id')

        if r._json.get('in_reply_to_status_id') is None:
            # post insertion
            text = r._json.get('text')
            post_id = r._json.get('id')
            post_url = 'https://twitter.com/Interior/status/' + str(post_id) #933965528657473537 odveč
            favourites = r._json.get('favorite_count')
            retweets = r._json.get('retweet_count')
            tags = ', '.join(i.get('text') for i in r._json.get('entities').get('hashtags'))
            created = datetime.strptime(r._json.get('created_at'),'%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC)
            if r._json.get('coordinates'):
                coordinates = ','.join(str(e) for e in r._json.get('coordinates').get('coordinates'))
            else:
                coordinates = None
            if r._json.get('place'):
                place = r._json.get('place').get('country')
            else:
                place = None
            cursor.execute('''
                INSERT IGNORE INTO post (id, text, creation_time, user, soc_media, like_count, share_count, coordinates, place, tags, type, keyword, url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', (post_id, text, created, user_id, soc_media, favourites, retweets, coordinates, place, tags, type_tw, keyword, post_url))
            cnx.commit()
        else:
            text = r._json.get('text')
            reply_status_id = r._json.get('in_reply_to_status_id')
            reply_user_id = r._json.get('in_reply_to_user_id')
            comment_id = r._json.get('id')
            comment_url = 'https://twitter.com/Interior/status/' + str(comment_id)
            post_url = 'https://twitter.com/Interior/status/' + str(reply_status_id)
            favourites = r._json.get('favourite_count')
            retweets = r._json.get('retweet_count')
            tags = ', '.join(i.get('text') for i in r._json.get('entities').get('hashtags'))
            created = datetime.strptime(r._json.get('created_at'),'%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC)

            # user insertion
            reply_user_info = methods.twitter_get_user_info(reply_user_id)
            if not reply_user_info:
                reply_user_id = 'unknown-twitter'
                cursor.execute('''
                                        INSERT IGNORE INTO user (id, soc_media, keyword)
                                        VALUES (%s, %s, %s)''', (reply_user_id, soc_media, keyword))
                cnx.commit()
            else:
                twitter_user_insertion(reply_user_info._json, cnx, cursor, keyword)

            # post insertion
            cursor.execute('''
                INSERT IGNORE INTO post (id, text, user, soc_media, keyword, url)
                VALUES (%s, %s, %s, %s, %s, %s)''', (reply_status_id, " ", reply_user_id, soc_media, keyword, post_url))
            cnx.commit()

            # comment insertion
            cursor.execute('''
                INSERT IGNORE INTO comment (id, text, creation_time, like_count, share_count, user, post, reply_user_id, tags, keyword, soc_media, url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', (comment_id, text, created, favourites, retweets, user_id, reply_status_id, reply_user_id, tags, keyword, soc_media, comment_url))
            cnx.commit()
    cursor.close()
    cnx.close()

def tumblr_mysql(results, keyword):
    '''
    Tumblr insert search results in mysql tables user and post.
    :param results: he results from method 'methods.tumblr_search'
    '''
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()

    soc_media = 4
    type_tumblr = 'tumblr post'
    for r in results:
        # user insertion
        name = r.get('blog_name')
        user_url = str(name)+'.tumblr.com'
        #info = methods.tumblr_get_blog_info(name)
        #if info.get('errors') is None:
        description = ""#.get('blog').get('description') if info.get('blog').get('description') else ""
        posts = ""#info.get('blog').get('posts')
        screen_name = ""#info.get('blog').get('title')
        cursor.execute('''
                        INSERT IGNORE INTO user (id, name, soc_media, screen_name, post_count, description, keyword, url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)''', (name, name, soc_media, screen_name, posts, description, keyword, user_url))
        cnx.commit()

        #post insertion
        id = r.get('id')
        created = r.get('date')
        tags = ', '.join(str(e.encode('utf-8')) for e in r.get('tags'))
        post_url = r.get('short_url')
        if r.get('trail'):
            text = BeautifulSoup(r.get('trail')[0].get('content')).text
        else:
            text = None
        cursor.execute('''
            INSERT IGNORE INTO post (id, text, creation_time, user, soc_media, tags, type, keyword, url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)''', (id, text, created, name, soc_media, tags, type_tumblr, keyword, post_url))
        cnx.commit()
    cursor.close()
    cnx.close()

def googleplus_mysql(results, keyword):
    '''
    Google+ insert search results in mysql tables user, post and comment.
    :param results: the results from method 'methods.googleplus_search'
    :param  keyword: search query keyword
    '''
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()

    soc_media = 3
    type_gp = 'googleplus post'
    for r in results:
        # user insertion
        user_id = r.get('actor').get('id')
        name = r.get('actor').get('displayName')
        user_url = r.get('actor').get('url')
        cursor.execute('''
                        INSERT IGNORE INTO user (id, name, soc_media, keyword, url)
                        VALUES (%s, %s, %s, %s, %s)''', (user_id, name, soc_media, keyword, user_url))
        cnx.commit()

        #post insertion
        post_id = r.get('id')
        title = r.get('title')
        created = datetime.strptime(r.get('published'),'%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=pytz.UTC)
        text = r.get('object').get('content')
        likes = r.get('object').get('plusoners').get('totalItems')
        post_url = r.get('object').get('url')
        comments_count = r.get('object').get('replies').get('totalItems')
        post_tags = ', '.join(i for i in re.findall(r">#(\w+)", r.get('object').get('content')))
        cursor.execute('''
            INSERT IGNORE INTO post (id, text, creation_time, user, soc_media, like_count, comment_count, tags, type, keyword, url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', (post_id, title+' '+text, created, user_id, soc_media, likes, comments_count, post_tags, type_gp, keyword, post_url))
        cnx.commit()

        if comments_count > 0:
            #comments insertion
            comments = methods.googleplus_get_post_comments(post_id)
            for comment in comments.get('items'):
                comment_id = comment.get('id')
                created_comment = comment.get('published')
                likes_comment = comment.get('plusoners').get('totalItems')
                text_comment = comment.get('object').get('content')
                comment_url = comment.get('inReplyTo')[0].get('url')

                # user insertion
                comment_user_id = comment.get('actor').get('id')
                comment_user_name = comment.get('actor').get('displayName')
                comment_user_url = comment.get('actor').get('url')
                cursor.execute('''
                                INSERT IGNORE INTO user (id, name, soc_media, keyword, url)
                                VALUES (%s, %s, %s, %s, %s)''', (comment_user_id, comment_user_name, soc_media, keyword, comment_user_url))
                cnx.commit()

                # comment instertion
                cursor.execute('''
                    INSERT IGNORE INTO comment (id, text, creation_time, like_count, user, post, keyword, soc_media, url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)''', (comment_id, text_comment, created_comment, likes_comment, comment_user_id, post_id, keyword, soc_media, comment_url))
                cnx.commit()
    cursor.close()
    cnx.close()

def youtube_mysql(results, keyword):
    '''
    Youtube insert search results in mysql tables post, user and comment.
    :param results: the results about videos, channels and playlists from method 'methods.youtube_search'
    :param  keyword: search query keyword
    '''
    (videos, channels, playlists) = results
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()

    soc_media = 5
    # VIDEO
    for video in videos:
        # user insertion
        user_id = video.get('snippet').get('channelId')
        user_name = video.get('snippet').get('channelTitle')
        user_info = methods.youtube_get_channel_info(user_id)
        user_url = 'https://www.youtube.com/channel/'+str(user_id)
        if user_info['items']:
            country = user_info['items'][0].get('snippet').get('country')
            created = datetime.strptime(user_info['items'][0].get('snippet').get('publishedAt'),'%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=pytz.UTC)
            description = user_info['items'][0].get('snippet').get('description')
            friends = user_info['items'][0].get('statistics').get('subscriberCount')
            video_count = user_info['items'][0].get('statistics').get('videoCount')
            cursor.execute('''
                            INSERT IGNORE INTO user (id, name, soc_media, description, post_count, friend_count, created_at, time_zone, keyword, url)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', (user_id, user_name, soc_media, description, video_count, friends, created, country, keyword, user_url))
            cnx.commit()
        else:
            cursor.execute('''
                            INSERT IGNORE INTO user (id, name, soc_media, keyword, url)
                            VALUES (%s, %s, %s, %s, %s)''', (user_id, user_name, soc_media, keyword, user_url))
            cnx.commit()

        # post insertion
        title = video.get('snippet').get('title')
        published = datetime.strptime(video.get('snippet').get('publishedAt'),'%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=pytz.UTC)
        description = video.get('snippet').get('description')
        video_id = video.get('id').get('videoId')
        video_url = 'https://www.youtube.com/watch?v=' + str(video_id)
        youtube_type = 'video'
        video_info = methods.youtube_get_video_info(video_id)
        comments_count = 0
        dislikes = 0
        likes = 0
        views = 0
        tags = ''
        if len(video_info['items']) > 0:
            cc = video_info['items'][0].get('statistics').get('commentCount')
            comments_count = 0 if cc == None else int(cc)
            dislikes = video_info['items'][0].get('statistics').get('dislikeCount')
            likes = video_info['items'][0].get('statistics').get('likeCount')
            views = video_info['items'][0].get('statistics').get('viewCount')
            if video_info['items'][0].get('snippet').get('tags')is not None:
                tags = ', '.join(i for i in video_info['items'][0].get('snippet').get('tags'))
            else:
                tags = None
        cursor.execute('''
            INSERT IGNORE INTO post (id, text, creation_time, user, soc_media, description, like_count, dislike_count, comment_count, view_count, tags, type, keyword, url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', (video_id, title, published, user_id, soc_media, description, likes, dislikes, comments_count, views, tags, youtube_type, keyword, video_url))
        cnx.commit()

        # comment insertion
        if comments_count > 0:
            try:
                comments = methods.youtube_get_video_comments(video_id)
                for c in comments:
                    comment_user_name = c.get('snippet').get('topLevelComment').get('snippet').get('authorDisplayName')
                    if c.get('snippet').get('topLevelComment').get('snippet').get('authorChannelId'):
                        comment_user_id = c.get('snippet').get('topLevelComment').get('snippet').get('authorChannelId').get('value')
                    else:
                        comment_user_id = 'unknown'
                    cursor.execute('''
                            INSERT IGNORE INTO user (id, name, soc_media, keyword)
                            VALUES (%s, %s, %s, %s)''', (comment_user_id, comment_user_name, soc_media, keyword))
                    cnx.commit()
                    comment_id = c.get('snippet').get('topLevelComment').get('id')
                    comment_text = c.get('snippet').get('topLevelComment').get('snippet').get('textDisplay')
                    comment_created = datetime.strptime(c.get('snippet').get('topLevelComment').get('snippet').get('publishedAt'),'%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=pytz.UTC)
                    comment_likes = c.get('snippet').get('topLevelComment').get('snippet').get('likeCount')
                    cursor.execute('''
                        INSERT IGNORE INTO comment (id, text, creation_time, like_count, user, post, keyword, soc_media, url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)''', (comment_id, comment_text, comment_created, comment_likes, comment_user_id, video_id, keyword, soc_media, video_url))
                    cnx.commit()

                    replys = c.get('snippet').get('totalReplyCount')
                    if replys > 0 and c.get('replies') is not None:
                        for r in c.get('replies').get('comments'):
                            reply_user_name = r.get('snippet').get('authorDisplayName')
                            reply_user_id = r.get('snippet').get('authorChannelId').get('value')
                            cursor.execute('''
                                INSERT IGNORE INTO user (id, name, soc_media, keyword)
                                VALUES (%s, %s, %s, %s)''', (reply_user_id, reply_user_name, soc_media, keyword))
                            cnx.commit()
                            reply_id = r.get('id')
                            reply_text = r.get('snippet').get('textDisplay')
                            reply_created = datetime.strptime(r.get('snippet').get('publishedAt'),'%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=pytz.UTC)
                            reply_likes = r.get('snippet').get('likeCount')
                            cursor.execute('''
                                INSERT IGNORE INTO comment (id, text, creation_time, like_count, user, post, reply_user_id, keyword, soc_media, url)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', (reply_id, reply_text, reply_created, reply_likes, reply_user_id, video_id, comment_user_id, keyword, soc_media, video_url))
                            cnx.commit()
            except HttpError:
                print('Unknown HttpError.')

    # CHANNEL
    for channel in channels:
        channel_id = channel.get('snippet').get('channelId')
        channel_name = channel.get('snippet').get('title')
        channel_url = 'https://www.youtube.com/channel/'+str(channel_id)
        channel_created = datetime.strptime(channel.get('snippet').get('publishedAt'),'%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=pytz.UTC)
        channel_description = channel.get('snippet').get('description')
        cursor.execute('''
                        INSERT IGNORE INTO user (id, name, soc_media, description, created_at, keyword, url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)''', (channel_id, channel_name, soc_media, channel_description, channel_created, keyword, channel_url))
        cnx.commit()

    # PLAYLIST
    for playlist in playlists:
        # user/channel insertion
        channel_id = playlist.get('snippet').get('channelId')
        channel_name = playlist.get('snippet').get('channelTitle')
        cursor.execute('''
                    INSERT IGNORE INTO user (id, name, soc_media, keyword)
                    VALUES (%s, %s, %s, %s)''', (channel_id, channel_name, soc_media, keyword))
        cnx.commit()

        # playlist insertion
        title = playlist.get('snippet').get('title')
        created = playlist.get('snippet').get('publishedAt')
        description = playlist.get('snippet').get('description')
        playlist_id = playlist.get('id').get('playlistId')
        youtube_type = 'playlist'
        cursor.execute('''
            INSERT IGNORE INTO post (id, text, creation_time, user, soc_media, description, type, keyword)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)''', (playlist_id, title, created, channel_id, soc_media, description, youtube_type, keyword))
        cnx.commit()
    cursor.close()
    cnx.close()

def blogger_mysql(url, from_date='2016-01-01'):
    '''
    Blogger insert data about a particular blog (defined with url) in mysql tables user and post.
    :param url: the URL of a blog
    :param from_date: the date to search from in format 'Y-M-D', default is '2016-01-01'
    '''
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()

    soc_media = 7
    type_bg = 'blogger post'

    # user insertion
    blog_info = methods.blogger_get_blog_info_byurl(url)
    blog_id = blog_info.get('id')
    posts = blog_info.get('posts').get('totalItems')
    description = blog_info.get('description')
    created = blog_info.get('published')
    blog_name = blog_info.get('name')
    country = blog_info.get('locale').get('country')
    language = blog_info.get('locale').get('language')
    cursor.execute('''
                INSERT IGNORE INTO user (id, name, soc_media, description, post_count, created_at, language, time_zone, url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)''', (blog_id, blog_name, soc_media, description, posts, created, language, country, url))
    cnx.commit()

    # post insertion
    posts = methods.blogger_get_blog_posts(blog_id, from_date)
    for post in posts:
        post_id = post.get('id')
        text = post.get('content')
        title = post.get('title')
        post_url = post.get('url')
        published = post.get('published')
        replies = post.get('replies').get('totalItems')
        cursor.execute('''
            INSERT IGNORE INTO sociopower.post (id, text, creation_time, user, soc_media, comment_count, type, url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)''', (post_id, title+' '+text, published, blog_id, soc_media, replies, type_bg, post_url))
        cnx.commit()

    cursor.close()
    cnx.close()

