from __future__ import print_function
import mysql.connector
from datetime import datetime
import pytz
import config

def facebook_mysql(users, keyword):
    '''
    Facebook insert data about users in mysql table user.
    :param users: the list of users from method 'facebook_search_users'
    :param keyword: search query keyword
    '''
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()

    soc_media = 1
    for u in users.get('data'):
        name = u.get('name')
        id = u.get('id')
        user_url = 'http://www.facebook.com/' + str(id)
        cursor.execute('''
                        INSERT IGNORE INTO user (id, name, soc_media, keyword, url)
                        VALUES (%s, %s, %s, %s, %s)''', (id, name, soc_media, keyword, user_url))
        cnx.commit()
    cursor.close()
    cnx.close()

def twitter_mysql(users, keyword):
    '''
    Twitter insert data about users in mysql table user and location.
    :param users: the list of users from method 'twitter_search_user'
    :param keyword: search query keyword
    '''
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()

    soc_media = 2
    for u in users:
        id = u._json.get('id')
        name = u._json.get('name')
        screen_name = u._json.get('screen_name')
        user_url = 'http://www.twitter.com/'+str(screen_name)
        followers = u._json.get('followers_count')
        tweets = u._json.get('statuses_count')
        description = u._json.get('description')
        friends = u._json.get('friends_count')
        created_at = datetime.strptime(u._json.get('created_at'),'%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC)
        time_zone = u._json.get('time_zone')
        language = u._json.get('lang')

        if u._json.get('location'):
            cursor.execute('''SELECT MAX(id) FROM location''')
            id_location = cursor.fetchall()[0][0]
            if not id_location:
                id_location = str(0)

            location = u._json.get('location')
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
                        INSERT IGNORE INTO user (id, name, location, soc_media, screen_name, follower_count, post_count, description, friend_count, created_at, time_zone, language, keyword, url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', (id, name, id_location, soc_media, screen_name, followers, tweets, description, friends, created_at, time_zone, language, keyword, user_url))
        cnx.commit()
    cursor.close()
    cnx.close()

def googleplus_mysql(users, keyword):
    '''
    Google+ insert data about users in mysql table user.
    :param users: the list of users from method 'googleplus_search_user'
    :param keyword: search query keyword
    '''
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()

    soc_media = 3
    for u in users:
        name = str(u.get('displayName').encode('utf-8'))
        id = u.get('id')
        user_url = u.get('url')
        cursor.execute('''
                        INSERT IGNORE INTO user (id, name, soc_media, keyword, url)
                        VALUES (%s, %s, %s, %s, %s)''', (id, name, soc_media, keyword, user_url))
        cnx.commit()
    cursor.close()
    cnx.close()

