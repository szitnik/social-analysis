from __future__ import print_function
import mysql.connector
import methods
import pytz
from datetime import datetime
import config

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
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', (user_id, name, id_location, soc_media, screen_name, description, followers, tweets, friends, created_at, time_zone, language, keyword, user_url))
    cnx.commit()

def facebook_mysql(results, keyword):
    '''
    Facebook insert data in mysql table venue.
    :param results: the results from method 'methods.facebook_search_location'
    :param location: the latitude and longitude of the location center in format 'xx.xx,yyy.yy'
    :param distance: the radius around location center in kilometres, default is 100
    :param  keyword: search query keyword
    '''
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()
    soc_media = 1
    for place in results.get('data'):
        place_id = int(place.get('id'))
        place_url = 'http://www.facebook.com/' + str(place_id)
        place_name = place.get('name')
        place_city = place.get('location').get('city')
        if place_city is None:
            place_city = ''
        place_country = place.get('location').get('country')
        if place_country is None:
            place_country = ''
        place_address = place.get('location').get('street')
        place_longitude = place.get('location').get('longitude')
        place_latitude = place.get('location').get('latitude')
        place_coordinates = str(place_latitude)+', '+str(place_longitude)

        # location insert
        location = place_city + ', ' + place_country
        cursor.execute('''SELECT MAX(id) FROM location''')
        id_location = cursor.fetchall()[0][0]
        if not id_location:
            id_location = str(0)
        cursor.execute('''SELECT id FROM location WHERE name = %s''', (location,))
        location_already = cursor.fetchall()
        if location_already:
            id_location = location_already[0][0]
        else:
            id_location = str(int(id_location) + 1)
        cursor.execute('''
                                INSERT IGNORE INTO location (id, name, city, country, keyword)
                                VALUES (%s, %s, %s, %s, %s)''', (place_id, location, place_city, place_country, keyword))
        cnx.commit()

        cursor.execute('''
                        INSERT IGNORE INTO venue (id, name, location, address, coordinates, soc_media, keyword, url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)''', (place_id, place_name, id_location, place_address, place_coordinates, soc_media, keyword, place_url))
        cnx.commit()
    cursor.close()
    cnx.close()

def twitter_mysql(results, keyword):
    '''
    Twitter insert posts and comments on a particular location in mysql tables user, location, post and comment.
    :param results: the results from method 'methods.twitter_search_on_location'
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
            post_url = 'https://twitter.com/Interior/status/' + str(post_id)
            favourites = r._json.get('favourite_count')
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
            post_url = 'https://twitter.com/Interior/status/' + str(reply_status_id)
            reply_user_id = r._json.get('in_reply_to_user_id')
            comment_id = r._json.get('id')
            comment_url = 'https://twitter.com/Interior/status/' + str(comment_id)
            favourites = r._json.get('favourite_count')
            retweets = r._json.get('retweet_count')
            tags = ', '.join(i.get('text') for i in r._json.get('entities').get('hashtags'))
            created = datetime.strptime(r._json.get('created_at'),'%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC)

            # user insertion
            reply_user_info = methods.twitter_get_user_info(reply_user_id)
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

def youtube_mysql(results, keyword):
    '''
    Youtube insert videos and comments on a particular location in mysql tables user, post and comment.
    :param results: the results from method 'methods.youtube_search_video_location'
    :param  keyword: search query keyword
    '''
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()

    soc_media = 5
    for video in results:
        # user insertion
        user_id = video.get('snippet').get('channelId')
        user_name = video.get('snippet').get('channelTitle')
        user_url = 'https://www.youtube.com/channel/' + str(user_id)
        user_info = methods.youtube_get_channel_info(user_id)
        country = user_info['items'][0].get('snippet').get('country')
        created = datetime.strptime(user_info['items'][0].get('snippet').get('publishedAt'),'%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=pytz.UTC)
        description = user_info['items'][0].get('snippet').get('description')
        friends = user_info['items'][0].get('statistics').get('subscriberCount')
        video_count = user_info['items'][0].get('statistics').get('videoCount')
        cursor.execute('''
                        INSERT IGNORE INTO user (id, name, soc_media, description, post_count, friend_count, created_at, time_zone, keyword, url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', (user_id, user_name, soc_media, description, video_count, friends, created, country, keyword, user_url))
        cnx.commit()

        # post insertion
        title = video.get('snippet').get('title')
        published = datetime.strptime(video.get('snippet').get('publishedAt'),'%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=pytz.UTC)
        description = video.get('snippet').get('description')
        video_id = video.get('id').get('videoId')
        video_url = 'https://www.youtube.com/watch?v=' + str(video_id)
        youtube_type = 'video'
        video_info = methods.youtube_get_video_info(video_id)
        comments_count = video_info['items'][0].get('statistics').get('commentCount')
        dislikes = video_info['items'][0].get('statistics').get('dislikeCount')
        likes = video_info['items'][0].get('statistics').get('likeCount')
        views = video_info['items'][0].get('statistics').get('viewCount')
        coordinates = str(video_info['items'][0].get('recordingDetails').get('location').get('latitude'))+', '+str(video_info['items'][0].get('recordingDetails').get('location').get('longitude'))
        if video_info['items'][0].get('snippet').get('tags')is not None:
            tags = ', '.join(i for i in video_info['items'][0].get('snippet').get('tags'))
        else:
            tags = None
        cursor.execute('''
            INSERT IGNORE INTO post (id, text, creation_time, user, soc_media, description, like_count, dislike_count, comment_count, view_count, coordinates, tags, type, keyword, url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', (video_id, title, published, user_id, soc_media, description, likes, dislikes, comments_count, views, coordinates, tags, youtube_type, keyword, video_url))
        cnx.commit()

        # comment insertion
        if comments_count > 0:
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
    cursor.close()
    cnx.close()

# Foursquare insert venues on a particular location in mysql tables
def foursquare_mysql(results, keyword):
    '''
    Foursquare insert venues on a particular location in mysql tables location, venue, user and post.
    :param results: the results from method 'methods.foursquare_search_location'
    :param  keyword: search query keyword
    '''
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()

    soc_media = 6
    for r in results.get('venues'):
        if r.get('location').get('state'):
            location_city = r.get('location').get('state')
            location_country = r.get('location').get('country')
            location = location_city+', '+location_country
        elif r.get('location').get('city'):
            location_city = r.get('location').get('city')
            location_country = r.get('location').get('country')
            location = location_city+', '+location_country
        else:
            location_city = None
            location_country = r.get('location').get('country')
            location = location_country
        location_coordinates = str(r.get('location').get('lat'))+', '+str(r.get('location').get('lng'))

        # location insert
        cursor.execute('''SELECT MAX(id) FROM location''')
        id_location = cursor.fetchall()[0][0]
        if not id_location:
            id_location = str(0)
        cursor.execute('''SELECT id FROM location WHERE name = %s''', (location,))
        location_already = cursor.fetchall()
        if location_already:
            id_location = location_already[0][0]
        else:
            id_location = str(int(id_location)+1)
        cursor.execute('''
                        INSERT IGNORE INTO location (id, name, city, country, coordinates, keyword)
                        VALUES (%s, %s, %s, %s, %s, %s)''', (id_location, location, location_city, location_country, location_coordinates, keyword))
        cnx.commit()

        # venue insert
        venue_id = r.get('id')
        venue_name = r.get('name')
        venue_url = 'https://foursquare.com/v/'+venue_name+'/'+venue_id
        if r.get('location').get('labeledLatLngs'):
            venue_coordinates = str(r.get('location').get('labeledLatLngs')[0].get('lat'))+', '+str(r.get('location').get('labeledLatLngs')[0].get('lng'))
        elif r.get('location').get('lat'):
            venue_coordinates = str(r.get('location').get('lat')) + ', ' + str(r.get('location').get('lng'))
        else:
            venue_coordinates = ''
        venue_address = r.get('location').get('address')
        venue_tips = r.get('stats').get('tipCount')
        venue_checkins = r.get('stats').get('checkinsCount')
        venue_users = r.get('stats').get('usersCount')
        venue_categories = ', '.join(c.get('name') for c in r.get('categories'))
        cursor.execute('''
            INSERT IGNORE INTO venue (id, name, location, address, coordinates, categories, checkin_count, tip_count, user_count, soc_media, keyword, url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', (venue_id, venue_name, id_location, venue_address, venue_coordinates, venue_categories, venue_checkins, venue_tips, venue_users, soc_media, keyword, venue_url))
        cnx.commit()

        # tips insert
        tips = methods.foursquare_get_venue_tips(venue_id)
        for tip in tips:
            # user insert
            tip_user_id = tip.get('user').get('id')
            tip_user_name = str(tip.get('user').get('firstName').encode('utf-8')) + ' ' + str((tip.get('user').get('lastName') or '').encode('utf-8'))
            cursor.execute('''
                        INSERT IGNORE INTO user (id, name, soc_media, keyword)
                        VALUES (%s, %s, %s, %s)''', (tip_user_id, tip_user_name, soc_media, keyword))
            cnx.commit()
            tip_id = tip.get('id')
            tip_text = tip.get('text')
            tip_created = datetime.fromtimestamp(int(tip.get('createdAt'))).strftime('%Y-%m-%d %H:%M:%S')
            agrees = tip.get('agreeCount')
            disagress = tip.get('disagreeCount')
            type_fs = 'foursquare tip'
            cursor.execute('''
                        INSERT IGNORE INTO post (id, text, creation_time, user, soc_media, venue, like_count, dislike_count, type, keyword, url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', (tip_id, tip_text, tip_created, tip_user_id, soc_media, venue_id, agrees, disagress, type_fs, keyword, venue_url))
            cnx.commit()
    cursor.close()
    cnx.close()

# tripadvisor insert data in mysql tables
def tripadvisor_mysql(results, keyword):
    '''
    Tripadvisor insert data in mysql tables venue and location.
    :param results: the results from method 'tripadvisor_search_location'
    :param  keyword: search query keyword
    '''
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()

    soc_media = 8
    for hotel in results:
        hotel_id = hotel['hotel_id']
        hotel_name = hotel['hotel_name']
        hotel_url = hotel['url']
        locality = str(hotel['locality'][0])
        rating = hotel['tripadvisor_rating']
        features = ', '.join(hotel['hotel_features'])
        reviews = hotel['reviews']
        category = 'hotel'
        address = str(hotel['address'][0]) if hotel['address'] else ''
        country = str(hotel['country'][0])
        coordinates = hotel['coordinates']
        location = locality+country

        cursor.execute('''SELECT MAX(id) FROM sociopower.location''')
        id_location = cursor.fetchall()[0][0]
        if not id_location:
            id_location = str(0)

        cursor.execute('''SELECT id FROM location WHERE name = %s''', (location,))
        location_already = cursor.fetchall()
        if location_already:
            id_location = location_already[0][0]
        else:
            id_location = str(int(id_location) + 1)
        cursor.execute('''
                                INSERT IGNORE INTO location (id, name, city, country, keyword)
                                VALUES (%s, %s, %s, %s, %s)''', (id_location, location, locality, country, keyword))
        cnx.commit()
        cursor.execute('''
            INSERT IGNORE INTO sociopower.venue (id, name, location, address, coordinates, rating, categories, features, tip_count, soc_media, keyword, url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', (hotel_id, hotel_name, id_location, address, coordinates, rating, category, features, reviews, soc_media, keyword, hotel_url))
        cnx.commit()
    cursor.close()
    cnx.close()



