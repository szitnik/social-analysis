# -*- coding: utf-8 -*-
import methods
import pytz
from datetime import datetime
import re
from bs4 import BeautifulSoup
import tweepy
from datetime import timedelta
import text_processing
import config

lang_detect = 0
sentiment_detect = 0
users = {}
users_with_gender = {}

def language_and_sentiment(text, lang_sens, sent_sens):
    '''
    Language and sentiment detection of given text.
    :param text: text
    :param lang_sens: sensitivity for language detection
    :param sent_sens: sensitiviti for sentiment detection
    :return: language and sentiment for given text
    '''
    global lang_detect
    global sentiment_detect

    language = text_processing.language_detection(text, sens=lang_sens)
    lang_detect += 1 if language else 0
    sentiment = ''
    if language == 'sl' or language == 'en':
        text_sent = text_processing.prepare_text(text, language)
        if language == 'sl':
            prob_dist = text_processing.classifier_slovene.prob_classify(text_sent)
        if language == 'en':
            prob_dist = text_processing.classifier_english.prob_classify(text_sent)
        p = prob_dist.prob("pos")
        n = prob_dist.prob("neg")
        if float(n) >= float(sent_sens):
            sentiment = 'neg'
        if float(p) >= float(sent_sens):
            sentiment = 'pos'
        sentiment_detect += 1 if sentiment else 0
    return (language, sentiment)

def gender(text, lang, user, sens_name, sens_text, number_of_predictions, percent_of_same):
    '''
    Gender determination based on username and text.
    :param text: text
    :param lang: language of given text
    :param user: username
    :param sens_name: sensibility for gender determination based on username
    :param sens_text: sensibility for gender determination based on text
    :param number_of_predictions: how many predictions have to be made to determine gender based on username and text
    :param percent_of_same: how many predictions have to have the same gender to determine gender to particular user
    :return: /
    '''
    global users
    global users_with_gender
    classifier_name_1 = text_processing.classifier_gender_name_1
    classifier_name_2 = text_processing.classifier_gender_name_2
    classifier_text_slovene = text_processing.classifier_gender_text_slovene
    classifier_text_english = text_processing.classifier_gender_text_english

    user_id = user['id']
    gender_text = ''
    if lang == 'sl' or lang == 'en':
        textp = text_processing.prepare_text(text, lang)
        if lang == 'en':
            prob_dist = classifier_text_english.prob_classify(textp)
        if lang == 'sl':
            prob_dist = classifier_text_slovene.prob_classify(textp)
        f = prob_dist.prob("female")
        m = prob_dist.prob("male")
        if float(f) >= float(sens_text):
            gender_text = 'f'
        if float(m) >= float(sens_text):
            gender_text = 'm'

    if user_id in users.keys():
        # napoved spola glede na text
        if gender_text:
            users[user_id].append(gender_text)
    else:
        users[user_id] = []
        # napoved spol glede na username
        name1 = user['name']
        name2 = user['screen_name'] if 'screen_name' in user.keys() else ''
        if name1 and name2:
            prob_dist = classifier_name_2.prob_classify(text_processing.gender_name_features_2(name1, name2, classifier_name_1))
        else:
            if name1:
                prob_dist = classifier_name_1.prob_classify(text_processing.gender_name_features_1(name1))
            if name2:
                prob_dist = classifier_name_1.prob_classify(text_processing.gender_name_features_1(name2))

        f = prob_dist.prob("female")
        m = prob_dist.prob("male")
        gender = ''
        if float(f) >= float(sens_name):
            gender = 'f'
        if float(m) >= float(sens_name):
            gender = 'm'
        if gender:
            users[user_id].append(gender)
        # napoved spola glede na text
        if gender:
            users[user_id].append(gender_text)

    if len(users[user_id]) >= number_of_predictions:
        g = check_gender(users[user_id], percent_of_same)
        if g:
            users_with_gender[user_id] = (0, g)

def check_gender(genders, percent_of_same):
    '''
    Check if gender can be determined.
    :param genders: list of gender predictions, that have been determinet till now (based on username and tet for specific user)
    :param percent_of_same: how many predictions have to have the same gender to determine gender to particular user
    :return: gender, if it can be determined, else empty string is returned
    '''
    gender = ''
    all = len(genders)
    males = genders.count('m')
    females = genders.count('f')
    if males / float(all) >= percent_of_same:
        gender = 'male'
    if females / float(all) >= percent_of_same:
        gender = 'female'
    return gender

def gender_facebook(text, lang, sens_text):
    '''
    Gender determination based on username and text.
    :param text: text
    :param lang: language of given text
    :param user: username
    :param sens_name: sensibility for gender determination based on username
    :param sens_text: sensibility for gender determination based on text
    :return: /
    '''
    classifier_text_slovene = text_processing.classifier_gender_text_slovene
    classifier_text_english = text_processing.classifier_gender_text_english

    gender_text = ''
    if lang == 'sl' or lang == 'en':
        textp = text_processing.prepare_text(text, lang)
        if lang == 'en':
            prob_dist = classifier_text_english.prob_classify(textp)
        if lang == 'sl':
            prob_dist = classifier_text_slovene.prob_classify(textp)
        f = prob_dist.prob("female")
        m = prob_dist.prob("male")
        if float(f) >= float(sens_text):
            gender_text = 'f'
        if float(m) >= float(sens_text):
            gender_text = 'm'

    return gender_text

def twitter_search_from_to(query, date_from, date_to, lang_sens, sent_sens, sens_name, sens_text, number_of_predictions, percent_of_same, number_of_tweets=100):
    '''
    Twitter search posts between particular dates.
    :param query: search keyword
    :param date_from: the date to search from in format 'Y-M-D'
    :param date_to: the date to search to in format 'Y-M-D'
    :param lang_sens: sensibility for language detection
    :param sent_sens: sensibility for sentiment analysis
    :param sens_name: sensibility for gender determination based on username
    :param sens_text: sensibility for gender determination based on text
    :param number_of_predictions: how many predictions have to be made to determine gender based on username and text
    :param percent_of_same: how many predictions have to have the same gender to determine gender to particular user
    :param number_of_tweets: the number of maximal tweets per request, default is 100
    :return: the list of tweets
    '''
    auth = tweepy.OAuthHandler(config.twitter_consumer_key, config.twitter_consumer_secret)
    auth.set_access_token(config.twitter_access_key, config.twitter_access_secret)
    api = tweepy.API(auth)

    date_to = (datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    posts = {}
    results = []
    tweets = api.search(q=query, count=number_of_tweets, since=date_from, until=date_to)
    results += tweets
    p = twitter_mysql(results, query, lang_sens, sent_sens, sens_name, sens_text, number_of_predictions, percent_of_same)
    posts.update(p)
    if len(tweets) > 0:
        before_id = int(tweets[-1]._json.get('id') - 1)
        contin = True
        while contin:
            results = []
            tweets = [1]
            while tweets:
                try:
                    tweets = api.search(q=query, count=number_of_tweets, max_id=before_id, since=date_from)
                    if len(tweets) == 0:
                        contin = False
                except tweepy.RateLimitError:
                    tweets = []
                if len(tweets) > 0:
                    before_id = int(tweets[-1]._json.get('id') - 1)
                    results += tweets
            p = twitter_mysql(results, query, lang_sens, sent_sens, sens_name, sens_text, number_of_predictions, percent_of_same)
            posts.update(p)
    return posts

def twitter_user_insertion(r):
    '''
    Twitter store user data.
    :param r: the user from method 'twitter_mysql'
    '''
    screen_name = r.get('screen_name')
    user_url = 'http://www.twitter.com/' + str(screen_name)
    name = r.get('name')
    user_id = r.get('id')
    followers = r.get('followers_count')
    tweets = r.get('statuses_count')
    description = r.get('description')
    friends = r.get('friends_count')
    created = datetime.strptime(r.get('created_at'),'%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC)
    time_zone = r.get('time_zone')
    language = r.get('lang')
    location = r.get('location') if r.get('location') else ''

    return {'id': str(user_id), 'name': name, 'screen_name': screen_name, 'location': location, 'description': description,
            'nr_followers': int(followers),
            'nr_posts': int(tweets), 'nr_friends': int(friends), 'created': str(datetime.strftime(created, '%Y-%m-%d %H:%M:%S')), 'time_zone': time_zone,
            'language': language, 'ULR': user_url}

def twitter_mysql(results, keyword, lang_sens, sent_sens, sens_name, sens_text, number_of_predictions, percent_of_same):
    '''
    Twitter store posts data.
    :param results: the results from method 'methods.twitter_search'
    :param keyword: search query keyword
    :param lang_sens: sensibility for language detection
    :param sent_sens: sensibility for sentiment analysis
    :param sens_name: sensibility for gender determination based on username
    :param sens_text: sensibility for gender determination based on text
    :param number_of_predictions: how many predictions have to be made to determine gender based on username and text
    :param percent_of_same: how many predictions have to have the same gender to determine gender to particular user
    :return: the list of tweets
    '''

    global lang_detect
    global sentiment_detect
    global users_with_gender
    posts = {}
    for r in results:
        if r._json.get('in_reply_to_status_id') is None:
            # user insertion
            user = twitter_user_insertion(r._json.get('user'))
            user_id = user['id']

            # post insertion
            text = r._json.get('text')
            post_id = r._json.get('id')
            post_url = 'https://twitter.com/Interior/status/' + str(post_id)
            favourites = r._json.get('favorite_count') if r._json.get('favourite_count') else 0
            retweets = r._json.get('retweet_count') if r._json.get('retweet_count') else 0
            tags = ', '.join(i.get('text') for i in r._json.get('entities').get('hashtags'))
            created = datetime.strptime(r._json.get('created_at'),'%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC)
            if r._json.get('coordinates'):
                coordinates = ','.join(str(e) for e in r._json.get('coordinates').get('coordinates'))
            else:
                coordinates = ''
            if r._json.get('place'):
                place = r._json.get('place').get('country')
            else:
                place = ''

            language, sentiment = language_and_sentiment(text, lang_sens, sent_sens)
            if user_id not in users_with_gender.keys():
                gender(text, language, user, sens_name, sens_text, number_of_predictions, percent_of_same)
            if user_id in users_with_gender.keys():
                gend = users_with_gender[user_id][1]
                user['gender'] = {'gender': gend, 'probability-text': sens_text, 'probability-name': sens_name}
                if users_with_gender[user_id][0] == 0:
                    for post in posts:
                        if posts[post]['user']['id'] == user_id:
                            posts[post]['user']['gender'] = gend
                    users_with_gender[user_id] = (1, gend)

            lang_data = {'language': language, 'probability': lang_sens}
            sent_data = {'sentiment': sentiment, 'probability': str(sent_sens)+'/'+str(number_of_predictions)+'/'+str(percent_of_same)}

            post = {'type': 'twitter-post', 'id': str(post_id), 'created': str(datetime.strftime(created, '%Y-%m-%d %H:%M:%S')), 'tags': tags, 'text': text, 'nr_favourites': int(favourites),
                    'nr_retweets': int(retweets), 'language': lang_data, 'sentiment': sent_data, 'coordinates': coordinates, 'place': place, 'URL': post_url, 'user': user, 'keyword': keyword}
        else:
            text = r._json.get('text')
            reply_status_id = r._json.get('in_reply_to_status_id')
            reply_user_id = r._json.get('in_reply_to_user_id')
            comment_id = r._json.get('id')
            comment_url = 'https://twitter.com/Interior/status/' + str(comment_id)
            favourites = r._json.get('favourite_count') if r._json.get('favourite_count') else 0
            retweets = r._json.get('retweet_count') if r._json.get('retweet_count') else 0
            tags = ', '.join(i.get('text') for i in r._json.get('entities').get('hashtags'))
            created = datetime.strptime(r._json.get('created_at'),'%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC)

            # user insertion
            reply_user_info = methods.twitter_get_user_info(reply_user_id)
            if not reply_user_info:
                print('krneki')
            else:
                user = twitter_user_insertion(reply_user_info._json)

            user_id = user['id']
            language, sentiment = language_and_sentiment(text, lang_sens, sent_sens)
            if user_id not in users_with_gender.keys():
                gender(text, language, user, sens_name, sens_text, number_of_predictions, percent_of_same)
            if user_id in users_with_gender.keys():
                gend = users_with_gender[user_id][1]
                user['gender'] = {'gender': gend, 'probability-text': sens_text, 'probability-name': sens_name}
                if users_with_gender[user_id][0] == 0:
                    for post in posts:
                        if posts[post]['user']['id'] == user_id:
                            posts[post]['user']['gender'] = gend
                    users_with_gender[user_id] = (1, gend)

            lang_data = {'language': language, 'probability': lang_sens}
            sent_data = {'sentiment': sentiment, 'probability': str(sent_sens)+'/'+str(number_of_predictions)+'/'+str(percent_of_same)}

            post = {'type': 'twitter-post', 'id': str(comment_id), 'created': str(datetime.strftime(created, '%Y-%m-%d %H:%M:%S')), 'tags': tags, 'text': text,
                    'nr_favourites': int(favourites), 'nr_retweets': int(retweets), 'language': lang_data, 'sentiment': sent_data, 'URL': comment_url, 'user': user, 'keyword': keyword}
            post_id = comment_id
        posts['twitter-post-' + str(post_id)] = post
    return posts

def tumblr_mysql(results, keyword, date_from, date_to, lang_sens, sent_sens, sens_name, sens_text, number_of_predictions, percent_of_same):
    '''
    Tumblr store search results.
    :param results: the results from method 'methods.tumblr_search'
    :param query: search keyword
    :param date_from: the date to search from in format 'Y-M-D'
    :param date_to: the date to search to in format 'Y-M-D'
    :param lang_sens: sensibility for language detection
    :param sent_sens: sensibility for sentiment analysis
    :param sens_name: sensibility for gender determination based on username
    :param sens_text: sensibility for gender determination based on text
    :param number_of_predictions: how many predictions have to be made to determine gender based on username and text
    :param percent_of_same: how many predictions have to have the same gender to determine gender to particular user
    :return: the list of posts
    '''

    global lang_detect
    global sentiment_detect

    posts = {}
    for r in results:
        created_date = datetime.strptime(r.get('date'), "%Y-%m-%d %H:%M:%S GMT")
        from_date = datetime.strptime(date_from, "%Y-%m-%d")
        if created_date >= from_date:
            # user
            name = r.get('blog_name')
            user_url = str(name)+'.tumblr.com'
            info = methods.tumblr_get_blog_info(name)
            description = info.get('blog').get('description') if info.get('blog') else ""
            nr_posts = info.get('blog').get('posts') if info.get('blog').get('posts') else ""
            screen_name = info.get('blog').get('title') if info.get('blog').get('title') else ""
            user = {'id': name, 'name': name, 'screen_name': screen_name, 'description': description, 'nr_posts': int(nr_posts), 'URL': user_url}

            # post
            id = r.get('id')
            created = r.get('date')
            tags = ', '.join(str(e.encode('utf-8')) for e in r.get('tags'))
            post_url = r.get('short_url')
            if r.get('trail'):
                text = BeautifulSoup(r.get('trail')[0].get('content')).text
            else:
                text = ""

            language, sentiment = language_and_sentiment(text, lang_sens, sent_sens)
            if name not in users_with_gender.keys():
                gender(text, language, user, sens_name, sens_text, number_of_predictions, percent_of_same)
            if name in users_with_gender.keys():
                gend = users_with_gender[name][1]
                user['gender'] = {'gender': gend, 'probability-text': sens_text, 'probability-name': sens_name}
                if users_with_gender[name][0] == 0:
                    for post in posts:
                        if posts[post]['user']['id'] == name:
                            posts[post]['user']['gender'] = gend
                    users_with_gender[name] = (1, gend)

            lang_data = {'language': language, 'probability': lang_sens}
            sent_data = {'sentiment': sentiment, 'probability': str(sent_sens)+'/'+str(number_of_predictions)+'/'+str(percent_of_same)}

            post = {'type': 'tumblr-post', 'id': str(id), 'created': str(datetime.strptime(created[:19], '%Y-%m-%d %H:%M:%S')),
                    'language': lang_data, 'sentiment': sent_data, 'tags': tags, 'text': text, 'URL': post_url, 'user': user, 'keyword': keyword}
            posts['tumblr-post-'+str(id)] = post
    return posts

def googleplus_mysql(results, keyword, date_from, date_to, lang_sens, sent_sens, sens_name, sens_text, number_of_predictions, percent_of_same):
    '''
    Google+ store search results.
    :param results: the results from method 'methods.tumblr_search'
    :param query: search keyword
    :param date_from: the date to search from in format 'Y-M-D'
    :param date_to: the date to search to in format 'Y-M-D'
    :param lang_sens: sensibility for language detection
    :param sent_sens: sensibility for sentiment analysis
    :param sens_name: sensibility for gender determination based on username
    :param sens_text: sensibility for gender determination based on text
    :param number_of_predictions: how many predictions have to be made to determine gender based on username and text
    :param percent_of_same: how many predictions have to have the same gender to determine gender to particular user
    :return: the list of posts
    '''
    global lang_detect
    global sentiment_detect

    posts = {}
    for r in results:
        created_date = datetime.strptime(r.get('published'),'%Y-%m-%dT%H:%M:%S.%fZ')
        from_date = datetime.strptime(date_from, "%Y-%m-%d")
        if created_date >= from_date:
            # user insertion
            user_id = r.get('actor').get('id')
            name = r.get('actor').get('displayName')
            user_url = r.get('actor').get('url')
            user = {'id': user_id, 'name': name, 'URL': user_url}

            # post insertion
            post_id = r.get('id')
            title = r.get('title')
            created = datetime.strptime(r.get('published')[:19],'%Y-%m-%dT%H:%M:%S')
            text = r.get('object').get('content')
            likes = r.get('object').get('plusoners').get('totalItems')
            post_url = r.get('object').get('url')
            comments_count = r.get('object').get('replies').get('totalItems')
            post_tags = ', '.join(i for i in re.findall(r">#(\w+)", r.get('object').get('content')))

            language, sentiment = language_and_sentiment(text, lang_sens, sent_sens)
            if user_id not in users_with_gender.keys():
                gender(text, language, user, sens_name, sens_text, number_of_predictions, percent_of_same)
            if user_id in users_with_gender.keys():
                gend = users_with_gender[user_id][1]
                user['gender'] = {'gender': gend, 'probability-text': sens_text, 'probability-name': sens_name}
                if users_with_gender[user_id][0] == 0:
                    for post in posts:
                        if posts[post]['user']['id'] == user_id:
                            posts[post]['user']['gender'] = gend
                    users_with_gender[user_id] = (1, gend)

            lang_data = {'language': language, 'probability': lang_sens}
            sent_data = {'sentiment': sentiment, 'probability': str(sent_sens)+'/'+str(number_of_predictions)+'/'+str(percent_of_same)}

            post = {'type': 'googleplus-post', 'id': str(post_id), 'created': str(created), 'tags': post_tags, 'nr_likes': int(likes),
                    'nr_comments': int(comments_count), 'language': lang_data, 'sentiment': sent_data, 'title': title, 'text': text, 'URL': post_url, 'user': user, 'keyword': keyword}
            posts['googleplus-post-'+str(post_id)] = post
    return posts

def youtube_mysql(results, keyword, date_from, date_to, lang_sens, sent_sens, sens_name, sens_text, number_of_predictions, percent_of_same):
    '''
    Youtube store search results.
    :param results: the results from method 'methods.tumblr_search'
    :param query: search keyword
    :param date_from: the date to search from in format 'Y-M-D'
    :param date_to: the date to search to in format 'Y-M-D'
    :param lang_sens: sensibility for language detection
    :param sent_sens: sensibility for sentiment analysis
    :param sens_name: sensibility for gender determination based on username
    :param sens_text: sensibility for gender determination based on text
    :param number_of_predictions: how many predictions have to be made to determine gender based on username and text
    :param percent_of_same: how many predictions have to have the same gender to determine gender to particular user
    :return: the list of posts
    '''
    (videos, channels, playlists) = results
    global lang_detect
    global sentiment_detect

    posts = {}
    # VIDEO
    for video in videos:
        created_date = datetime.strptime(video.get('snippet').get('publishedAt'),'%Y-%m-%dT%H:%M:%S.%fZ')
        from_date = datetime.strptime(date_from, "%Y-%m-%d")
        if created_date >= from_date:
            # user insertion
            user_id = video.get('snippet').get('channelId')
            user_name = video.get('snippet').get('channelTitle')
            user_info = methods.youtube_get_channel_info(user_id)
            user_url = 'https://www.youtube.com/channel/'+str(user_id)
            if user_info['items']:
                country = user_info['items'][0].get('snippet').get('country')
                created = datetime.strptime(user_info['items'][0].get('snippet').get('publishedAt')[:19],'%Y-%m-%dT%H:%M:%S')
                description = user_info['items'][0].get('snippet').get('description')
                friends = user_info['items'][0].get('statistics').get('subscriberCount')
                video_count = user_info['items'][0].get('statistics').get('videoCount')

            user = {'id': str(user_id), 'name': user_name, 'description': description, 'nr_videos': int(video_count), 'country': country,
                    'created': str(created), 'nr_friends':int(friends), 'URL': user_url}

            # post insertion
            title = video.get('snippet').get('title')
            published = datetime.strptime(video.get('snippet').get('publishedAt')[:19],'%Y-%m-%dT%H:%M:%S')
            description = video.get('snippet').get('description')
            video_id = video.get('id').get('videoId')
            video_url = 'https://www.youtube.com/watch?v=' + str(video_id)
            video_info = methods.youtube_get_video_info(video_id)
            comments_count = 0
            dislikes = 0
            likes = 0
            views = 0
            tags = ''
            if len(video_info['items']) > 0:
                comments_count = 0 if video_info['items'][0].get('statistics').get('commentCount') == None else int(video_info['items'][0].get('statistics').get('commentCount'))
                dislikes = 0 if video_info['items'][0].get('statistics').get('dislikeCount') == None else video_info['items'][0].get('statistics').get('dislikeCount')
                likes = 0 if video_info['items'][0].get('statistics').get('likeCount') == None else video_info['items'][0].get('statistics').get('likeCount')
                views = 0 if video_info['items'][0].get('statistics').get('viewCount') == None else video_info['items'][0].get('statistics').get('viewCount')
                if video_info['items'][0].get('snippet').get('tags')is not None:
                    tags = ', '.join(i for i in video_info['items'][0].get('snippet').get('tags'))
                else:
                    tags = ''

            language, sentiment = language_and_sentiment(description, lang_sens, sent_sens)
            if user_id not in users_with_gender.keys():
                gender(description, language, user, sens_name, sens_text, number_of_predictions, percent_of_same)
            if user_id in users_with_gender.keys():
                gend = users_with_gender[user_id][1]
                user['gender'] = {'gender': gend, 'probability-text': sens_text, 'probability-name': sens_name}
                if users_with_gender[user_id][0] == 0:
                    for post in posts:
                        if posts[post]['user']['id'] == user_id:
                            posts[post]['user']['gender'] = gend
                    users_with_gender[user_id] = (1, gend)

            lang_data = {'language': language, 'probability': lang_sens}
            sent_data = {'sentiment': sentiment, 'probability': str(sent_sens)+'/'+str(number_of_predictions)+'/'+str(percent_of_same)}

            post = {'type': 'youtube-video', 'id': str(video_id), 'created': str(published), 'description': description,
                    'nr_likes': int(likes), 'nr_dislikes': int(dislikes), 'nr_comments': int(comments_count), 'language': lang_data, 'sentiment': sent_data,
                    'tags': tags, 'nr_views':int(views), 'title': title, 'URL': video_url, 'user': user, 'keyword': keyword}
            posts['youtube-video-'+str(video_id)] = post
    return posts

def facebook_mysql(results, keyword, date_from, date_to, lang_sens, sent_sens, sens_name, sens_text, number_of_predictions, percent_of_same):
    '''
    Facebook store search results.
    :param results: the results from method 'methods.tumblr_search'
    :param query: search keyword
    :param date_from: the date to search from in format 'Y-M-D'
    :param date_to: the date to search to in format 'Y-M-D'
    :param lang_sens: sensibility for language detection
    :param sent_sens: sensibility for sentiment analysis
    :param sens_name: sensibility for gender determination based on username
    :param sens_text: sensibility for gender determination based on text
    :param number_of_predictions: how many predictions have to be made to determine gender based on username and text
    :param percent_of_same: how many predictions have to have the same gender to determine gender to particular user
    :return: the list of posts
    '''
    (events, groups, pages) = results
    all_posts = {}
    global lang_detect
    global sentiment_detect

    for event in events.get('data'):
        event_id = int(event.get('id'))
        event_url = 'http://www.facebook.com/'+str(event_id)
        event_name = event.get('name')
        event_description = event.get('description')
        event_start_time = event.get('start_time')
        event_end_time = event.get('end_time')
        event_coordinates = None
        event_city = None
        event_country = None
        if event.get('place') != None:
            event_place = event.get('place').get('name')
            if event.get('place').get('location') != None:
                if event.get('place').get('location').get('city') != None:
                    event_city = event.get('place').get('location').get('city')
                if event.get('place').get('location').get('country') != None:
                    event_country = event.get('place').get('location').get('country')
                if event.get('place').get('location').get('latitude') != None:
                    event_coordinates = str(event.get('place').get('location').get('latitude'))+','+str(event.get('place').get('location').get('longitude'))
            else:
                event_coordinates = ''
        else:
            event_place = ''
            event_coordinates = ''

        language, sentiment = language_and_sentiment(event_description, lang_sens, sent_sens)
        lang_data = {'language': language, 'probability': lang_sens}
        sent_data = {'sentiment': sentiment, 'probability': str(sent_sens) + '/' + str(number_of_predictions) + '/' + str(percent_of_same)}
        gender_text = gender_facebook(event_description, lang_data['language'], sens_text)

        event_data = {'id': str(event_id), 'name': event_name, 'description': event_description, 'start_time': event_start_time, 'end_time': event_end_time,
                      'place': {'place': event_place, 'coordinates': event_coordinates, 'city': event_city, 'country': event_country}, 'URL': event_url,
                      'language': lang_data, 'sentiment': sent_data, 'gender': gender_text}

        posts = methods.facebook_get_egp_posts_from_to(event_id, date_from, date_to)
        for post in posts:
            # post insertion
            post_id = post.get('id')
            post_url = 'http://www.facebook.com/' + str(post_id)
            post_text = post.get('message')
            if post.get('created_time'):
                post_published = datetime.strptime(post.get('created_time')[:19], '%Y-%m-%dT%H:%M:%S')
            else:
                post_published = datetime.strptime(post.get('updated_time')[:19], '%Y-%m-%dT%H:%M:%S')

            language, sentiment = language_and_sentiment(post_text, lang_sens, sent_sens)
            lang_data = {'language': language, 'probability': lang_sens}
            sent_data = {'sentiment': sentiment, 'probability': str(sent_sens)+'/'+str(number_of_predictions)+'/'+str(percent_of_same)}
            gender_text = gender_facebook(post_text, lang_data['language'], sens_text)

            post = {'type': 'facebook-event-post', 'id': str(post_id), 'created': str(post_published), 'text': post_text, 'language': lang_data, 'sentiment': sent_data,
                    'gender': gender_text, 'URL': post_url, 'keyword': keyword, 'event_data': event_data}
            all_posts['facebook-event-post-'+str(post_id)] = post

    for group in groups.get('data'):
        group_id = int(group.get('id'))
        group_url = 'http://www.facebook.com/' + str(group_id)
        group_name = group.get('name')
        group_data = methods.facebook_get_group_info(group_id)
        group_about = group_data.get('about')
        group_privacy = group.get('privacy')

        group_data = {'id': group_id, 'name': group_name, 'description': group_about, 'privacy': group_privacy, 'URL': group_url}

        posts = methods.facebook_get_egp_posts_from_to(group_id, date_from, date_to)
        for post in posts:
            # post insertion
            post_id = post.get('id')
            post_url = 'http://www.facebook.com/' + str(post_id)
            post_text = post.get('message')
            if post.get('created_time'):
                post_published = datetime.strptime(post.get('created_time')[:19], '%Y-%m-%dT%H:%M:%S')
            else:
                post_published = datetime.strptime(post.get('updated_time')[:19], '%Y-%m-%dT%H:%M:%S')

            language, sentiment = language_and_sentiment(post_text, lang_sens, sent_sens)
            lang_data = {'language': language, 'probability': lang_sens}
            sent_data = {'sentiment': sentiment, 'probability': str(sent_sens)+'/'+str(number_of_predictions)+'/'+str(percent_of_same)}
            gender_text = gender_facebook(post_text, lang_data['language'], sens_text)

            post = {'type': 'facebook-group-post', 'id': str(post_id), 'created': str(post_published), 'text': post_text, 'language': lang_data, 'sentiment': sent_data,
                    'gender': gender_text, 'URL': post_url, 'keyword': keyword, 'group_data': group_data}
            all_posts['facebook-group-post-'+str(post_id)] = post

    for page in pages.get('data'):
        page_id = int(page.get('id'))
        page_url = 'http://www.facebook.com/' + str(page_id)
        page_name = page.get('name')
        page_fans = methods.facebook_get_page_fans(page_id)
        page_data = methods.facebook_get_page_info(page_id)
        page_description = page_data.get('description')
        page_privacy = page.get('privacy')

        page_data = {'id': str(page_id), 'name': page_name, 'description': page_description, 'nr_fans': int(page_fans), 'privacy': page_privacy, 'URL': page_url}

        # post and comment insertion
        posts = methods.facebook_get_egp_posts_from_to(page_id, date_from, date_to)
        for post in posts:
            # post insertion
            post_id = post.get('id')
            post_url = 'http://www.facebook.com/' + str(post_id)
            post_text = post.get('message')
            if post.get('created_time'):
                post_published = datetime.strptime(post.get('created_time')[:19], '%Y-%m-%dT%H:%M:%S')
            else:
                post_published = datetime.strptime(post.get('updated_time')[:19], '%Y-%m-%dT%H:%M:%S')

            language, sentiment = language_and_sentiment(post_text, lang_sens, sent_sens)
            lang_data = {'language': language, 'probability': lang_sens}
            sent_data = {'sentiment': sentiment, 'probability': str(sent_sens)+'/'+str(number_of_predictions)+'/'+str(percent_of_same)}
            gender_text = gender_facebook(post_text, lang_data['language'], sens_text)

            post = {'type': 'facebook-page-post', 'id': str(post_id), 'created': str(post_published),
                    'text': post_text, 'language': lang_data, 'sentiment': sent_data, 'gender': gender_text, 'URL': post_url, 'keyword': keyword, 'page_data': page_data}
            all_posts['facebook-page-post-'+str(post_id)] = post
    return all_posts

