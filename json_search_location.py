import methods
import pytz
from datetime import datetime
from datetime import timedelta
import tweepy
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
                prob_dist = classifier_name_1.prob_classify( text_processing.gender_name_features_1(name1))
            if name2:
                prob_dist = classifier_name_1.prob_classify( text_processing.gender_name_features_1(name2))

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

def twt_location_after_date(query, date_from, date_to, lang_sens, sent_sens, sens_name, sens_text, number_of_predictions, percent_of_same, location, radius=100, number_of_tweets=100):
    '''
    Twitter search posts on location between particular dates.
    :param query: search keyword
    :param date_from: the date to search from in format 'Y-M-D'
    :param date_to: the date to search to in format 'Y-M-D'
    :param lang_sens: sensibility for language detection
    :param sent_sens: sensibility for sentiment analysis
    :param sens_name: sensibility for gender determination based on username
    :param sens_text: sensibility for gender determination based on text
    :param number_of_predictions: how many predictions have to be made to determine gender based on username and text
    :param percent_of_same: how many predictions have to have the same gender to determine gender to particular user
    :param location: the latitude and longitude of the location center in format 'xx.xx,yyy.yy'
    :param radius: the radius around location center in kilometres, default is 100 km
    :param number_of_tweets: the number of maximal tweets per request, default is 100
    :return: the list of tweets
    '''

    auth = tweepy.OAuthHandler(config.twitter_consumer_key, config.twitter_consumer_secret)
    auth.set_access_token(config.twitter_access_key, config.twitter_access_secret)
    api = tweepy.API(auth)

    date_to = (datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    posts = {}
    results = []
    loc = location+','+str(radius)+'km'
    tweets = api.search(q=query, geocode=loc, count=number_of_tweets, since=date_from, until=date_to)
    results += tweets
    p = twitter_mysql(results, query, lang_sens, sent_sens, sens_name, sens_text, number_of_predictions, percent_of_same)
    posts.update(p)
    if len(tweets) > 0:
        before_id = int(tweets[-1]._json.get('id')-1)
        contin = True
        while contin:
            results = []
            tweets = [1]
            while tweets:
                try:
                    tweets = api.search(q=query, geocode=loc, count=number_of_tweets, max_id=int(before_id), since=date_from)
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
    created = datetime.strptime(r.get('created_at'), '%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC)
    time_zone = r.get('time_zone')
    language = r.get('lang')
    location = r.get('location') if r.get('location') else ''

    return {'id': str(user_id), 'name': name, 'screen_name': screen_name, 'location': location, 'description': description,
            'nr_followers': int(followers),
            'nr_posts': int(tweets), 'nr_friends': int(friends), 'created': str(datetime.strftime(created, '%Y-%m-%d %H:%M:%S')), 'time_zone': time_zone,
            'language': language, 'ULR': user_url}

def twitter_mysql(results, keyword, lang_sens, sent_sens, sens_name, sens_text, number_of_predictions, percent_of_same):
    '''
    Twitter store search results.
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

    posts = {}
    for r in results:
        if r._json.get('in_reply_to_status_id') is None:
            user = twitter_user_insertion(r._json.get('user'))
            user_id = user['id']
            # post insertion
            text = r._json.get('text')
            post_id = r._json.get('id')
            post_url = 'https://twitter.com/Interior/status/' + str(post_id)
            favourites = r._json.get('favourite_count') if r._json.get('favourite_count') else 0
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
            post_url = 'https://twitter.com/Interior/status/' + str(reply_status_id)
            reply_user_id = r._json.get('in_reply_to_user_id')
            comment_id = r._json.get('id')
            comment_url = 'https://twitter.com/Interior/status/' + str(comment_id)
            favourites = r._json.get('favourite_count') if r._json.get('favourite_count') else 0
            retweets = r._json.get('retweet_count') if r._json.get('retweet_count') else 0
            tags = ', '.join(i.get('text') for i in r._json.get('entities').get('hashtags'))
            created = datetime.strptime(r._json.get('created_at'),'%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC)

            # user insertion
            reply_user_info = methods.twitter_get_user_info(reply_user_id)
            user = twitter_user_insertion(reply_user_info._json)
            user_id = user['id']

            language, sentiment = language_and_sentiment(text, lang_sens, sent_sens)
            if user_id not in users_with_gender.keys():
                gender(text, language, user, sens_name, sens_text, number_of_predictions, percent_of_same)
            if user_id in users_with_gender.keys():
                user['gender'] = {'gender': gend, 'probability-text': sens_text, 'probability-name': sens_name}
                for post in posts:
                    if posts[post]['user']['id'] == user_id:
                        posts[post]['user']['gender'] = users_with_gender[user_id]

            lang_data = {'language': language, 'probability': lang_sens}
            sent_data = {'sentiment': sentiment, 'probability': str(sent_sens)+'/'+str(number_of_predictions)+'/'+str(percent_of_same)}

            post = {'type': 'twitter-post', 'id': str(comment_id), 'created': str(datetime.strftime(created, '%Y-%m-%d %H:%M:%S')), 'tags': tags, 'text': text,
                    'nr_favourites': int(favourites), 'nr_retweets': int(retweets), 'language': lang_data, 'sentiment': sent_data, 'URL': comment_url, 'user': user, 'keyword': keyword}
            post_id = comment_id
        posts['twitter-post-' + str(post_id)] = post
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
    posts = {}
    for video in results:
        created_date = datetime.strptime(video.get('snippet').get('publishedAt'),'%Y-%m-%dT%H:%M:%S.%fZ')
        from_date = datetime.strptime(date_from, "%Y-%m-%d")
        if created_date >= from_date:
            # user insertion
            user_id = video.get('snippet').get('channelId')
            user_name = video.get('snippet').get('channelTitle')
            user_url = 'https://www.youtube.com/channel/' + str(user_id)
            user_info = methods.youtube_get_channel_info(user_id)
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
            comments_count = video_info['items'][0].get('statistics').get('commentCount')
            dislikes = video_info['items'][0].get('statistics').get('dislikeCount')
            likes = video_info['items'][0].get('statistics').get('likeCount')
            views = video_info['items'][0].get('statistics').get('viewCount')
            coordinates = str(video_info['items'][0].get('recordingDetails').get('location').get('latitude'))+', '+str(video_info['items'][0].get('recordingDetails').get('location').get('longitude'))
            if video_info['items'][0].get('snippet').get('tags')is not None:
                tags = ', '.join(i for i in video_info['items'][0].get('snippet').get('tags'))
            else:
                tags = None

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
                    'nr_likes': int(likes), 'nr_dislikes': int(dislikes), 'nr_comments': int(comments_count), 'tags': tags,
                    'nr_views': int(views),  'language': lang_data, 'sentiment': sent_data, 'title': title, 'coordinates': coordinates, 'URL': video_url, 'user': user, 'keyword': keyword}
            posts['youtube-video-'+str(video_id)] = post
    return posts


