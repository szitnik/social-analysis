import facebook
import json
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()
import oauth2
import urlparse
import pytumblr
import tweepy
import httplib2
import apiclient.discovery
import urllib2
import foursquare
from datetime import datetime
from datetime import timedelta
import time
import pytz
from facebook import GraphAPIError
import config
from lxml import html
import requests, re

# FACEBOOK
def facebook_search(query, max_results=5000):
    '''
    Facebook basic search events, pages, groups.
    :param query: search keyword
    :param max_results: maximal number of results, default is 5000
    :return: triple (events, pages, groups), where events, pages and groups are lists of results: events, pages and groups, respectively
    '''
    fb_api = facebook.GraphAPI(access_token=config.facebook_token, version=2.7)
    max_results = str(max_results)

    # event search
    events = fb_api.request('/search?q='+query+'&type=event&limit='+max_results+'')
    # page search
    pages = fb_api.request('/search?q='+query+'&type=page&limit='+max_results+'')
    # group search
    groups = fb_api.request('/search?q='+query+'&type=group&limit='+max_results+'')

    return (events, pages, groups)

def facebook_get_page_info(page_id):
    '''
    Facebook get page info: id, name, about.
    :param page_id: the identification number of page
    :return: information about desired page
    '''
    fb_api = facebook.GraphAPI(access_token=config.facebook_token, version=2.7)

    page_id = str(page_id)
    try:
        page_info = fb_api.request('/' + page_id + '?fields=id,name,about')
    except GraphAPIError:
        print('Tried accessing nonexisting field (about) on node type.')
        page_info = fb_api.request('/' + page_id + '')
    return page_info

def facebook_get_group_info(group_id):
    '''
    Facebook get group info: id, name, description.
    :param group_id: the identification number of group
    :return: information about desired group
    '''
    fb_api = facebook.GraphAPI(access_token=config.facebook_token, version=2.7)

    group_id = str(group_id)
    try:
        group_info = fb_api.request('/' + group_id + '?fields=id,name,description')
    except GraphAPIError:
        print('Tried accessing nonexisting field (about) on node type.')
        group_info = fb_api.request('/' + group_id + '')
    return group_info

def facebook_get_page_fans(page_id):
    '''
    Facebook get page number of fans.
    :param page_id: the identification number of page
    :return: number of fans
    '''
    fb_api = facebook.GraphAPI(access_token=config.facebook_token, version=2.7)

    page_id = str(page_id)
    page_fans = fb_api.request('/' + page_id + '/insights/page_fans_country?period=lifetime')
    nr_fans = 0
    if len(page_fans['data']) > 0 and'value' in page_fans['data'][0]['values'][0].keys():
        r = page_fans['data'][0]['values'][1]['value']
        nr_fans = sum(r.values())
    return nr_fans


def facebook_get_egp_posts_since(egp_id, since_date='2000-01-01', max_results=100):
    '''
    Facebook get event/group/page posts since particular date.
    :param egp_id: the identification number of event/group/page
    :param since_date: the date to search from in format 'Y-M-D', default is '2000-01-01'
    :param max_results: the number of maximal results per request, default is 100
    :return: the list of posts
    '''
    fb_api = facebook.GraphAPI(access_token=config.facebook_token, version=2.7)

    egp_id = str(egp_id)
    max_results = str(max_results)
    since_date = int(time.mktime(datetime.strptime(since_date, "%Y-%m-%d").timetuple()))
    egp_post = fb_api.request('/' + egp_id + '/feed?since=' + str(since_date) + '&limit=' + max_results + '')
    results = egp_post.get('data')

    if len(results) > int(max_results)-1:
        if results[-1].get('created_time'):
            d = datetime.strptime(results[-1].get('created_time')[:-5], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=pytz.UTC)
        if results[-1].get('updated_time'):
            d = datetime.strptime(results[-1].get('updated_time')[:-5], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=pytz.UTC)
        last_post_date = int(time.mktime(d.timetuple()))
        while int(since_date) < int(last_post_date):
            try:
                egp_post = fb_api.request('/' + egp_id + '/feed?until=' + str(last_post_date) + '&limit=' + max_results + '')
                results+=egp_post.get('data')
                if results[-1].get('created_time'):
                    d = datetime.strptime(results[-1].get('created_time')[:-5], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=pytz.UTC)
                if results[-1].get('updated_time'):
                    d = datetime.strptime(results[-1].get('updated_time')[:-5], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=pytz.UTC)
                last_post_date = int(time.mktime(d.timetuple()))
            except GraphAPIError:
                print('GraphAPIError: An unknown error has occurred.')
                break
    return results

def facebook_get_egp_posts_until(egp_id, until_date=datetime.now().strftime("%Y-%m-%dT%H:%M:%S"), max_results=100):
    '''
    Facebook get last 100 posts of event/group/page.
    :param egp_id: the identification number of event/group/page
    :param until_date: the date to search to in format 'Y-M-D', default is '2000-01-01'
    :param max_results: the number of maximal results per request, default is 100
    :return: the list of posts
    '''
    fb_api = facebook.GraphAPI(access_token=config.facebook_token, version=2.7)

    egp_id = str(egp_id)
    max_results = str(max_results)
    egp_post = fb_api.request('/'+egp_id+'/feed?until='+until_date+'&limit='+max_results+'')
    results = egp_post.get('data')
    return results

def facebook_get_egp_posts_from_to(egp_id, date_from, date_to, max_results=100):
    '''
    Facebook get event/group/page posts between dates.
    :param egp_id: the identification number of event/group/page
    :param date_from: the date to search from in format 'Y-M-D'
    :param date_to: the date to search to in format 'Y-M-D'
    :param max_results: the number of maximal results per request, default is 100
    :return: the list of posts
    '''
    fb_api = facebook.GraphAPI(access_token=config.facebook_token, version=2.7)

    date_from = date_from + 'T00:00:00'
    date_to = date_to + 'T23:59:59'
    egp_id = str(egp_id)
    max_results = str(max_results)
    since_date = (datetime.strptime(date_from, "%Y-%m-%dT%H:%M:%S"))
    until_date = (datetime.strptime(date_to, "%Y-%m-%dT%H:%M:%S"))
    egp_post = fb_api.request('/' + egp_id + '/feed?since=' + str(since_date) +'&until='+str(until_date) +'&limit=' + max_results + '')
    results = egp_post.get('data')
    egp_post_last = []
    if len(results) > int(max_results)-1:
        if results[-1].get('created_time'):
            d = datetime.strptime(results[-1].get('created_time')[:-5], '%Y-%m-%dT%H:%M:%S')
        if results[-1].get('updated_time'):
            d = datetime.strptime(results[-1].get('updated_time')[:-5], '%Y-%m-%dT%H:%M:%S')
        last_post_date = d
        while since_date <= last_post_date and egp_post_last != egp_post:
            try:
                egp_post_last = egp_post
                egp_post = fb_api.request('/' + egp_id + '/feed?since=' + str(since_date) +'&until='+str(last_post_date) +'&limit=' + max_results + '')
                results+=egp_post.get('data')
                if results[-1].get('created_time'):
                    d = datetime.strptime(results[-1].get('created_time')[:-5], '%Y-%m-%dT%H:%M:%S')
                if results[-1].get('updated_time'):
                    d = datetime.strptime(results[-1].get('updated_time')[:-5], '%Y-%m-%dT%H:%M:%S')
                last_post_date = d
            except GraphAPIError:
                print('GraphAPIError: An unknown error has occurred.')
                break
    return results

def facebook_get_post_comments(post_id):
    '''
    Facebook get post comments.
    :param post_id: the identification number of post
    :return: the list of post comments
    '''
    fb_api = facebook.GraphAPI(access_token=config.facebook_token, version=2.7)

    post_id = str(post_id)
    try:
        post_comments = fb_api.request('/' + post_id + '/comments')
    except GraphAPIError:
        print('GraphAPIError: Unsupported get request. Object with ID'+post_id+' does not exist, cannot be loaded due to missing permissions, or does not support this operation.')
        post_comments = []
    return post_comments

def facebook_search_user(query, max_results=5000):
    '''
    Facebook search user.
    :param query: search keyword
    :param max_results: maximal number of results, default is 5000
    :return: the list of users
    '''
    fb_api = facebook.GraphAPI(access_token=config.facebook_token, version=2.7)

    max_results = str(max_results)
    users = fb_api.request('/search?q='+query+'&type=user&limit='+max_results+'')
    return users

def facebook_search_location(query, lat_long, distance=100, max_results=100):
    '''
    Facebook search places on particular location.
    :param query: search keyword
    :param lat_long: the latitude and longitude of the location center in format 'xx.xx,yyy.yy'
    :param distance: the radius around location center in kilometres, default is 100 km
    :param max_results: maximal number of results, default (limit) is 100
    :return: the list of places around particular coordinates
    '''
    fb_api = facebook.GraphAPI(access_token=config.facebook_token, version=2.7)

    distance = str(int(distance)*1000)
    max_results = str(max_results)
    location = fb_api.request('/search?q='+query+'&type=place&center='+lat_long+'&distance='+distance+'&limit='+max_results+'&fields=id,name,location')
    return location

# TUMBLR
def tumblr_get_blog_info(blog):
    '''
    Tumblr get blog info.
    :param blog: the identification number of a blog
    :return: the information about a blog
    '''
    consumer = oauth2.Consumer(config.tumblr_consumer_key, config.tumblr_consumer_secret)
    client = oauth2.Client(consumer)
    resp, content = client.request(config.tumblr_request_token_url, "GET")
    request_token = dict(urlparse.parse_qsl(content))
    oauth_token = request_token['oauth_token']
    oauth_token_secret = request_token['oauth_token_secret']
    client = pytumblr.TumblrRestClient(config.tumblr_consumer_key, config.tumblr_consumer_secret, oauth_token, oauth_token_secret)

    info = client.blog_info(blog)
    return info

def tumblr_search(query, before_date=datetime.now().strftime("%Y-%m-%d"), max_results=20):
    '''
    Tumblr basic search posts.
    :param query: search keyword
    :param before_date: the date to search to in format 'Y-M-D', default is 'today'
    :param max_results: the number of maximal results per request, default is 20
    :return: the list of posts
    '''
    consumer = oauth2.Consumer(config.tumblr_consumer_key, config.tumblr_consumer_secret)
    client = oauth2.Client(consumer)
    resp, content = client.request(config.tumblr_request_token_url, "GET")
    request_token = dict(urlparse.parse_qsl(content))
    oauth_token = request_token['oauth_token']
    oauth_token_secret = request_token['oauth_token_secret']
    client = pytumblr.TumblrRestClient(config.tumblr_consumer_key, config.tumblr_consumer_secret, oauth_token, oauth_token_secret)

    timestamp = int(time.mktime(datetime.strptime(before_date, "%Y-%m-%d").timetuple()))
    results = []
    search_tag = client.tagged(query, before=timestamp, limit=max_results)
    results += search_tag
    timestamps = []
    while(search_tag):
        if 'errors' not in search_tag:
            for item in search_tag:
                timestamps.append(item.get('timestamp'))
            min_timestamp = min(timestamps)
            search_tag = client.tagged(tag=query, before=min_timestamp, limit=max_results)
            results += search_tag
        else:
            search_tag = []
    return results

def tumblr_search_after(query, date = '2000-01-01', max_results=20):
    '''
    Tumblr basic search posts after particular date.
    :param query: search keyword
    :param date: the date to search from in format 'Y-M-D', default is '2000-01-01'
    :return: the list of posts
    '''
    consumer = oauth2.Consumer(config.tumblr_consumer_key, config.tumblr_consumer_secret)
    client = oauth2.Client(consumer)
    resp, content = client.request(config.tumblr_request_token_url, "GET")
    request_token = dict(urlparse.parse_qsl(content))
    oauth_token = request_token['oauth_token']
    oauth_token_secret = request_token['oauth_token_secret']
    client = pytumblr.TumblrRestClient(config.tumblr_consumer_key, config.tumblr_consumer_secret, oauth_token, oauth_token_secret)

    after_date = datetime.strptime(date, "%Y-%m-%d")
    results = []
    # search last < 20 posts
    search_tag = client.tagged(query, limit=max_results)
    if len(search_tag) > 0:
        for item in search_tag:
            results.append(item)
        last_date = datetime.strptime(item['date'], '%Y-%m-%d %H:%M:%S GMT')
        last_timestamp = item['timestamp']

        # if there are more than 20 posts, search forward
        if len(results) > 19:
            while last_date > after_date:
                search_tag = client.tagged(tag=query, before=last_timestamp)
                last_date = datetime.strptime(item['date'], '%Y-%m-%d %H:%M:%S GMT')
                last_timestamp = item['timestamp']
                for item in search_tag:
                    results.append(item)
    else:
        print('Empty results.')
    return results

def tumblr_search_from_to(query, date_from, date_to, max_results=20):
    '''
    Tumblr basic search posts between dates.
    :param query: search keyword
    :param date_from: the date to search from in format 'Y-M-D'
    :param date_to: the date to search to in format 'Y-M-D'
    :return: the list of posts
    '''
    consumer = oauth2.Consumer(config.tumblr_consumer_key, config.tumblr_consumer_secret)
    client = oauth2.Client(consumer)
    resp, content = client.request(config.tumblr_request_token_url, "GET")
    request_token = dict(urlparse.parse_qsl(content))
    oauth_token = request_token['oauth_token']
    oauth_token_secret = request_token['oauth_token_secret']
    client = pytumblr.TumblrRestClient(config.tumblr_consumer_key, config.tumblr_consumer_secret, oauth_token, oauth_token_secret)

    date_from = date_from + 'T00:00:00'
    date_from = datetime.strptime(date_from, "%Y-%m-%dT%H:%M:%S")
    date_to = date_to + 'T23:59:59'
    date_to = datetime.strptime(date_to, "%Y-%m-%dT%H:%M:%S")
    results = []
    # search last < 20 posts
    search_tag = client.tagged(query, limit=max_results)
    if len(search_tag) > 0:
        for item in search_tag:
            results.append(item)
        last_date = datetime.strptime(item['date'], '%Y-%m-%d %H:%M:%S GMT')
        last_timestamp = item['timestamp']

        # if there are more than 20 posts, search forward
        if len(results) > 19:
            while last_date > date_from:
                search_tag = client.tagged(tag=query, before=last_timestamp)
                last_date = datetime.strptime(item['date'], '%Y-%m-%d %H:%M:%S GMT')
                last_timestamp = item['timestamp']
                for item in search_tag:
                    results.append(item)
    else:
        print('Empty results.')

    filtered_results = []
    for r in results:
        r_date = datetime.strptime(r['date'], '%Y-%m-%d %H:%M:%S GMT')
        if r_date <= date_to and r_date >= date_from:
            filtered_results.append(r)
    return filtered_results

def tumblr_get_blog_posts(blog, number_of_posts=20):
    '''
    Tumblr get posts of particular blog.
    :param blog: the identification number of a blog
    :param number_of_posts: the number of maximal results per request, default is 20
    :return: the list of posts
    '''
    consumer = oauth2.Consumer(config.tumblr_consumer_key, config.tumblr_consumer_secret)
    client = oauth2.Client(consumer)
    resp, content = client.request(config.tumblr_request_token_url, "GET")
    request_token = dict(urlparse.parse_qsl(content))
    oauth_token = request_token['oauth_token']
    oauth_token_secret = request_token['oauth_token_secret']
    client = pytumblr.TumblrRestClient(config.tumblr_consumer_key, config.tumblr_consumer_secret, oauth_token, oauth_token_secret)

    info = client.blog_info(blog)
    total_posts = info.get('blog').get('total_posts')
    offsets = range(0, total_posts, number_of_posts)
    results = []
    for offset in offsets:
        blog_posts = client.posts(blog, offset=offset, limit=number_of_posts, notes_info=True)
        results += blog_posts.get('posts')
    return results

def tumblr_get_blog_post(blog_id, post_id):
    '''
    Tumblr get particular post of particular blog.
    :param blog_id: the identification number of a blog
    :param post_id: the identificatino number of a post
    :return: post
    '''
    consumer = oauth2.Consumer(config.tumblr_consumer_key, config.tumblr_consumer_secret)
    client = oauth2.Client(consumer)
    resp, content = client.request(config.tumblr_request_token_url, "GET")
    request_token = dict(urlparse.parse_qsl(content))
    oauth_token = request_token['oauth_token']
    oauth_token_secret = request_token['oauth_token_secret']
    client = pytumblr.TumblrRestClient(config.tumblr_consumer_key, config.tumblr_consumer_secret, oauth_token, oauth_token_secret)

    post = client.posts(blog_id, post_id, notes_info=True)
    return post

# TWITTER
def twitter_get_user_tweets(username, number_of_tweets=200):
    '''
    Twitter get tweets of a particular user.
    :param username: the username of a user
    :param number_of_tweets: the number of tweets per request, default is 200
    :return: the list of tweets
    '''
    auth = tweepy.OAuthHandler(config.twitter_consumer_key, config.twitter_consumer_secret)
    auth.set_access_token(config.twitter_access_key, config.twitter_access_secret)
    api = tweepy.API(auth)

    user = api.get_user(username)
    number_of_user_tweets = user._json.get('statuses_count')
    results = []
    if (number_of_user_tweets > 0):
        tweets = api.user_timeline(screen_name=username, count=number_of_tweets)
        results += tweets
        before = str(tweets[-1]._json.get('id')-1)
        while tweets:
            tweets = api.user_timeline(screen_name=username, count=number_of_tweets, max_id=before)
            if len(tweets) > 0:
                before = str(tweets[-1]._json.get('id')-1)
                results += tweets
    return results

def twitter_get_user_info(user_id):
    '''
    Twitter get user info.
    :param user_id: the idetification number of a user
    :return: the information about user
    '''
    auth = tweepy.OAuthHandler(config.twitter_consumer_key, config.twitter_consumer_secret)
    auth.set_access_token(config.twitter_access_key, config.twitter_access_secret)
    api = tweepy.API(auth)

    try:
        user = api.get_user(user_id)
    except tweepy.TweepError:
        print('TweepError: [{u\'message\': u\'User has been suspended.\', u\'code\': 63}]')
        user = []
    return user

def twitter_search_user(query, max_results=20, max_page=51):
    '''
    Twitter search user.
    :param query: search keyword
    :param max_results: maximal number of results per request, default is 20
    :param max_page: maximal number of result pages, default is 51
    :return: the list of users
    '''
    auth = tweepy.OAuthHandler(config.twitter_consumer_key, config.twitter_consumer_secret)
    auth.set_access_token(config.twitter_access_key, config.twitter_access_secret)
    api = tweepy.API(auth)

    results = []
    ids = []
    page = 0
    retrieve = True
    while (retrieve and page < max_page):
        page = page + 1
        users = api.search_users(q=query, count=max_results, page=page)
        if users:
            if users[0].id in ids:
                retrieve = False
            else:
                ids = ids + users.ids()
                results += users
    return results

def twitter_search(query, number_of_tweets=100):
    '''
    Twitter basic search posts.
    :param query: search keyword
    :param number_of_tweets: the number of maximal tweets per request, default is 100
    :return: the list of tweets
    '''
    auth = tweepy.OAuthHandler(config.twitter_consumer_key, config.twitter_consumer_secret)
    auth.set_access_token(config.twitter_access_key, config.twitter_access_secret)
    api = tweepy.API(auth)

    results = []
    tweets = api.search(q=query, count=number_of_tweets)
    results += tweets
    if len(tweets) > 0:
        before = str(tweets[-1]._json.get('id')-1)
        while tweets:
            tweets = api.search(q=query, count=number_of_tweets, max_id=before)
            if len(tweets) > 0:
                before = str(tweets[-1]._json.get('id')-1)
                results += tweets
    return results

def twitter_search_after(query, date = '2000-01-01', number_of_tweets=100):
    '''
    Twitter search posts after particular date.
    :param query: search keyword
    :param number_of_tweets: the number of maximal tweets per request, default is 100
    :param date: the date to search from in format 'Y-M-D', default is '2000-01-01'
    :return: the list of tweets
    '''
    auth = tweepy.OAuthHandler(config.twitter_consumer_key, config.twitter_consumer_secret)
    auth.set_access_token(config.twitter_access_key, config.twitter_access_secret)
    api = tweepy.API(auth)
    #api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    results = []
    tweets = api.search(q=query, count=number_of_tweets, since=date)
    results += tweets
    if len(tweets) > 0:
        before = str(tweets[-1]._json.get('id')-1)
        while tweets:
            tweets = api.search(q=query, count=number_of_tweets, max_id=before, since=date)
            if len(tweets) > 0:
                before = str(tweets[-1]._json.get('id')-1)
                results += tweets
    return results

def twitter_search_from_to(query, date_from, date_to, number_of_tweets=100):
    '''
    Twitter search posts between dates.
    :param query: search keyword
    :param number_of_tweets: the number of maximal tweets per request, default is 100
    :param date_from: the date to search from in format 'Y-M-D'
    :param date_to: the date to search to in format 'Y-M-D'
    :return: the list of tweets
    '''
    auth = tweepy.OAuthHandler(config.twitter_consumer_key, config.twitter_consumer_secret)
    auth.set_access_token(config.twitter_access_key, config.twitter_access_secret)
    api = tweepy.API(auth)

    date_to = (datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    results = []
    tweets = api.search(q=query, count=number_of_tweets, since=date_from, until=date_to)
    results += tweets
    if len(tweets) > 0:
        before = str(tweets[-1]._json.get('id')-1)
        while tweets:
            tweets = api.search(q=query, count=number_of_tweets, max_id=before, since=date_from)
            if len(tweets) > 0:
                before = str(tweets[-1]._json.get('id')-1)
                results += tweets
    return results

def twitter_search_on_location(query, location, radius=100, number_of_tweets=100):
    '''
    Twitter search tweets on a particular location.
    :param query: search keyword
    :param location: the latitude and longitude of the location center in format 'xx.xx,yyy.yy'
    :param radius: the radius around location center in kilometres, default is 100
    :param number_of_tweets: maximal number of results, default is 1000
    :return: the list of tweets around a particular coordinates
    '''
    auth = tweepy.OAuthHandler(config.twitter_consumer_key, config.twitter_consumer_secret)
    auth.set_access_token(config.twitter_access_key, config.twitter_access_secret)
    api = tweepy.API(auth)
    #api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    results = []
    loc = location+','+str(radius)+'km'
    tweets = api.search(q=query, geocode=loc, count=number_of_tweets)
    if tweets:
        results += tweets
        before = str(tweets[-1]._json.get('id')-1)
        while tweets:
            tweets = api.search(q=query, geocode=loc, count=number_of_tweets, max_id=before)
            if len(tweets) > 0:
                before = str(tweets[-1]._json.get('id')-1)
                results += tweets
    return results

def twitter_search_country(country, count=100):
    '''
    Twitter search tweets in a particular country.
    :param country: country name
    :param count: the maximal number of tweets, default is 100
    :return:
    '''
    auth = tweepy.OAuthHandler(config.twitter_consumer_key, config.twitter_consumer_secret)
    auth.set_access_token(config.twitter_access_key, config.twitter_access_secret)
    api = tweepy.API(auth)

    places = api.geo_search(query=country, granularity="country")
    place_id = places[0].id
    tweets = api.search(q="place:%s" % place_id, count=count)
    return tweets

# GOOGLE+
def googleplus_search_user(query, max_results=50):
    '''
    Google+ search user.
    :param query: search keyword
    :param max_results: maximal number of results per request, default is 50
    :return: the list of users
    '''
    service = apiclient.discovery.build('plus', 'v1', http=httplib2.Http(), developerKey=config.google_api_key)

    results = []
    people_feed = service.people().search(query=query, maxResults=max_results).execute()
    results += people_feed.get('items')
    nextPageToken = people_feed.get("nextPageToken")
    while nextPageToken:
        people_feed = service.people().search(query=query, maxResults=max_results, pageToken=nextPageToken).execute()
        results += people_feed.get('items')
        nextPageToken = people_feed.get("nextPageToken")
    return results

def googleplus_get_user_posts(user_id, max_results=100):
    '''
    Google+ get posts of a particular user.
    :param user_id: the identification number of a user
    :param max_results: the number of posts per request, default is 100
    :return: the list of posts
    '''
    service = apiclient.discovery.build('plus', 'v1', http=httplib2.Http(), developerKey=config.google_api_key)

    results = []
    activity_feed = service.activities().list(userId=user_id, collection='public', maxResults=max_results).execute()
    results += activity_feed.get('items')
    nextPageToken = activity_feed.get("nextPageToken")
    while nextPageToken:
        activity_feed = service.activities().list(userId=user_id, collection='public', maxResults=max_results, pageToken=nextPageToken).execute()
        results += activity_feed.get('items')
        nextPageToken = activity_feed.get("nextPageToken")
    return results

def googleplus_search(query, date='2000-01-01', max_results_page=20):
    '''
    Google+ basic search posts.
    :param query: search keyword
    :param date: the date to search from in format 'Y-M-D', default is '2000-01-01'
    :param max_results_page: maximal number of result pages, default is 20
    :return: the list of posts
    '''
    service = apiclient.discovery.build('plus', 'v1', http=httplib2.Http(), developerKey=config.google_api_key)

    results = []
    activity = service.activities().search(query=query, maxResults=max_results_page).execute()
    results += activity.get('items')
    if len(results) > 0:
        nextPageToken = activity.get("nextPageToken")
        date = date+'T00:00:00Z'
        date_from = datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ")
        last_date = datetime.strptime(results[-1].get('published'), "%Y-%m-%dT%H:%M:%S.%fZ")
        if last_date > date_from:
            while last_date > date_from and len(activity.get('items')) > 0:
                print(nextPageToken + ' ' + str(last_date))
                try:
                    activity = service.activities().search(query=query, maxResults=max_results_page, pageToken=nextPageToken).execute()
                except:
                    print('Backend Error')
                    pass
                results += activity.get('items')
                nextPageToken = activity.get("nextPageToken")
                last_date = datetime.strptime(results[-1].get('published'), "%Y-%m-%dT%H:%M:%S.%fZ")
    return results

def googleplus_search_from_to(query, date_from, date_to, max_results_page=20):
    '''
    Google+ basic search posts between dates.
    :param query: search keyword
    :param date_from: the date to search from in format 'Y-M-D'
    :param date_to: the date to search to from in format 'Y-M-D'
    :param max_results_page: maximal number of result pages, default is 20
    :return: the list of posts
    '''
    service = apiclient.discovery.build('plus', 'v1', http=httplib2.Http(), developerKey=config.google_api_key)

    results = []
    date_from = date_from + 'T00:00:00Z'
    date_from = datetime.strptime(date_from, "%Y-%m-%dT%H:%M:%SZ")
    date_to = date_to + 'T23:59:59Z'
    date_to = datetime.strptime(date_to, "%Y-%m-%dT%H:%M:%SZ")

    activity = service.activities().search(query=query, maxResults=max_results_page, orderBy='recent').execute()
    results += activity.get('items')
    if len(results) > 0:
        nextPageToken = activity.get("nextPageToken")
        last_date = datetime.strptime(results[-1].get('published'), "%Y-%m-%dT%H:%M:%S.%fZ")
        if last_date > date_from:
            while last_date > date_from and len(activity.get('items')) > 0:
                print(nextPageToken + ' ' + str(last_date))
                try:
                    activity = service.activities().search(query=query, maxResults=max_results_page, pageToken=nextPageToken,  orderBy='recent').execute()
                except:
                    print('Backend Error')
                    pass
                results += activity.get('items')
                nextPageToken = activity.get("nextPageToken")
                last_date = datetime.strptime(results[-1].get('published'), "%Y-%m-%dT%H:%M:%S.%fZ")
    filtered_results = []
    for r in results:
        r_date = datetime.strptime(r.get('published'), "%Y-%m-%dT%H:%M:%S.%fZ")
        if r_date <= date_to and r_date >= date_from:
            filtered_results.append(r)
    return filtered_results

def googleplus_get_post_comments(post_id, max_results = 500):
    '''
    Google+ get post comments.
    :param post_id: the identification number of a post
    :param max_results: the maximal number of results, default is 500
    :return: the list of post comments
    '''
    service = apiclient.discovery.build('plus', 'v1', http=httplib2.Http(), developerKey=config.google_api_key)

    comments = service.comments().list(activityId=post_id, maxResults=max_results).execute()
    return comments

def googleplus_get_post_info(post_id):
    '''
    Google+ get post info.
    :param post_id: the identification number of a post
    :return: the information about a post
    '''
    service = apiclient.discovery.build('plus', 'v1', http=httplib2.Http(), developerKey=config.google_api_key)

    activities_service = service.activities()
    activity = activities_service.get(activityId=post_id).execute()
    return activity

# YOUTUBE
def youtube_search(query, max_results=50):
    '''
    Youtube basic search videos, channels and playlists.
    :param query: search keyword
    :param max_results: maximal number of results per page, default is 50
    :return: triple (videos, channels, playlists), where esearch videos, channels and playlists are lists of results: search videos, channels and playlists, respectively
    '''
    service = apiclient.discovery.build('youtube', 'v3', http=httplib2.Http(), developerKey = config.google_api_key)

    results = []
    search_response = service.search().list(q=query, part="id,snippet", maxResults=max_results).execute()
    results += search_response.get('items')
    nextPageToken = search_response.get("nextPageToken")
    while nextPageToken:
        search_response = service.search().list(q=query, part="id,snippet", maxResults=max_results, pageToken=nextPageToken).execute()
        results += search_response.get('items')
        nextPageToken = search_response.get("nextPageToken")

    videos = []
    channels = []
    playlists = []
    for search_result in results:
        if search_result["id"]["kind"] == "youtube#video":
            videos.append(search_result)
        elif search_result["id"]["kind"] == "youtube#channel":
            channels.append(search_result)
        elif search_result["id"]["kind"] == "youtube#playlist":
            playlists.append(search_result)
    return (videos, channels, playlists)

def youtube_get_video_comments(video_id, maxResults=100):
    '''
    Youtube get video comments.
    :param video_id: the identification number of a video
    :param maxResults: the maximal number of results per page, default is 100
    :return: the list of video comments
    '''
    service = apiclient.discovery.build('youtube', 'v3', http=httplib2.Http(), developerKey = config.google_api_key)

    results = []
    search_response = service.commentThreads().list(part="snippet,replies", videoId=video_id, textFormat="plainText", maxResults=maxResults).execute()
    results += search_response.get('items')
    nextPageToken = search_response.get("nextPageToken")
    while nextPageToken:
        search_response = service.commentThreads().list(part="snippet,replies", videoId=video_id, textFormat="plainText", maxResults=maxResults, pageToken=nextPageToken).execute()
        results += search_response.get('items')
        nextPageToken = search_response.get("nextPageToken")
    return results

def youtube_get_channel_info(channel_id):
    '''
    Youtube get channel info.
    :param channel_id: the identification number of a channel
    :return: the information about a channel
    '''
    service = apiclient.discovery.build('youtube', 'v3', http=httplib2.Http(), developerKey = config.google_api_key)

    search_response = service.channels().list(part="snippet,statistics",id=channel_id).execute()
    return search_response

def youtube_get_video_info(video_id):
    '''
    Youtube get video info.
    :param video_id: the identification number of a video
    :return: the information about a video
    '''
    service = apiclient.discovery.build('youtube', 'v3', http=httplib2.Http(), developerKey = config.google_api_key)

    search_response = service.videos().list(part="snippet,statistics,recordingDetails",id=video_id).execute()
    return search_response

def youtube_search_video_location(query, location, radius=100, max_results=50):
    '''
    Youtube search videos on a particular location.
    :param query: search keyword
    :param location: the latitude and longitude of the location center in format 'xx.xx,yyy.yy'
    :param radius: the radius around location center in kilometres, default is 100
    :param max_results: maximal number of results per page, default is 50
    :return: the list of videos around a particular coordinates
    '''
    service = apiclient.discovery.build('youtube', 'v3', http=httplib2.Http(), developerKey = config.google_api_key)

    distance = str(radius)+'km'
    results = []
    search_response = service.search().list(q=query, location=location, locationRadius=distance, part="id,snippet", order='date', type='video', maxResults=max_results).execute()
    results += search_response.get('items')
    nextPageToken = search_response.get("nextPageToken")
    while nextPageToken:
        search_response = service.search().list(q=query, location=location, locationRadius=distance, part="id,snippet", order='date', type='video', maxResults=max_results, pageToken=nextPageToken).execute()
        results += search_response.get('items')
        nextPageToken = search_response.get("nextPageToken")
    return results

def youtube_search_video_location_from_to(query, date_from, date_to, location, radius=100, max_results=50):
    '''
    Youtube search videos on a particular location between dates.
    :param query: search keyword
    :param date_from: the date to search from in format 'Y-M-D'
    :param date_to: the date to search to in format 'Y-M-D'
    :param location: the latitude and longitude of the location center in format 'xx.xx,yyy.yy'
    :param radius: the radius around location center in kilometres, default is 100
    :param max_results: maximal number of results per page, default is 50
    :return: the list of videos around a particular coordinates
    '''
    service = apiclient.discovery.build('youtube', 'v3', http=httplib2.Http(), developerKey = config.google_api_key)

    date_from = date_from + 'T00:00:00Z'
    date_to = date_to + 'T23:59:59Z'
    distance = str(radius)+'km'
    results = []
    search_response = service.search().list(q=query, publishedBefore=date_to, publishedAfter=date_from, location=location, locationRadius=distance, part="id,snippet", order='date', type='video', maxResults=max_results).execute()
    results += search_response.get('items')
    nextPageToken = search_response.get("nextPageToken")
    while nextPageToken:
        search_response = service.search().list(q=query, publishedBefore=date_to, publishedAfter=date_from, location=location, locationRadius=distance, part="id,snippet", order='date', type='video', maxResults=max_results, pageToken=nextPageToken).execute()
        results += search_response.get('items')
        nextPageToken = search_response.get("nextPageToken")
    return results

def youtube_search_date_after(query, date='2000-01-01', max_results=50):
    '''
    Youtube basic search videos after a particular date.
    :param query: search keyword
    :param date: the date to search from in format 'Y-M-D', default is '2000-01-01'
    :param max_results: maximal number of results per page, default is 50
    :return: the list of videos
    '''
    service = apiclient.discovery.build('youtube', 'v3', http=httplib2.Http(), developerKey = config.google_api_key)

    date = date+'T00:00:00Z'
    results = []
    search_response = service.search().list(q=query, publishedAfter=date, part="id,snippet", order='date',  maxResults=max_results).execute()
    results += search_response.get('items')
    nextPageToken = search_response.get("nextPageToken")
    while nextPageToken:
        search_response = service.search().list(q=query, publishedAfter=date, part="id,snippet", order='date', maxResults=max_results, pageToken=nextPageToken).execute()
        results += search_response.get('items')
        nextPageToken = search_response.get("nextPageToken")

    videos = []
    channels = []
    playlists = []
    for search_result in results:
        if search_result["id"]["kind"] == "youtube#video":
            videos.append(search_result)
        elif search_result["id"]["kind"] == "youtube#channel":
            channels.append(search_result)
        elif search_result["id"]["kind"] == "youtube#playlist":
            playlists.append(search_result)
    return (videos, channels, playlists)

def youtube_search_date_before(query, date=datetime.now().strftime("%Y-%m-%d"), max_results=50):
    '''
    Youtube basic search videos before a particular date.
    :param query: search keyword
    :param date: the date to search to in format 'Y-M-D', default is 'today'
    :param max_results: maximal number of results per page, default is 50
    :return: the list of videos
    '''
    service = apiclient.discovery.build('youtube', 'v3', http=httplib2.Http(), developerKey = config.google_api_key)

    date = date+'T00:00:00Z'
    results = []
    search_response = service.search().list(q=query, publishedBefore=date, part="id,snippet", order='date', type='video', maxResults=max_results).execute()
    results += search_response.get('items')
    nextPageToken = search_response.get("nextPageToken")
    while nextPageToken:
        search_response = service.search().list(q=query, publishedBefore=date, part="id,snippet", order='date', type='video', maxResults=max_results, pageToken=nextPageToken).execute()
        results += search_response.get('items')
        nextPageToken = search_response.get("nextPageToken")
    return results

def youtube_search_from_to(query, date_from, date_to, max_results=50):
    '''
    Youtube basic search videos between dates.
    :param query: search keyword
    :param date_from: the date to search from in format 'Y-M-D'
    :param date_to: the date to search to in format 'Y-M-D'
    :param max_results: maximal number of results per page, default is 50
    :return: the list of videos
    '''
    service = apiclient.discovery.build('youtube', 'v3', http=httplib2.Http(), developerKey = config.google_api_key)

    results = []
    date_from = date_from + 'T00:00:00Z'
    date_to = date_to + 'T23:59:59Z'

    search_response = service.search().list(q=query, publishedBefore=date_to, publishedAfter=date_from, part="id,snippet", order='date', type='video', maxResults=max_results).execute()
    results += search_response.get('items')
    nextPageToken = search_response.get("nextPageToken")
    while nextPageToken:
        search_response = service.search().list(q=query, publishedBefore=date_to, publishedAfter=date_from, part="id,snippet", order='date', type='video', maxResults=max_results, pageToken=nextPageToken).execute()
        results += search_response.get('items')
        nextPageToken = search_response.get("nextPageToken")

    videos = []
    channels = []
    playlists = []
    for search_result in results:
        if search_result["id"]["kind"] == "youtube#video":
            videos.append(search_result)
        elif search_result["id"]["kind"] == "youtube#channel":
            channels.append(search_result)
        elif search_result["id"]["kind"] == "youtube#playlist":
            playlists.append(search_result)
    return (videos, channels, playlists)

# BLOGGER
def blogger_get_blog_info(blog_id):
    '''
    Blogger get blog info by its id.
    :param blog_id: the identification number of a blog
    :return: the information about a blog
    '''
    service = apiclient.discovery.build('blogger', 'v3', http=httplib2.Http(), developerKey=config.google_api_key)

    blog = service.blogs().get(blogId=blog_id).execute()
    return blog

def blogger_get_blog_info_byurl(blog_url):
    '''
    Blogger get blog info by its URL.
    :param blog_url: the URL of a blog
    :return: the information about a blog
    '''
    service = apiclient.discovery.build('blogger', 'v3', http=httplib2.Http(), developerKey=config.google_api_key)

    blog = service.blogs().getByUrl(url=blog_url).execute()
    return blog

def blogger_get_blog_posts(blog_id, from_date='2000-01-01', max_results=500):
    '''
    Blogger get posts of a blog after a particular date.
    :param blog_id: the identification number of a blog
    :param from_date: the date to search from in format 'Y-M-D', default is '2000-01-01'
    :param max_results: maximal number of results per page, default is 500
    :return: the list of posts
    '''
    service = apiclient.discovery.build('blogger', 'v3', http=httplib2.Http(), developerKey=config.google_api_key)

    date = from_date+'T00:00:00Z'
    results = []
    blog = service.posts().list(blogId=blog_id, startDate=date, maxResults=max_results).execute()
    results += blog.get('items')
    nextPageToken = blog.get("nextPageToken")
    while nextPageToken:
        blog = service.posts().list(blogId=blog_id, startDate=date, maxResults=max_results, pageToken=nextPageToken).execute()
        results += blog.get('items')
        nextPageToken = blog.get("nextPageToken")
    return results

def blogger_get_blog_post(blog_id, post_id):
    '''
    Blogger get specific post of a blog.
    :param blog_id: the identification number of a blog
    :param post_id: the identification number of a post
    :return: post
    '''
    service = apiclient.discovery.build('blogger', 'v3', http=httplib2.Http(), developerKey=config.google_api_key)

    post_by_id = service.posts().get(blogId=blog_id, postId=post_id).execute()
    return post_by_id

def blogger_get_blog_post_comments(blog_id, post_id, max_results=500):
    '''
    Blogger get post comments.
    :param blog_id: the identification number of a blog
    :param post_id: the identification number of a post
    :param max_results: the maximal number of results per page, default is 500
    :return: the list of post comments
    '''
    service = apiclient.discovery.build('blogger', 'v3', http=httplib2.Http(), developerKey=config.google_api_key)

    results = []
    comments_of_post = service.comments().list(blogId=blog_id, postId=post_id, maxResults=max_results).execute()
    results += comments_of_post.get('items')
    nextPageToken = comments_of_post.get("nextPageToken")
    while nextPageToken:
        comments_of_post = service.comments().list(blogId=blog_id, postId=post_id, maxResults=max_results, pageToken=nextPageToken).execute()
        results += comments_of_post.get('items')
        nextPageToken = comments_of_post.get("nextPageToken")
    return results

def blogger_search_blog(query, blog_id):
    '''
    Blogger basic search a particular blog.
    :param query: search keyword
    :param blog_id: the identification number of a blog
    :return: the list of posts
    '''
    blog_id = str(blog_id)
    results = []
    url = 'https://www.googleapis.com/blogger/v3/blogs/'+blog_id+'/posts/search?q='+query+'&maxResults=10&key=AIzaSyCZX9omDtYnNBLiFjXgsAmDeEFiMS5ZAAU'
    data = json.load(urllib2.urlopen(url))
    results += data.get('items')
    nextPageToken = data.get("nextPageToken")
    while nextPageToken:
        url = 'https://www.googleapis.com/blogger/v3/blogs/'+blog_id+'/posts/search?q='+query+'&maxResults=10&pageToken='+nextPageToken+'&key=AIzaSyCZX9omDtYnNBLiFjXgsAmDeEFiMS5ZAAU'
        data = json.load(urllib2.urlopen(url))
        results += data.get('items')
        nextPageToken = data.get("nextPageToken")
    return results

# FOURSQUARE
def foursquare_get_venue_info(venue_id):
    '''
    Foursquare get venue info.
    :param venue_id: the identification number of a venue
    :return: the information about a venue
    '''
    client = foursquare.Foursquare(client_id=config.foursquare_consumer_key, client_secret=config.foursquare_consumer_secret)

    venue = client.venues(venue_id)
    return venue

def foursquare_get_venue_tips(venue_id):
    '''
    Foursquare tips of particular venue.
    :param venue_id: the identification number of a venue
    :return: the list of tips about a particular venue
    '''
    client = foursquare.Foursquare(client_id=config.foursquare_consumer_key, client_secret=config.foursquare_consumer_secret)

    tips = client.venues.tips(venue_id, {'limit':200})
    tips_list = tips["tips"]["items"]
    tip_count = tips["tips"]["count"]

    while len(tips_list) < tip_count:
        tips = client.venues.tips(venue_id, {"offset": len(tips_list)})
        tips_list += tips["tips"]["items"]
    return tips_list

def foursquare_get_user_info(user_id):
    '''
    Foursquare user info.
    :param user_id: the identification number of a user
    :return: the information about a particular user
    '''
    client = foursquare.Foursquare(client_id=config.foursquare_consumer_key, client_secret=config.foursquare_consumer_secret)

    user = client.users(user_id)
    return user

def foursquare_search_city(city, max_results=50):
    '''
    Foursquare search location.
    :param city: search query, basically city
    :param max_results: the maximal number of results, default (limit) is 50
    :return: the list of venues
    '''
    client = foursquare.Foursquare(client_id=config.foursquare_consumer_key, client_secret=config.foursquare_consumer_secret)

    venues = client.venues.search(params={'near':city, 'limit':max_results})
    return venues

def foursquare_search_location(query, location, max_results=50):
    '''
    Foursquare search venues on a particular location.
    :param query: search keyword
    :param location: the latitude and longitude of the location center in format 'xx.xx,yyy.yy'
    :param max_results: maximal number of results, default is 50
    :return: the list of venued around a particular coordinates
    :return:
    '''
    client = foursquare.Foursquare(client_id=config.foursquare_consumer_key, client_secret=config.foursquare_consumer_secret)

    venues = client.venues.search(params={'ll':location,'query':query,'limit':max_results})
    return venues

# TRIPAVISOR
def tripadvisor_search_location (locality, checkin_date="2017/01/01", checkout_date="2017/01/02", sort='popularity'):
    '''
    Tripadvisor search venues on location.
    :param locality: search keyword - locatation to search
    :param checkin_date: checkin date in format 'Y/M/D', default is '2017/01/01'
    :param checkout_date: checkout date in format 'Y/M/D', default is '2017/01/02'
    :param sort: the sort of the results, default is popularity
    :return: the list of hotels with their information
    '''
    checkin_date = datetime.strptime(checkin_date, "%Y/%m/%d")
    checkout_date = datetime.strptime(checkout_date,"%Y/%m/%d")
    checkIn = checkin_date.strftime("%Y/%m/%d")
    checkOut = checkout_date.strftime("%Y/%m/%d")
    geo_url = 'https://www.tripadvisor.com/TypeAheadJson?action=API&startTime='+str(int(time.time()))+'&uiOrigin=GEOSCOPE&source=GEOSCOPE&interleaved=true&types=geo,theme_park&neighborhood_geos=true&link_type=hotel&details=true&max=12&injectNeighborhoods=true&query='+locality
    api_response  = requests.get(geo_url).json()
    hotel_data = []
    if api_response['results']:
        url_from_autocomplete = "http://www.tripadvisor.com"+api_response['results'][0]['url']
        geo = api_response['results'][0]['value']
        date = checkin_date.strftime("%Y_%m_%d")+"_"+checkout_date.strftime("%Y_%m_%d")
        form_data ={'adults': '2', 'dateBumped': 'NONE', 'displayedSortOrder':sort, 'geo': geo, 'hs': '',
                    'isFirstPageLoad': 'false', 'rad': '0', 'refineForm': 'true', 'requestingServlet': 'Hotels', 'rooms': '1',
                    'scid': 'null_coupon', 'searchAll': 'false', 'seen': '0', 'sequence': '7', 'o':"0", 'staydates': date}
        headers = {'Accept': 'text/javascript, text/html, application/xml, text/xml, */*', 'Accept-Encoding': 'gzip,deflate',
                   'Accept-Language': 'en-US,en;q=0.5', 'Cache-Control': 'no-cache', 'Connection': 'keep-alive',
                    'Content-Type': 'application/x-www-form-urlencoded; charset=utf-8', 'Host': 'www.tripadvisor.com',
                    'Pragma': 'no-cache', 'Referer': url_from_autocomplete,
                    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:28.0) Gecko/20100101 Firefox/28.0', 'X-Requested-With': 'XMLHttpRequest'}
        cookies=  {"SetCurrency":"USD"}
        page_response  = requests.post(url = "https://www.tripadvisor.com/Hotels",data=form_data,headers = headers, cookies = cookies).text
        parser = html.fromstring(page_response)
        hotel_lists = parser.xpath('//div[contains(@id,"HOTELDEAL")]')

        for hotel in hotel_lists:
            XPATH_HOTEL_LINK = './/div[@class="listing_title"]/a/@href'
            XPATH_REVIEWS  = './/span[@class="reviewCount"]//text()'
            XPATH_RATING = './/span[contains(@property,"ratingValue")]/@content'
            XPATH_HOTEL_NAME = './/a[contains(@class,"property_title")]//text()'
            XPATH_HOTEL_PRICE = './/div[contains(@class,"price")]/text()'
            XPATH_VIEW_DEALS = './/div[contains(@id,"VIEW_ALL_DEALS")]//span[@class="viewAll"]/text()'
            XPATH_BOOKING_PROVIDER = './/span[contains(@data-sizegroup,"mini-meta-provider")]//text()'

            raw_booking_provider = hotel.xpath(XPATH_BOOKING_PROVIDER)
            raw_no_of_deals =  hotel.xpath(XPATH_VIEW_DEALS)
            raw_hotel_link = hotel.xpath(XPATH_HOTEL_LINK)
            raw_no_of_reviews = hotel.xpath(XPATH_REVIEWS)
            raw_rating = hotel.xpath(XPATH_RATING)
            raw_hotel_name = hotel.xpath(XPATH_HOTEL_NAME)
            raw_hotel_price_per_night  = hotel.xpath(XPATH_HOTEL_PRICE)

            url = 'http://www.tripadvisor.com'+raw_hotel_link[0] if raw_hotel_link else  None
            hotel_id = url.split('-')[2][1:]
            reviews = re.findall('(\d*\,?\d+)',raw_no_of_reviews[0])[0].replace(',','') if raw_no_of_reviews else None
            rating = ''.join(raw_rating).replace(' of 5 bubbles','') if raw_rating else None
            name = ''.join(raw_hotel_name).strip() if raw_hotel_name else None
            price_per_night = ''.join(raw_hotel_price_per_night).encode('utf-8').replace('\n','') if raw_hotel_price_per_night else None
            no_of_deals = re.findall("all\s+?(\d+)\s+?",''.join(raw_no_of_deals))
            if no_of_deals:
                no_of_deals = no_of_deals[0]
            else:
                no_of_deals = 0
            booking_provider = ''.join(raw_booking_provider).strip() if raw_booking_provider else None
            page_response = requests.get(url=url).text
            parser = html.fromstring(page_response)
            t = parser.xpath('.//script[contains(@type,"text/javascript")]/text()')
            for t1 in t:
                if ('\nlat' in t1):
                    tr = t1.split('\n')
                    lat = [i for i in tr if 'lat' in i][0].split(': ')[1]
                    lng = [i for i in tr if 'lng' in i][0].split(': ')[1]
            XPATH_ADDRESS = './/div[@class="content hidden"]//span[@class="street-address"]//text()'
            XPATH_LOCALITY = './/div[@class="content hidden"]//span[@class="locality"]//text()'
            XPATH_COUNTRY = './/div[@class="content hidden"]//span[@class="country-name"]//text()'
            XPATH_FEATURES = './/div[@class="detailsMid"]//div[@class="highlightedAmenity detailListItem"]//text()'
            address = parser.xpath(XPATH_ADDRESS)
            locality = parser.xpath(XPATH_LOCALITY)
            country = parser.xpath(XPATH_COUNTRY)
            features = parser.xpath(XPATH_FEATURES)

            data = {
                        'hotel_id':hotel_id,
                        'hotel_name':name,
                        'url':url,
                        'reviews':reviews,
                        'tripadvisor_rating':rating,
                        'checkOut':checkOut,
                        'checkIn':checkIn,
                        'hotel_features':features,
                        'price_per_night':price_per_night,
                        'no_of_deals':no_of_deals,
                        'booking_provider':booking_provider,
                        'address':address,
                        'locality':locality,
                        'country':country,
                        'coordinates':lat[:-1]+', '+lng[:-1]
            }
            hotel_data.append(data)
    return hotel_data
