# -*- coding: UTF-8 -*-
import methods
import json_search
import json_search_location
import config
import json
import zipfile
from os.path import basename

def srch_json(query, m, file, date_from, date_to, first, last, lang_sens, sent_sens, sens_name, sens_text, number_of_predictions, percent_of_same):
    '''
    Get posts and other hits on different social media according to specific query.
    :param query: search keyword
    :param m: social media to search
    :param file: file to store posts to
    :param date_from: the date to search from in format 'Y-M-D'
    :param date_to: the date to search to in format 'Y-M-D'
    :param first: if post is first, writting in json file differs
    :param last: if post is first, writting in json file differs
    :param lang_sens: sensibility for language detection
    :param sent_sens: sensibility for sentiment analysis
    :param sens_name: sensibility for gender determination based on username
    :param sens_text: sensibility for gender determination based on text
    :param number_of_predictions: how many predictions have to be made to determine gender based on username and text
    :param percent_of_same: how many predictions have to have the same gender to determine gender to particular user
    :return: number of posts that have been found
    '''
    tuples = {'facebook': (methods.facebook_search, json_search.facebook_mysql),
                'tumblr': (methods.tumblr_search_from_to, json_search.tumblr_mysql),
                'googleplus': (methods.googleplus_search_from_to, json_search.googleplus_mysql),
                'youtube': (methods.youtube_search_from_to, json_search.youtube_mysql)}
    print(m+' '+query)
    if m == 'twitter':
        posts = json_search.twitter_search_from_to(query, date_from, date_to, lang_sens, sent_sens, sens_name, sens_text, number_of_predictions, percent_of_same)
    else:
        func = tuples.get(m)[0]
        if m == 'facebook':
            results = func(query)
        else:
            results = func(query, date_from, date_to)
        func_print = tuples.get(m)[1]
        posts = func_print(results, query, date_from, date_to, lang_sens, sent_sens, sens_name, sens_text, number_of_predictions, percent_of_same)
        print(str(json_search.lang_detect))
    write_to_file(posts, m, file, first, last)
    return len(posts)


def srch_location_json(query, location, distance, m, file, date_from, date_to, first, last, lang_sens, sent_sens, sens_name, sens_text, number_of_predictions, percent_of_same):
    '''
    Get posts and other hits on different social media according to specific query on specific location.
    :param query: search keyword
    :param location: the latitude and longitude of the location center in format 'xx.xx,yyy.yy'
    :param distance: the radius around location center in kilometres, default is 100
    :param m: social media to search
    :param file: file to store posts to
    :param date_from: the date to search from in format 'Y-M-D'
    :param date_to: the date to search to in format 'Y-M-D'
    :param first: if post is first, writting in json file differs
    :param last: if post is first, writting in json file differs
    :param lang_sens: sensibility for language detection
    :param sent_sens: sensibility for sentiment analysis
    :param sens_name: sensibility for gender determination based on username
    :param sens_text: sensibility for gender determination based on text
    :param number_of_predictions: how many predictions have to be made to determine gender based on username and text
    :param percent_of_same: how many predictions have to have the same gender to determine gender to particular user
    :return: number of posts that have been found
    '''
    tuples = {'twitter': (json_search_location.twt_location_after_date, json_search_location.twitter_mysql),
                'youtube': (methods.youtube_search_video_location_from_to, json_search_location.youtube_mysql)}
    print(m+' '+query)
    func = tuples.get(m)[0]
    func_print = tuples.get(m)[1]
    if m == 'youtube':
        results = func(query, date_from, date_to, location, distance)
        posts = func_print(results, query, date_from, date_to, lang_sens, sent_sens, sens_name, sens_text, number_of_predictions, percent_of_same)
    else:
        posts = json_search_location.twt_location_after_date(query, date_from, date_to, lang_sens, sent_sens, sens_name, sens_text, number_of_predictions, percent_of_same, location, radius=distance)
    write_to_file(posts, m, file, first, last)
    return len(posts)

def write_to_file(posts, media, file, first, last):
    '''
    Function, that writes the data about posts in particular file or multiple files (in this case, the zip file is created).
    :param posts: post for writting
    :param media: social media, that posts are from
    :param file: filename to write in
    :param first: if post is first, writting in json file differs
    :param last: if post is first, writting in json file differs
    :return: /
    '''
    if type(file) is str and len(posts) > 0:
        with open(file, 'a') as outfile:
            if first == 1:
                outfile.write('{ \n')
            for p in posts.keys()[:-1]:
                post = posts[p]
                outfile.write('\"'+p+'\": ')
                json.dump(post, outfile, indent=2)
                outfile.write(',\n')
            if last == 1:
                post = posts[posts.keys()[-1]]
                outfile.write('\"' + posts.keys()[-1] + '\": ')
                json.dump(post, outfile, indent=2)
                outfile.write('}')
            else:
                post = posts[posts.keys()[-1]]
                outfile.write('\"' + posts.keys()[-1] + '\": ')
                json.dump(post, outfile, indent=2)
                outfile.write(',\n')

    else:
        for f in file[:-1]:
            if media in f:
                with open(f, 'w') as outfile:
                    json.dump(posts, outfile, indent=2)
                zip_file = zipfile.ZipFile(file[-1], 'a')
                zip_file.write(f, basename(f), compress_type=zipfile.ZIP_DEFLATED)
                zip_file.close()
