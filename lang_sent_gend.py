# -*- coding: UTF-8 -*-
from __future__ import print_function
import mysql.connector
from langdetect import DetectorFactory
DetectorFactory.seed = 0
import config
import text_processing
from collections import Counter

def set_lsg(keyword, soc_media, date_from, date_to, lang_sens, sent_sens, gender_sens, number_of_predictions, percent_of_same):
    '''
    Language, sentiment and gender detection for posts from selected social media, published from-to given dates and for selected keyword.
    :param keyword: selected keyword from database
    :param media: selected social media
    :param date_from: the date to search from in format 'Y-M-D'
    :param date_to: the date to search to in format 'Y-M-D'
    :param lang_sens: sensibility for language detection
    :param sent_sens: sensibility for sentiment analysis
    :param gender_sens: sensibility for gender determination based on username and text
    :param number_of_predictions: how many predictions have to be made to determine gender based on username and text
    :param percent_of_same: how many predictions have to have the same gender to determine gender to particular user
    :return: /
    '''
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()

    cursor.execute('''SELECT id FROM ''' + config.database + '''.social_media WHERE name = %s''', (soc_media,))
    media = cursor.fetchall()[0][0]

    cursor.execute(
        '''SELECT ''' + config.database + '''.`post`.`id`, ''' + config.database + '''.`post`.`text` ''' \
        '''FROM ''' + config.database + '''.`post` ''' \
        '''WHERE ''' + config.database + '''.`post`.`soc_media` IN (%s) AND ''' + config.database + '''.`post`.`keyword` = %s ''' \
        '''AND ''' + config.database + '''.`post`.`creation_time` >= %s AND ''' + config.database + '''.`post`.`creation_time` <= %s;''',
        (media, keyword, date_from, date_to))
    results = cursor.fetchall()

    for id, text in results:
        lang, sent, gend = detect_lsg(text.encode('utf-8'), lang_sens, sent_sens, gender_sens)
        cursor.execute(
            '''UPDATE ''' + config.database + '''.`post` ''' \
            '''SET ''' + config.database + '''.`post`.`language` = \'''' + lang + '''\', ''' + config.database + '''.`post`.`language_prob` = \'''' + str(lang_sens) + '''\', '''\
            + config.database + '''.`post`.`sentiment` = \'''' + sent + '''\', ''' + config.database + '''.`post`.`sentiment_prob` = \'''' + str(sent_sens) + '''\', '''\
             + config.database + '''.`post`.`gender` = \'''' + gend + '''\', ''' + config.database + '''.`post`.`gender_prob` = \'''' +  str(gender_sens)+'/'+str(number_of_predictions)+'/'+str(percent_of_same) + '''\' '''\
            '''WHERE ''' + config.database + '''.`post`.`id` = \'''' + str(id) + '''\';''')
        cnx.commit()
    cursor.close()
    cnx.close()

    set_user_gender(keyword, media, gender_sens, number_of_predictions, percent_of_same)

def set_user_gender(keyword, media, gender_sens, number_of_predictions, percent_of_same):
    '''
    Gender determination for posts and users from selected media.
    :param keyword: selected keyword from database
    :param media: selected social media
    :param gender_sens: sensibility for gender determination based on username and text
    :param number_of_predictions: how many predictions have to be made to determine gender based on username and text
    :param percent_of_same: how many predictions have to have the same gender to determine gender to particular user
    :return: /
    '''
    cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
    cursor = cnx.cursor()

    cursor.execute(
        '''SELECT ''' + config.database + '''.`user`.`id`, ''' + config.database + '''.`user`.`name`, ''' + config.database + '''.`user`.`screen_name` ''' \
        '''FROM ''' + config.database + '''.`user` ''' \
        '''WHERE ''' + config.database + '''.`user`.`soc_media` IN (%s) AND ''' + config.database + '''.`user`.`keyword` = %s;''',
        (media, keyword ))
    results = cursor.fetchall()

    for id, name1, name2 in results:
        gend = detect_user_gender(name1, name2, gender_sens)

        cursor.execute(
            '''SELECT ''' + config.database + '''.`post`.`id`, '''+ config.database + '''.`post`.`gender` ''' \
            '''FROM ''' + config.database + '''.`post` ''' \
            '''WHERE ''' + config.database + '''.`post`.`user` = %s;''', (id,))
        posts = cursor.fetchall()

        cursor.execute(
            '''SELECT ''' + config.database + '''.`comment`.`id`, ''' + config.database + '''.`comment`.`gender` ''' \
            '''FROM ''' + config.database + '''.`comment` ''' \
            '''WHERE ''' + config.database + '''.`comment`.`user` = %s''', (id,))
        comments = cursor.fetchall()

        ids_posts = [i for i, g in posts]
        ids_comments = [i for i, g in comments]

        genders = [i[1] for i in comments+posts if i[1]]+[gend]

        if len(genders) >= number_of_predictions:
            c = Counter(genders)
            value, count = c.most_common()[0]
            if count/float(len(genders)) >= percent_of_same:
                cursor.execute(
                    '''UPDATE ''' + config.database + '''.`user` ''' \
                    '''SET ''' + config.database + '''.`user`.`gender` = \'''' + value + '''\', ''' + config.database + '''.`user`.`gender_prob` = \'''' + str(gender_sens)+'/'+str(number_of_predictions)+'/'+str(percent_of_same) + '''\' ''' \
                    '''WHERE ''' + config.database + '''.`user`.`id` = \'''' + str(id) + '''\';''')
                cnx.commit()
                for i in ids_posts:
                    cursor.execute(
                        '''UPDATE ''' + config.database + '''.`post` ''' \
                        '''SET ''' + config.database + '''.`post`.`gender` = \'''' + value + '''\' '''\
                        '''WHERE ''' + config.database + '''.`post`.`id` = \'''' + str(i) + '''\';''')
                    cnx.commit()
                for i in ids_comments:
                    cursor.execute(
                        '''UPDATE ''' + config.database + '''.`comment` ''' \
                        '''SET ''' + config.database + '''.`comment`.`gender` = \'''' + value + '''\' ''' \
                        '''WHERE ''' + config.database + '''.`comment`.`id` = \'''' + str(i) + '''\';''')
                    cnx.commit()
    cursor.close()
    cnx.close()


def detect_lsg(text, lang_sens, sent_sens, gender_sens):
    '''
    Language and sentiment detection of given text.
    :param text: text
    :param lang_sens: sensitivity for language detection
    :param sent_sens: sensitiviti for sentiment detection
    :param gender_sens: sensibility for gender determination based on text
    :return: language, sentiment and gender for given text
    '''

    language = text_processing.language_detection(text, sens=lang_sens)
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

    classifier_text_slovene = text_processing.classifier_gender_text_slovene
    classifier_text_english = text_processing.classifier_gender_text_english
    gender_text = ''
    if language == 'sl' or language == 'en':
        textp = text_processing.prepare_text(text, language)
        if language == 'en':
            prob_dist = classifier_text_english.prob_classify(textp)
        if language == 'sl':
            prob_dist = classifier_text_slovene.prob_classify(textp)
        f = prob_dist.prob("female")
        m = prob_dist.prob("male")
        if float(f) >= float(gender_sens):
            gender_text = 'female'
        if float(m) >= float(gender_sens):
            gender_text = 'male'
    return (language, sentiment, gender_text)

def detect_user_gender(name1, name2, gender_sens):
    '''
    Gender determination based on username (consisting of one or two names).
    :param name1: username one
    :param name2: username two
    :param gender_sens: sensibility for gender determination based on username
    :return: gender
    '''
    classifier_name_1 = text_processing.classifier_gender_name_1
    classifier_name_2 = text_processing.classifier_gender_name_2

    prob_dist = ''
    if name1 and name2:
        prob_dist = classifier_name_2.prob_classify(text_processing.gender_name_features_2(name1, name2, classifier_name_1))
    else:
        if name1:
            prob_dist = classifier_name_1.prob_classify(text_processing.gender_name_features_1(name1))
        if name2:
            prob_dist = classifier_name_1.prob_classify(text_processing.gender_name_features_1(name2))

    gender = ''
    if prob_dist:
        f = prob_dist.prob("female")
        m = prob_dist.prob("male")

        if float(f) >= float(gender_sens):
            gender = 'female'
        if float(m) >= float(gender_sens):
            gender = 'male'
    return gender



