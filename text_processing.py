# -*- coding: utf-8 -*-
from langdetect import DetectorFactory
DetectorFactory.seed = 0
import langdetect
import lemmagen
from lemmagen.lemmatizer import Lemmatizer
from nltk.corpus import stopwords
from nltk.corpus import names
import nltk.metrics
import config
import random
import re

# english stop words
stopwords_set_english = set(stopwords.words("english"))
# slovenian stop words
stopwords_set_slovene = [word.replace(' ', '') for word in set(stopwords.words("slovenian"))]
# number of instances for classifier preparation (slovenian sentiment and gender detection)
nr_instances = 30000

def language_detection(text, sens):
    '''
    Detect language of given text with desired sensibility.
    :param text: text
    :param sens: sensibility
    :return: detected language or empty string (if detection can be more reliable than desired sensibility)
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

def preprocess(text):
    '''
    Only for texts in slovenian language: lemmatization of text.
    :param text: text
    :return: lemmatized text
    '''
    lemmatizer = Lemmatizer(dictionary=lemmagen.DICTIONARY_SLOVENE)
    lemmatizedText = [lemmatizer.lemmatize(x.lower()) for x in text]
    return lemmatizedText

def sentiment_prepare_slovene():
    '''
    Classifier preparation for slovenian text sentiment analysis.
    :return: naive bayes classifier for slovenian text
    '''
    data_folder = config.data_folder
    global stopwords_set_slovene
    global nr_instances

    data_pos, data_neg = [], []
    lines = random.sample(open(data_folder+'/tweets_slovene_sentiment.txt').readlines(), nr_instances)
    for line in lines:
        line = line.split('\t')
        if line[0] == 'slv':
            words_filtered = [
                e.lower().rstrip('\'\"-,.:;!?*').replace("\"", "").replace("\[", "").replace("\]", "").replace("*", "")
                for e in re.sub('\.\.+', ' ', line[2]).split() if len(e) >= 2]
            words_cleaned = [word for word in words_filtered
                             if 'http' not in word and not word.startswith('@') and not word.startswith('#') and word != 'rt']
            words_lemmatized = [word.decode('utf-8') for word in preprocess(words_cleaned)]
            words_without_stopwords = {word: True for word in words_lemmatized if not word in stopwords_set_slovene}
            if line[1] == 'positive':
                data_pos.append([words_without_stopwords, 'pos'])
            elif line[1] == 'negative':
                data_neg.append([words_without_stopwords, 'neg'])
            else:
                if ':D' in words_without_stopwords or ':)' in words_without_stopwords:
                    data_pos.append([words_without_stopwords, 'pos'])
                if ':(' in words_without_stopwords:
                    data_pos.append([words_without_stopwords, 'neg'])
    training = data_neg + data_pos
    classifier = nltk.NaiveBayesClassifier.train(training)
    return classifier

def sentiment_prepare_english():
    '''
        Classifier preparation for english text sentiment analysis.
        :return: naive bayes classifier
        '''
    data_folder = config.data_folder
    global stopwords_set_english

    data_pos, data_neg = [], []
    with open(data_folder+"/tweets_english_sentiment.txt") as f:
        for line in f:
            line = line.split('\t')
            words_filtered = [e.lower().rstrip('\'\"-,.:;!?*').replace("#", "").replace("\"", "").replace("*", "") for e in re.sub('\.\.+', ' ', line[2]).split() if len(e) >= 2]
            words_cleaned = [word for word in words_filtered
                             if 'http' not in word and not word.startswith('@') and word != 'rt']
            words_without_stopwords = {word: True for word in words_cleaned if not word in stopwords_set_english}
            if line[1] == 'positive':
                data_pos.append([words_without_stopwords, 'pos'])
            elif line[1] == 'negative':
                data_neg.append([words_without_stopwords, 'neg'])
    training = data_neg + data_pos
    classifier = nltk.NaiveBayesClassifier.train(training)
    return classifier

def prepare_text(text, lang):
    '''
    Text preparation for sentiment analysis and gender detection; text is returned as list of included words without stopwords and special signs.
    :param text: text
    :param lang: language of text
    :return: prepared text for further analysis
    '''
    if lang == 'en':
        global stopwords_set_english
        stopwords_set = stopwords_set_english
        words_filtered = [e.lower().rstrip('\'\"-,.:;!?*').replace("#", "").replace("\"", "").replace("*", "") for e in re.sub('\.\.+', ' ', text).split() if len(e) >= 2]
        words_cleaned = [word for word in words_filtered
                         if 'http' not in word and not word.startswith('@') and not word.startswith('#') and word != 'rt']
        words_without_stopwords = {word: True for word in words_cleaned if not word in stopwords_set}
    else:
        global stopwords_set_slovene
        stopwords_set = stopwords_set_slovene
        words_filtered = [e.lower().rstrip('\'\"-,.:;!?*').replace("\"", "").replace("\[", "").replace("\]", "").replace("*", "") for e in re.sub('\.\.+', ' ', text).split() if len(e) >= 2]
        words_cleaned = [word for word in words_filtered
                         if 'http' not in word and not word.startswith('@') and not word.startswith('#') and word != 'rt']
        words_lemmatized = preprocess(words_cleaned)
        words_without_stopwords = {word: True for word in words_lemmatized if not word in stopwords_set}
    return words_without_stopwords

def gender_name_prepare():
    '''
    Classifier preparation for gender detection based on username: first for one username and second for two usernames (like for example
    users on twitter have username and screen name).
    :return: two naive ayes classifiers
    '''
    data_folder = config.data_folder
    users1 = []
    users2 = []
    fname = data_folder+'/names_gender.txt'
    with open(fname) as f:
        content = f.readlines()
    for u in content:
        line = u.strip().split('\t')
        if len(line) == 2:
            users1.append((line[1], line[0]))
        if len(line) == 3:
            users2.append((line[1], line[2], line[0]))

    users1 = [(name.lower(), 'male') for name in names.words('male.txt')] + [(name.lower(), 'female') for name in names.words('female.txt')] + users1

    # one-name-classification
    featuresets1 = [(gender_name_features_1(n), gender) for (n, gender) in users1]
    classifier1 = nltk.NaiveBayesClassifier.train(featuresets1)
    # two-name-classification
    featuresets2 = [(gender_name_features_2(n1, n2, classifier1), gender) for (n1, n2, gender) in users2]
    classifier2 = nltk.NaiveBayesClassifier.train(featuresets2)
    return (classifier1, classifier2)

def gender_name_features_1(name):
    '''
    Preparation of username for gender detection for one username.
    :param name: username
    :return: username statistics
    '''
    return {'last_letter': name[-1],
            'last_two': name[-2:],
            'last_three': name[-3:],
            'last_is_vowel': (name[-1] in 'aeiouy'),
            'first_letter': name[0],
            'length': len(name)}

def gender_name_features_2(word1, word2, classifier1):
    '''
    Preparation of username for gender detection for two usernames.
    :param word1: first username
    :param word1: second username
    :return: statistics of usernames
    '''
    return {'class1': classifier1.classify(gender_name_features_1(word1)),
            'class2': classifier1.classify(gender_name_features_1(word2)),
            'last_letter1': word1[-1],
            'last_two1': word1[-2:],
            'last_three1': word1[-3:],
            'last_is_vowel1': (word1[-1] in 'aeiouy'),
            'first_letter1': word1[0],
            'length1': len(word1),
            'last_letter2': word2[-1],
            'last_two2': word2[-2:],
            'last_three2': word2[-3:],
            'last_is_vowel2': (word2[-1] in 'aeiouy'),
            'first_letter2': word2[0],
            'length2': len(word2)}

def gender_text_prepare_slovene():
    '''
    Classifier preparation for gender detection of slovenian text.
    :return: naive bayes classifier for slovenian text
    '''
    data_folder = config.data_folder
    global stopwords_set_slovene
    stopwords_set = stopwords_set_slovene
    global nr_instances

    data_male, data_female = [], []
    lines = random.sample(open(data_folder+'/tweet_base_slovene.txt').readlines(), nr_instances)
    for line in lines:
        line = line.split('\t')
        if line[0] == 'slv':
            gender = line[3]
            text = line[4]
            words_filtered = [
                e.lower().rstrip('\'\"-,.:;!?*').replace("\"", "").replace("\/", "").replace("\[", "").replace("\]", "").replace(
                    "*", "").replace("\n", '') for e in re.sub('\.\.+', ' ', text).split() if len(e) >= 2]
            words_cleaned = [word for word in words_filtered
                             if 'http' not in word and not word.startswith('@') and not word.startswith('#') and word != 'rt']
            words_lemmatized = [word.decode('utf-8') for word in preprocess(words_cleaned)]
            words_without_stopwords = {word: True for word in words_lemmatized if not word in stopwords_set}
            if gender == 'male':
                data_male.append([words_without_stopwords, gender])
            if gender == 'female':
                data_female.append([words_without_stopwords, gender])

    training = data_male + data_female
    classifier = nltk.NaiveBayesClassifier.train(training)
    return classifier

def gender_text_prepare_english():
    '''
    Classifier preparation for gender detection of english text.
    :return: naive bayes classifier for english text
    '''
    data_folder = config.data_folder
    global stopwords_set_english
    stopwords_set = stopwords_set_english
    texts = []
    with open(data_folder+'/tweets_english_gender.txt') as filename:
        for line in filename:
            line = line.split('\t')
            text = line[1]
            if len(text) > 2:
                texts.append((line[0], text))

    data_female, data_male = [], []
    for g, t in texts:
        words_filtered = [e.lower().rstrip('\'\"-,.:;!?*').replace("#", "") for e in re.sub('\.\.+', ' ', t).split() if
                          len(e) >= 2]
        words_cleaned = [word for word in words_filtered if 'http' not in word and not word.startswith('@') and word != 'rt']
        words_without_stopwords = {word: True for word in words_cleaned if not word in stopwords_set}
        if g == 'male':
            data_male.append([words_without_stopwords, 'male'])
        else:
            data_female.append([words_without_stopwords, 'female'])

    training = data_male + data_female
    classifier = nltk.NaiveBayesClassifier.train(training)
    return classifier

# classifiers for sentiment analysis and gender detection
classifier_slovene = sentiment_prepare_slovene()
classifier_english = sentiment_prepare_english()
classifier_gender_name_1, classifier_gender_name_2 = gender_name_prepare()
classifier_gender_text_slovene = gender_text_prepare_slovene()
classifier_gender_text_english = gender_text_prepare_english()


