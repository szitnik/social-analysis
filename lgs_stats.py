# -*- coding: UTF-8 -*-
from __future__ import print_function
import mysql.connector
from langdetect import DetectorFactory
DetectorFactory.seed = 0
import config
from pprint import pprint

media = range(1,6)
keywords = ['odprta kuhna', 'drugi tir', 'alples', 'jan plestenjak', 'miro cerar', 'anze kopitar', 'oktoberfest',
            'motorola', 'arnold schwarzenegger', 'usain bolt', 'tour de france', 'pokemon go']
keywords = ['adidas', 'cristiano ronaldo', 'melania trump', 'rihanna', 'hillary clinton', 'brexit']
cnx = mysql.connector.connect(user=config.user, password=config.password, host=config.host, database=config.database)
cursor = cnx.cursor()

for k in keywords:
    print(k)
    for m in media:
        print(m)

        cursor.execute('''SELECT count(*) FROM ''' + config.database + '''.post ''' \
                        '''WHERE ''' + config.database + '''.`post`.`soc_media` IN (%s) AND ''' + config.database + '''.`post`.`keyword` = %s ''' \
                        '''AND ''' + config.database + '''.`post`.`creation_time` >= '2017-11-14' AND ''' + config.database + '''.`post`.`creation_time` < '2018-01-01';''',
                       (m, k,))
        all = cursor.fetchall()[0][0]

        # LANGUAGE
        cursor.execute('''SELECT count(*) FROM ''' + config.database + '''.post ''' \
                        '''WHERE ''' + config.database + '''.`post`.`soc_media` IN (%s) AND ''' + config.database + '''.`post`.`keyword` = %s ''' \
                        '''AND length(''' + config.database + '''.`post`.`language`)>1 ''' \
                        '''AND ''' + config.database + '''.`post`.`creation_time` >= '2017-11-14' AND ''' + config.database + '''.`post`.`creation_time` < '2018-01-01';''',
                       (m, k, ))
        nr_lang = cursor.fetchall()[0][0]

        cursor.execute('''SELECT language, count(*) FROM ''' + config.database + '''.post ''' \
                       '''WHERE ''' + config.database + '''.`post`.`soc_media` IN (%s) AND ''' + config.database + '''.`post`.`keyword` = %s ''' \
                       '''AND length(''' + config.database + '''.`post`.`language`)>1 ''' \
                       '''AND ''' + config.database + '''.`post`.`creation_time` >= '2017-11-14' AND ''' + config.database + '''.`post`.`creation_time` < '2018-01-01' ''' \
                       '''GROUP BY language;''',
                       (m, k,))
        count_lang = cursor.fetchall()

        # SENTIMENT
        cursor.execute('''SELECT count(*) FROM ''' + config.database + '''.post ''' \
                        '''WHERE ''' + config.database + '''.`post`.`soc_media` IN (%s) AND ''' + config.database + '''.`post`.`keyword` = %s ''' \
                        '''AND length(''' + config.database + '''.`post`.`sentiment`)>1 ''' \
                        '''AND ''' + config.database + '''.`post`.`creation_time` >= '2017-11-14' AND ''' + config.database + '''.`post`.`creation_time` < '2018-01-01';''',
                       (m, k, ))
        nr_sent = cursor.fetchall()[0][0]

        cursor.execute('''SELECT sentiment, count(*) FROM ''' + config.database + '''.post ''' \
                       '''WHERE ''' + config.database + '''.`post`.`soc_media` IN (%s) AND ''' + config.database + '''.`post`.`keyword` = %s ''' \
                       '''AND length(''' + config.database + '''.`post`.`sentiment`)>1 ''' \
                       '''AND ''' + config.database + '''.`post`.`creation_time` >= '2017-11-14' AND ''' + config.database + '''.`post`.`creation_time` < '2018-01-01' ''' \
                       '''GROUP BY sentiment;''',
                       (m, k,))
        count_sent = cursor.fetchall()

        # GENDER
        cursor.execute('''SELECT count(*) FROM ''' + config.database + '''.post ''' \
                        '''WHERE ''' + config.database + '''.`post`.`soc_media` IN (%s) AND ''' + config.database + '''.`post`.`keyword` = %s ''' \
                        '''AND length(''' + config.database + '''.`post`.`gender`)>1 ''' \
                        '''AND ''' + config.database + '''.`post`.`creation_time` >= '2017-11-14' AND ''' + config.database + '''.`post`.`creation_time` < '2018-01-01';''',
                       (m, k, ))
        nr_gend = cursor.fetchall()[0][0]

        cursor.execute('''SELECT gender, count(*) FROM ''' + config.database + '''.post ''' \
                       '''WHERE ''' + config.database + '''.`post`.`soc_media` IN (%s) AND ''' + config.database + '''.`post`.`keyword` = %s ''' \
                       '''AND length(''' + config.database + '''.`post`.`gender`)>1 ''' \
                       '''AND ''' + config.database + '''.`post`.`creation_time` >= '2017-11-14' AND ''' + config.database + '''.`post`.`creation_time` < '2018-01-01' ''' \
                       '''GROUP BY gender;''',
                       (m, k,))
        count_gend = cursor.fetchall()

        # GENDER - USER
        cursor.execute('''SELECT count(*) FROM ''' + config.database + '''.user ''' \
                        '''WHERE ''' + config.database + '''.`user`.`soc_media` IN (%s) AND ''' + config.database + '''.`user`.`keyword` = %s ''',
                       (m, k, ))
        all_user = cursor.fetchall()[0][0]

        cursor.execute('''SELECT count(*) FROM ''' + config.database + '''.user ''' \
                        '''WHERE ''' + config.database + '''.`user`.`soc_media` IN (%s) AND ''' + config.database + '''.`user`.`keyword` = %s ''' \
                        '''AND length(''' + config.database + '''.`user`.`gender`)>1 ''',
                       (m, k, ))
        nr_gend_user = cursor.fetchall()[0][0]

        cursor.execute('''SELECT gender, count(*) FROM ''' + config.database + '''.user ''' \
                       '''WHERE ''' + config.database + '''.`user`.`soc_media` IN (%s) AND ''' + config.database + '''.`user`.`keyword` = %s ''' \
                       '''AND length(''' + config.database + '''.`user`.`gender`)>1 ''' \
                       '''GROUP BY gender;''',
                       (m, k,))
        count_gend_user = cursor.fetchall()

        if all > 0:
            print('Vseh postov: '+str(all))
            print('Posti z določenim jezikom: '+str(nr_lang)+' ('+str(round(float(nr_lang)*100/all, 3))+')')
            print('Jeziki postov: '+str(count_lang))
            print('Posti z določenim sentimentom: ' + str(nr_sent) + ' (' + str(round(float(nr_sent) * 100 / all, 3)) + ')')
            print('Sentimenti postov: '+str(count_sent))
            print('Posti z določenim spolom: '+str(nr_gend)+' ('+str(round(float(nr_gend)*100/all, 3))+')')
            print('Spoli postov: '+str(count_gend))
            print('Vseh userjev: '+str(all_user))
            print('Userji z določenim spolom: '+str(nr_gend_user)+' ('+str(round(float(nr_gend_user)*100/all_user, 3))+')')
            print('Spoli userjev: '+str(count_gend_user))

import re

def glk(tr):
    r = re.split(' \[|\]', tr)
    for s in r[0].split():
        if s.isdigit():
            print(s)
    t = dict(eval( "[%s]" % r[1] ))
    print(t['de']) if t.has_key('de') else 0
    print(t['en']) if t.has_key('en') else 0
    print(t['es']) if t.has_key('es') else 0
    print(t['hr']) if t.has_key('hr') else 0
    print(t['fr']) if t.has_key('fr') else 0
    print(t['id']) if t.has_key('id') else 0
    print(t['it']) if t.has_key('it') else 0
    print(t['nl']) if t.has_key('nl') else 0
    print(t['sl']) if t.has_key('sl') else 0
    print(t['pt']) if t.has_key('pt') else 0
    for s in r[2].split():
        if s.isdigit():
            print(s)
    t1 = dict(eval( "[%s]" % r[3] ))
    print(t1['neg'])
    print(t1['pos'])
    for s in r[4].split():
        if s.isdigit():
            print(s)
    t2 = dict(eval( "[%s]" % r[5] ))
    print(t2['female'])
    print(t2['male'])


glk("Vseh postov: 14 Posti z določenim jezikom: 13 (92.857) Jeziki postov: [(u'en', 5), (u'sl', 8)] Posti z določenim sentimentom: 13 (92.857) Sentimenti postov: [(u'neg', 9), (u'pos', 4)] Posti z določenim spolom: 13 (92.857) Spoli postov: [(u'female', 6), (u'male', 7)]")

