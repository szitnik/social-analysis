import methods
import search_user
import search
import search_location

def srch_user(query, soc_media):
    '''
    Get users from facebook, twitter or google+ according to specific query.
    :param query: search keyword
    :param soc_media: social media to search
    '''
    tuples = {'facebook': (methods.facebook_search_user, search_user.facebook_mysql),
                'twitter': (methods.twitter_search_user, search_user.twitter_mysql),
                'googleplus': (methods.googleplus_search_user, search_user.googleplus_mysql)}
    for m in soc_media:
        func = tuples.get(m)[0]
        users = func(query)
        func_print = tuples.get(m)[1]
        func_print(users, query)

def srch(query, soc_media, date_from):
    '''
    Get posts and other hits on different social media according to specific query.
    :param query: search keyword
    :param date_from: date to search from in format 'Y-M-D'
    :param soc_media: social media to search
    '''
    tuples = {'facebook': (methods.facebook_search, search.facebook_mysql),
                'tumblr': (methods.tumblr_search_after, search.tumblr_mysql),
                'googleplus': (methods.googleplus_search, search.googleplus_mysql),
                'youtube': (methods.youtube_search_date_after, search.youtube_mysql)}
    for m in soc_media:
        print(m+' '+query)
        if m == 'twitter':
            search.twt_after_date(query, date_from)
        else:
            func = tuples.get(m)[0]
            if m == 'facebook': #or m == 'twitter' #or m == 'googleplus':
                results = func(query)
            else:
                results = func(query, date=date_from)
            func_print = tuples.get(m)[1]
            if m == 'facebook':
                func_print(results, query, date=date_from)
            else:
                func_print(results, query)

def srch_location(query, location, distance, soc_media):
    '''
    Get posts and other hits on different social media according to specific query on specific location.
    :param query: search keyword
    :param location: the latitude and longitude of the location center in format 'xx.xx,yyy.yy'
    :param distance: the radius around location center in kilometres, default is 100
    :param soc_media: social media to search
    '''
    tuples = {'facebook': (methods.facebook_search_location, search_location.facebook_mysql),
                'twitter': (methods.twitter_search_on_location, search_location.twitter_mysql),
                'youtube': (methods.youtube_search_video_location, search_location.youtube_mysql),
                'foursquare': (methods.foursquare_search_location, search_location.foursquare_mysql),
                'tripadvisor': (methods.tripadvisor_search_location, search_location.tripadvisor_mysql)}
    for m in soc_media:
        func = tuples.get(m)[0]
        if m in ['facebook', 'twitter', 'youtube']:
            results = func(query, location, distance)
        elif m == 'tripadvisor':
            results = func(query)
        else:
            results = func(query, location)

        func_print = tuples.get(m)[1]
        func_print(results, query)