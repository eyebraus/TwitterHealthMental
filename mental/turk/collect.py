#!/usr/bin/env python

""" collect.py
    ----------
    collect tweets
"""

import couchdb
from datetime import datetime, timedelta
import logging
from optparse import OptionParser
import os
import re
import sys
from twython import Twython

from TwitterHealthMental.common.couch_help import *
from TwitterHealthMental.common.logging_help import *

if __name__ == "__main__":
    """ start loggin' """
    init_logging(__file__)

    environment = None

    """ OptionParser stuff """
    parser = OptionParser()
    parser.add_option("-e", "--env", dest = "environment", help = "environment to run on (development or production)")
    parser.set_defaults(environment = "dev")
    (options, args) = parser.parse_args()
    if options.environment:
        if options.environment in ["d", "dev", "development"]:
            environment = "development"
        elif options.environment in ["p", "prod", "production"]:
            environment = "production"
        else:
            logging.error("No such environment \"%s\"" % options.environment)
            sys.exit(-1)

    """ initialize CouchDB stuff """
    couch = couchdb.Server('http://dev.fount.in:5984')
    couch.resource.credentials = ('admin', 'admin')
    db = openOrCreateDb(couch, 'mturk_tweets')

    """ initialize Twython with Sean Buckets credentials """
    consumer_key = 'dBed6SmjIPIGcdnkMI03nw'
    consumer_secret = 'ATICxmYw5dC9TBLgqg2s0QFeMagfwJALKDUTRoZN94'
    access_token = '981919850-pfAwbpx5gy6Ath4mQMLCldUywMN5PrPLwTABiMMe'
    access_secret = 'AItuj14hggRp7mTQ7slt1gps7lumRshi9LCF18TnII'
    twitter = Twython(twitter_token = consumer_key,
        twitter_secret = consumer_secret,
        oauth_token = access_token,
        oauth_token_secret = access_secret)

    # just load like 50 tweets, "whatever man" - rintaro kuroiwa
    search_results = None
    numSuccessfulQueries, numTweets = 0, 0

    # perform a simple keyword search
    try:
        search_results = twitter.searchTwitter(q = "sad", rpp = "50", lang = "en", result_type = "recent")
    except:
        logging.debug("What. I couldn't Twython that.")
        sys.exit(-1)

    # print and save resultssssssssss
    if search_results.has_key('error'):
        e = search_results['error']
        logging.debug("Something bad happened compadre.")
        logging.debug("\t" + search_results['error'])
        sys.exit(-1)
    elif not search_results.has_key('results'):
        logging.debug("No results key was found.")
        sys.exit(-1)
    else:
        numSuccessfulQueries += 1

    for tweet in search_results['results']:
        # Remove newlines
        tweet['text'] = re.sub(re.compile('[\r\n]+'), ' ', tweet['text']).encode("utf-8")
        # add new fields
        tweet['environment'] = environment
        tweet['depression'] = None
        tweet['anxiety'] = None
        saveObjectToCouch(db, tweet)
        logging.debug("%s" % (tweet['text']))
        numTweets += 1
