#!/usr/bin/env python

""" collect.py
    ----------
    collect tweets
"""

import couchdb
import cPickle as pickle
from datetime import datetime, timedelta
import logging
from math import ceil, floor
from optparse import OptionParser
import os
import re
import sys
import time
from twython import Twython

from TwitterHealthMental.common.couch_help import *
from TwitterHealthMental.common.logging_help import *

if __name__ == "__main__":
    """ start loggin' """
    init_logging(__file__)

    corpi, environment, query_complexity, pause = None, None, None, None
    keywords, kstart, kfinish = set([]), None, None
    corpus_reverse = {}
    vitals = None

    """ OptionParser stuff """
    parser = OptionParser()
    parser.add_option("-e", "--env", dest = "environment", help = "environment to run on (development or production)")
    parser.add_option("-c", "--corpi", dest = "corpi", help = "corpi to collect over")
    parser.add_option("-q", "--query", dest = "query", help = "number of keywords to query over at once (I think this affects rate limits)")
    parser.add_option("-p", "--pause", dest = "pause", help = "time to pause between queries")
    parser.set_defaults(environment = "dev", corpi = "d_pos d_neg a_pos a_neg unr", query = 10, pause = 60)
    (options, args) = parser.parse_args()

    if options.environment:
        if options.environment in ["d", "dev", "development"]:
            environment = "development"
        elif options.environment in ["p", "prod", "production"]:
            environment = "production"
        else:
            logging.error("No such environment \"%s\"" % options.environment)
            sys.exit(-1)
    if options.corpi:
        corpi = set(options.corpi.split())
    if options.query:
        query_complexity = options.query
        kstart, kfinish = 0, query_complexity
    if options.pause:
        pause = options.pause

    """ initialize CouchDB stuff """
    couch = couchdb.Server('http://dev.fount.in:5984')
    couch.resource.credentials = ('admin', 'admin')
    db = openOrCreateDb(couch, 'mturk_tweets')
    db_status = openOrCreateDb(couch, 'demon_status')
    vitals = loadVitals(db_status, __file__)

    """ build set of keywords to query over """
    for corpus in corpi:
        corpus_path = "%s/TwitterHealthMental/mental/data/%s.pickle" % (os.environ["TH_PATH"], corpus)
        with open(corpus_path, 'rb') as pickle_file:
            keywords_loaded = pickle.load(pickle_file)
            keywords |= keywords_loaded
            for keyword in keywords_loaded:
                if keyword not in corpus_reverse:
                    corpus_reverse[keyword] = set([])
                corpus_reverse[keyword] |= set([corpus])
    keywords = list(keywords)
    logging.debug("All keywords...\n\t%s" % keywords)

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

    # periodically query over the keyword search
    while True:
        kstart, kfinish = kstart % len(keywords), kfinish % len(keywords)
        if kstart > kfinish:
            kfinish = len(keywords)
        keywords_slice = keywords[kstart:kfinish]
        query_string = " OR ".join(keywords_slice)
        query_time = None
        logging.debug("\tQuery over string \"%s\"" % query_string)

        try:
            search_results = twitter.searchTwitter(q = query_string, rpp = "100", lang = "en", result_type = "recent")
            query_time = datetime.utcnow()
        except:
            logging.error("What. I couldn't Twython that.")
            sys.exit(-1)

        # print and save resultssssssssss
        if search_results.has_key('error'):
            e = search_results['error']
            logging.error("Something bad happened compadre.")
            logging.error("\t%s" % search_results['error'])
            sys.exit(-1)
        elif not search_results.has_key('results'):
            logging.error("No results key was found.")
            logging.error("\tsearch results: %s" % search_results)
            sys.exit(-1)
        else:
            numSuccessfulQueries += 1

        for tweet in search_results['results']:
            # Remove newlines
            tweet['_id'] = tweet['id_str']
            tweet['text'] = re.sub(re.compile('[\r\n]+'), ' ', tweet['text']).encode("utf-8")
            # add new fields
            tweet['environment'] = environment
            tweet['depression'] = None
            tweet['anxiety'] = None
            tweet['corpus'] = []
            for keyword in keywords_slice:
                if keyword in tweet['text']:
                    tweet['corpus'] += list(corpus_reverse[keyword])
            logging.debug("\tTweet %s: %s" % (tweet["_id"], tweet["text"].decode("ascii", "ignore")))
            logging.debug("\t\tCorpi: %s" % ", ".join(tweet["corpus"]))
            saveObjectToCouch(db, tweet)
            numTweets += 1

        # write vitals and sleep for alloted time
        vitals_copy = {
            "db_name": "mturk_tweets",
            "keywords": keywords_slice,
            "slice": [kstart, kfinish],
            "query_string": query_string,
            "numSuccessfulQueries": numSuccessfulQueries,
            "numTweets": numTweets,
            "last_update": str(datetime.utcnow())
        }
        if "_rev" in vitals:
            vitals_copy["_rev"] = vitals["_rev"]
        _, vitals["_rev"] = updateVitals(db_status, __file__, vitals_copy)
        passed = datetime.utcnow() - query_time
        if passed.seconds < pause:
            sleep = ceil(pause - passed.seconds)
            logging.debug("Sleeping for %d seconds..." % sleep)
            time.sleep(sleep)
            logging.debug("Waking up; getting reviewable hits...")
        kstart, kfinish = kstart + query_complexity, kfinish + query_complexity
