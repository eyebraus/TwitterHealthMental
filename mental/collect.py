#!/usr/bin/env python

import couchdb
from datetime import datetime, timedelta
import logging
import os
import re
import sys
from twython import Twython

def openOrCreateDb(server, name):
    try:
        db = server.create(name) # newly created
        return db
    except couchdb.http.PreconditionFailed:
        db = server[name]
        return db

def saveObjectToCouch(db, o):
    o['_id'] = o['id_str']
    try:
        db.save(o, batch='ok')
        return True
    except couchdb.http.ResourceConflict:
        logging.exception('Object with _id %s already in db; continuing...' % o['_id'])
        return False

""" start loggin' """
basename = os.path.basename(__file__).split('.')[0]
logging.basicConfig(filename = '%s.log' % basename, level = logging.DEBUG, filemode = 'w', format='%(message)s')

""" initialize CouchDB stuff """
couch = couchdb.Server('http://dev.fount.in:5984')
couch.resource.credentials = ('admin', 'admin')
db = openOrCreateDb(couch, 'mturk_trial')

""" initialize Twython with Sean Buckets credentials """
consumer_key = 'dBed6SmjIPIGcdnkMI03nw'
consumer_secret = 'ATICxmYw5dC9TBLgqg2s0QFeMagfwJALKDUTRoZN94'
access_token = '981919850-pfAwbpx5gy6Ath4mQMLCldUywMN5PrPLwTABiMMe'
access_secret = 'AItuj14hggRp7mTQ7slt1gps7lumRshi9LCF18TnII'
twitter = Twython(twitter_token = consumer_key,
    twitter_secret = consumer_secret,
    oauth_token = access_token,
    oauth_token_secret = access_secret)

if __name__ == "__main__":
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
        saveObjectToCouch(db, tweet)
        logging.debug("%s" % (tweet['text']))
        numTweets += 1
