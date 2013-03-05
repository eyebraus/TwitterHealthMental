#!/usr/bin/env python

from boto.mturk.connection import MTurkConnection
import couchdb
from datetime import datetime, timedelta
import logging
from optparse import OptionParser
import os
from io import BytesIO
import sys
from questions import *
import xml.dom.minidom as minidom

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

if __name__ == "__main__":
    host = None
    title = "Fill out some quick surveys about these tweets."
    description = "Please rate these tweets on dimensions of depression and anxiety."
    max_assn = 5
    duration = timedelta(days = 7)
    reward = 0.05
    keywords = "twitter, tweet, tweets, mood, depression, anxiety, survey, health"

    """ logging stuff """
    basename = os.path.basename(__file__).split('.')[0]
    logging.basicConfig(filename = '%s.log' % basename, level = logging.DEBUG, filemode = 'w', format='%(message)s')

    """ OptionParser stuff """
    parser = OptionParser()
    parser.add_option("-e", "--env", dest = "environment", help = "environment to run on (development or production)")
    parser.add_option("-t", "--title", dest = "title", help = "title for these HITs")
    parser.add_option("-d", "--description", dest = "description", help = "description for these HITs")
    parser.add_option("-m", "--max_assn", dest = "max_assn", help = "max number of assignments for these HITs")
    parser.add_option("-r", "--reward", dest = "reward", help = "reward for these HITs")
    parser.add_option("-k", "--keywords", dest = "keywords", help = "list of keywords for these HITs")
    parser.set_defaults(environment = "dev")
    (options, args) = parser.parse_args()

    if options.environment:
        if options.environment in ["d", "dev", "development"]:
            host = "mechanicalturk.sandbox.amazonaws.com"
        elif options.environment in ["p", "prod", "production"]:
            host = "mechanicalturk.amazonaws.com"
        else:
            logging.error("No such environment \"%s\"" % options.environment)
            sys.exit(-1)
    if options.title:
        title = options.title
    if options.description:
        description = options.description
    if options.max_assn:
        max_assn = options.max_assn
    if options.reward:
        reward = float(options.reward)
    if options.keywords:
        keywords = options.keywords

    """ couch db stuff """
    couch = couchdb.Server('http://dev.fount.in:5984')
    couch.resource.credentials = ('admin', 'admin')
    db = openOrCreateDb(couch, 'mturk_trial')
    results = db.view("Tweet/all", include_docs = True, limit = 5)

    """ aws mechanical turk stuff """
    connection = MTurkConnection(host = host)

    """ grab a bunch of docs and go """
    for i in range(0, 5, 5):
        tweets, c = [], 0
        for tweet in results:
            if c < 1:
                c += 1
                continue
            if c >= 5:
                break
            tweets += [tweet.doc]
            logging.debug("\tSending off tweet as HIT")
            logging.debug("\t\t%s: %s" % (tweet.doc["from_user"], tweet.doc["text"]))
            c += 1
        questions = TweetEvaluationQuestion(tweets)
        pretty = ''.join([x for x in questions.form.get_as_xml() if ord(x) < 128])
        pretty = '<?xml version="1.0" encoding="UTF-8"?>\n%s' % pretty
        # strip all unicode chars
        logging.debug("%s" % minidom.parseString(pretty).toprettyxml())
        result = connection.create_hit(
            questions = questions.form,
            max_assignments = max_assn,
            title = title,
            description = description,
            keywords = keywords,
            duration = duration,
            reward = reward)
