#!/usr/bin/env python

from boto.mturk.connection import MTurkConnection
import couchdb
from datetime import datetime, timedelta
from dateutil.parser import parse as date_parse
import logging
from optparse import OptionParser
import os
import sys
import xml.dom.minidom as minidom

from TwitterHealthMental.common.couch_help import *
from TwitterHealthMental.common.logging_help import *
from questions import *

if __name__ == "__main__":
    """ logging stuff """
    init_logging(__file__)

    environment = None
    host = None
    title = "Fill out some quick surveys about these tweets."
    description = "Please rate these tweets on dimensions of depression and anxiety."
    max_assn = 5
    lifetime, duration = None, None
    reward = 0.05
    keywords = "twitter, tweet, tweets, mood, depression, anxiety, survey, health"

    """ OptionParser stuff """
    parser = OptionParser()
    parser.add_option("-e", "--env", dest = "environment", help = "environment to run on (development or production)")
    parser.add_option("-t", "--title", dest = "title", help = "title for these HITs")
    parser.add_option("-d", "--description", dest = "description", help = "description for these HITs")
    parser.add_option("-m", "--max_assn", dest = "max_assn", help = "max number of assignments for these HITs")
    parser.add_option("-r", "--reward", dest = "reward", help = "reward for these HITs")
    parser.add_option("-k", "--keywords", dest = "keywords", help = "list of keywords for these HITs")
    parser.add_option("-l", "--lifetime", dest = "lifetime", help = "time string for HIT lifetimes. (format: <days>:<hours>:<minutes>:<seconds>)")
    parser.add_option("-u", "--duration", dest = "lifetime", help = "time string for HIT durations. (format: <days>:<hours>:<minutes>:<seconds>)")
    parser.set_defaults(environment = "dev", lifetime = "0:0:30:0", duration = "0:0:10:0")
    (options, args) = parser.parse_args()

    if options.environment:
        if options.environment in ["d", "dev", "development"]:
            environment = "development"
            host = "mechanicalturk.sandbox.amazonaws.com"
        elif options.environment in ["p", "prod", "production"]:
            environment = "production"
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
    if options.lifetime:
        (days, hours, minutes, seconds) = options.lifetime.split(":")
        lifetime = timedelta(days = int(days), hours = int(hours), minutes = int(minutes), seconds = int(seconds))
    if options.duration:
        (days, hours, minutes, seconds) = options.duration.split(":")
        duration = timedelta(days = int(days), hours = int(hours), minutes = int(minutes), seconds = int(seconds))

    """ couch db stuff """
    couch = couchdb.Server('http://dev.fount.in:5984')
    couch.resource.credentials = ('admin', 'admin')
    db = openOrCreateDb(couch, 'mturk_tweets')
    db_hits = openOrCreateDb(couch, 'mturk_hits')
    results = db.view("Tweet/turkable", include_docs = True)

    """ aws mechanical turk stuff """
    connection = MTurkConnection(host = host)

    """ grab a bunch of docs and go """
    step = 5
    results_list = [result for result in results]
    for i in range(0, len(results_list), step):
        tweets, tweet_ids = [], set()
        start, finish = i, min(i + step, len(results_list))
        for tweet in results_list[start:finish]:
            if tweet.doc["_id"] in tweet_ids:
                continue
            tweet_ids |= set([tweet.doc["_id"]])
            tweets += [tweet.doc]
            logging.debug("\tSending off tweet as HIT")
            logging.debug("\t\t%s: %s" % (tweet.doc["from_user"], tweet.doc["text"]))
        logging.debug("%s" % tweets)
        questions = TweetEvaluationQuestion(tweets)
        results = connection.create_hit(
            questions = questions.form,
            max_assignments = max_assn,
            title = title,
            description = description,
            keywords = keywords,
            lifetime = lifetime,
            duration = duration,
            reward = reward)
        hit = results[0]
        if hit.IsValid:
            # save HIT and its status to separate db
            tweet_hit = {
                "_id": hit.HITId,
                "created_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S +0000"),
                "environment": environment,
                "type": hit.HITTypeId,
                "tweets": [tweet["id_str"] for tweet in tweets],
                "content": { tweet["id_str"]: tweet["text"] for tweet in tweets },
                "status": "Assignable", # NOTE: this might not be the right status to use
                # TODO: step your game up
                "responses": [
                    # { "assignment": ..., "worker": ..., "tweet": ..., "response": [("anxiety"|"depression"), ... ] }
                ]
            }
            saveObjectToCouch(db_hits, tweet_hit)
        else:
            # bomb out on this HIT
            # TODO: change the range notated when we do this for real
            logging.error("HIT %s [%d, %d] was not valid, continuing..." % (hit.HITId, i, i + 5))
