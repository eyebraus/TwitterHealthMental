#!/usr/bin/env python

from boto.mturk.connection import MTurkConnection
import couchdb
from datetime import datetime, timedelta
import logging
from math import ceil
from optparse import OptionParser
import os
import sys
import time

from TwitterHealthMental.common.couch_help import *
from TwitterHealthMental.common.logging_help import *

# credit to Mauro Rocco for this code
# http://www.toforge.com/2011/05/boto-mturk-tutorial-fetch-results-and-pay-workers/
def get_reviewable_hits(connection, page_size):
    total_pages = None
    hits = connection.get_reviewable_hits(page_size = page_size)
    total_pages = int(ceil(float(hits.TotalNumResults) / page_size))
    if total_pages == 0:
        logging.debug("\t%d/%d, %d" % (0, total_pages, len(hits)))
        logging.debug("\tNo HITs found up for review.")
        return []
    logging.debug("\tPage %d/%d: %d results" % (1, total_pages, len(hits)))
    for page in range(2, total_pages):
        new_hits = connection.get_reviewable_hits(page_size = page_size, page_number = page)
        logging.debug("\tPage %d/%d: %d results" % (page, total_pages, len(new_hits)))
        hits.extend(new_hits)
    return hits

def process_hit(hit):
    # get couch doc, update HIT status
    couch_hit = db_hits.get(hit.HITId)
    #couch_hit["Status"] = hit.HITStatus
    # NOTE: does get_assignments only get UNSEEN assignments?
    # docs are not clear; for now, I assume not.
    assignments = connection.get_assignments(hit.HITId)
    submitted_assignments = [assn for assn in assignments if assn.AssignmentStatus == "Submitted" ]
    logging.debug("\tAssignments submitted: %d" % submitted_assignments)
    for assignment in submitted_assignments:
        logging.debug("\t\tAssignment: { assign_id: %s, worker_id: %s }" % (assignment.AssignmentId, assignment.WorkerId))
        for question_form_answer in assignment.answers[0]:
            for key, value in question_form_answer.fields:
                # figure out which tweet it was based on the question id
                tweet_index, label_type = key.split("_")[1], key.split("_")[2]
                content = couch_hit["contents"][couch_hit["tweets"][tweet_index]]
                logging.debug("\t\t\t%s: %s" % (content, value))
                couch_hit["responses"] += [{
                    "assignment": assignment.AssignmentId,
                    "worker": assignment.WorkerId,
                    "tweet": couch_hit["tweets"][tweet_index],
                    "response": [label_type, value]
                }]
    return couch_hit

if __name__ == "__main__":
    """ init logging """
    init_logging(__file__)

    host, tick = None, None
    page_size = 50

    """ OptionParser stuff """
    parser = OptionParser()
    parser.add_option("-e", "--env", dest = "environment", help = "environment to run on (development or production)")
    parser.add_option("-t", "--tick", dest = "tick", help = "number of seconds between polls to mturk")
    parser.set_defaults(environment = "dev", tick = 300)
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
    if options.tick:
        try:
            tick = int(options.tick)
        except ValueError:
            logging.error("Argument tick = %s is not an integer" % options.tick)

    """ init couchdb stuff """
    couch = couchdb.Server('http://dev.fount.in:5984')
    couch.resource.credentials = ('admin', 'admin')
    db_hits = openOrCreateDb(couch, 'mturk_hits')

    """ init mturk stuff """
    connection = MTurkConnection(host = host)

    """ periodically poll the HITs """
    while True:
        try:
            wake_time = datetime.utcnow()
            hits = get_reviewable_hits(connection, page_size)
            logging.debug("%d hits found in total." % len(hits))
            for hit in hits:
                logging.debug("\tHIT: { hit_id: %s }" % hit.HITId)
                # cuz I fucking guarantee it won't actually return shit it's supposed to...
                logging.debug("\tHIT fields: %s" % dir(hit))
                while True:
                    couch_hit = process_hit(hit)
                    logging.debug("\tProcessed HIT, trying to save. { _id: %s, _rev: %s }" % (couch_hit["_id"], couch_hit["_rev"]))
                    # write back changes to couch, or try again
                    # (another script could have stepped on our toes here)
                    if saveObjectToCouch(db_hits, couch_hit):
                        logging.debug("\tSave successful!")
                        break
                    else:
                        logging.debug("\tSave unsuccessful :(")
            # sleep
            passed = datetime.utcnow() - wake_time
            if passed.seconds < tick:
                sleep = tick - passed.seconds
                logging.debug("Sleeping for %d seconds..." % sleep)
                time.sleep(sleep)
                logging.debug("Waking up; getting reviewable hits...")
        except Exception as e:
            logging.error("Something terrible happened!!!")
            logging.error("\ttype: %s" % type(e))
            logging.error("\t%s" % e)
            sys.exit(-1)
