#!/usr/bin/env python

"""
    generate_keywords.py
    --------------------
    utility script for building keywords files from LIWC word stat files.

        python generate_keywords.py [-c <corpus>] [-s <liwc_stat_file>] [-d <dest_file>]
"""

import cPickle as pickle
from datetime import datetime, timedelta
import logging
from optparse import OptionParser
import os
import re
import sys

from TwitterHealthMental.common.logging_help import *

corpi = {
    "d_pos": ["NEGATIVE EMOTION", "SADNESS", "EXCLUSION"],
    "d_neg": ["POSITIVE EMOTION", "INCLUSION"],
    "a_pos": ["ANXIETY", "WORK"],
    "a_neg": ["LEISURE"],
    "unr":   []
}
valid_corpi = set(["d_pos", "depression_positive", "d_neg", "depression_negative",
    "a_pos", "anxiety_positive", "a_neg", "anxiety_negative", "unr", "unrelated"])
words = {
    "d_pos": set([]),
    "d_neg": set([]),
    "a_pos": set([]),
    "a_neg": set([]),
    "unr":   set([])
}

if __name__ == "__main__":
    """ init logging """
    init_logging(__file__)

    corpus, source, dest = None, None, None
    toplevel_re = r'^([A-Z]+)( [A-Z]+)*'
    sublevel_re = r'^\t([A-Z]+)( [A-Z]+)*'
    keyword_re = r'^\t\t([A-Z]+)\*? \(\d+\)'

    """ OptionParser stuff """
    parser = OptionParser()
    parser.add_option("-c", "--corpus", dest = "corpus", help = "name of the corpus keyword file to generate")
    parser.add_option("-s", "--source", dest = "source", help = "path to the LIWC word stat file")
    parser.add_option("-d", "--dest", dest = "destination", help = "path to the keyword file that will be output")
    parser.set_defaults(corpus = "unrelated",
        source = "/Applications/LIWC2007/Dictionaries/LIWC2007WordStat/LIWC2007.cat",
        dest = os.environ["TH_PATH"] + "/TwitterHealthMental/mental/data")
    (options, args) = parser.parse_args()

    if options.corpus:
        if options.corpus == "d_pos" or options.corpus == "depression_positive":
            corpus = "d_pos"
        elif options.corpus == "d_neg" or options.corpus == "depression_negative":
            corpus = "d_neg"
        elif options.corpus == "a_pos" or options.corpus == "anxiety_positive":
            corpus = "a_pos"
        elif options.corpus == "a_neg" or options.corpus == "anxiety_negative":
            corpus = "a_neg"
        elif options.corpus == "unr" or options.corpus == "unrelated":
            corpus = "unr"
        else:
            logging.error("No such tweet corpus \"%s\"" % options.corpus)
            sys.exit(-1)
        logging.debug("Processing keywords for corpus \"%s\"" % corpus)
    if options.source:
        source = options.source
    if options.dest:
        dest = "%s/%s.pickle" % (options.dest, corpus)

    """ process word stat file """
    with open(source, 'r') as word_stat_file:
        toplevel, sublevel = None, None
        for line in word_stat_file:
            if re.match(toplevel_re, line):
                toplevel = re.match(toplevel_re, line).group(0)
                logging.debug("Top-level category: %s" % toplevel)
            elif re.match(sublevel_re, line):
                sublevel = re.match(sublevel_re, line).group(0).lstrip()
                logging.debug("\tSub-level category: %s" % sublevel)
                if sublevel in corpi[corpus]:
                    logging.debug("\tScanning %s for %s corpus keywords" % (sublevel, corpus))
            elif re.match(keyword_re, line):
                word = re.match(keyword_re, line).group(1).lstrip().lower()
                if sublevel in corpi[corpus]:
                    words[corpus] |= set([word])
                    logging.debug("\t\tKeyword %s from corpus %s" % (word, sublevel))
            else:
                logging.error("Couldn't match \"%s\"" % line)
                logging.error("Continuing...")

    """ output keyworks to pickle file """
    with open(dest, 'wb') as pickle_file:
        pickle.dump(words[corpus], pickle_file, -1)
