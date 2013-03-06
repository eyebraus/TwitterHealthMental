#!/usr/bin/env python

from boto.mturk.connection import MTurkConnection
import couchdb
from datetime import datetime, timedelta
import logging
from optparse import OptionParser
import os
import sys