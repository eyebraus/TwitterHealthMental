
import logging
import os

def init_logging(file_ref):
    basename = os.path.basename(file_ref).split('.')[0]
    logging.basicConfig(filename = '%s.log' % basename, level = logging.DEBUG, filemode = 'w', format='%(message)s')
