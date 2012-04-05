#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""\
This sets up an Omeka instance from a root RDF resource on Freebase. This is
really pretty specific at this point, in that the path through the graph that
it takes to get from the exhibit node to the item nodes is hardcoded, but
conceivably, this could come from the configuration also.

It uses the information in the triples returned to pull labels, descriptions,
images, and coverage. If there are any Dublin Core metadata, these are also
transfered into Omeka.
"""


import argparse
import atexit
import datetime
import logging
import os
import sys
import time

import rdflib
import requests


__version__ = '0.0'


LOG_FORMAT = (
    '%(asctime)s [%(levelname)s] %(name)s : %(message)s'
    )
LOG_LEVELS = {
    'very-quiet': logging.CRITICAL,
    'quiet': logging.WARNING,
    'normal': logging.INFO,
    'verbose': logging.DEBUG,
    }

if sys.platform == 'win32':
    timer = time.clock
else:
    timer = time.time


##########################################
## Some useful namespaces and constants ##
##########################################


CC    = rdflib.Namespace('http://creativecommons.org/ns#')
FB    = rdflib.Namespace('http://rdf.freebase.com/ns/')
XHTML = rdflib.Namespace('http://www.w3.org/1999/xhtml/vocab#')


#######################
## Walking the Graph ##
#######################


###################
## Loading Files ##
###################


LAST_DOWNLOAD = 0
def graph_parse(graph, uri, seconds=1):
    """\
    This throttles downloading resources to make sure we don't do more than
    one a second.

    Yucky global, but otherwise we'd need a superfluous class.

    """

    global LAST_DOWNLOAD

    now = time.time()
    elapsed = now - LAST_DOWNLOAD
    if elapsed < seconds:
        time.sleep(elapsed)
    LAST_DOWNLOAD = now

    return graph.parse(uri)


####################################
## Getting Data, Populating Omeka ##
####################################


def load_parks(args):
    """\
    This is the simple entry-point to processing. This populates the graph and
    feeds all the information into Omeak.

    """

    uri = rdflib.URIRef(args.exhibit_url)

    g = rdflib.Graph()
    graph_parse(g, uri)


####################
## Infrastructure ##
####################


def parse_args(argv):
    """\
    This parses the command-line arguments in argv and returns a tuple
    containing the options and other arguments.  
    """

    op = argparse.ArgumentParser(
            description=__doc__,
            fromfile_prefix_chars='@'
            )

    op.add_argument('-e', '--exhibit-uri', action='store', dest='exhibit_uri',
                    default=None,
                    help="The URI for the exhibit's RDF data.")

    op.add_argument('-o', '--omeka', action='store', dest='omeka_url',
                    default=None,
                    help='The Omeka installation to populate.')
    op.add_argument('-u', '--user', action='store', dest='omeka_user',
                    default=None,
                    help='The Omeka admin user to log in as.')
    op.add_argument('-p', '--passwd', action='store', dest='omeka_passwd',
                    default=None,
                    help='The password for the Omeka admin user OMEKA_USER.')

    op.add_argument('--log-dest', action='store', dest='log_file',
                    default='STDERR',
                    help='The name of the file to send log messages to. '
                         '"STDOUT" and "STDERR" will print to the screen. '
                         'Default=%(default)s.')
    op.add_argument('--log-level', action='store', dest='log_level',
                    choices=sorted(LOG_LEVELS), default='normal',
                    help='The level of logging information to output. Valid '
                         'choices are "quiet", "normal", and "verbose". '
                         'Default="%(default)s".')

    args = op.parse_args(argv)

    if (args.exhibit_uri is None or args.omeka_url is None or
        args.omeka_user is None or args.omeka_passwd is None):
        op.error(
            'You must supply all of EXHIBIT_URI, OMEKA_USER, OMEKA_PASSWD.'
            )

    return args


def setup_logging(opts):
    """\
    This sets up the logging system, based on the values in opts. Specifically,
    this looks for the log_file and log_level attributes on opts.

    """

    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(LOG_LEVELS[opts.log_level])
    if opts.log_file == 'STDOUT':
        handler = logging.StreamHandler(sys.stdout)
    else:
        handler = logging.FileHandler(opts.log_file)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(handler)
    atexit.register(logging.shutdown)


def main(argv=None):
    args = parse_args(argv or sys.argv[1:])
    setup_logging(args)
    try:
        start = timer()

        load_parks(args)

        end = timer()
        logging.info('done')
        logging.info('elapsed time: %s', datetime.timedelta(seconds=end-start))
    except SystemExit, exit:
        return exit.code
    except KeyboardInterrupt:
        logging.warning('KeyboardInterrupt')
        return 2
    except:
        logging.exception('ERROR')
        return 1
    else:
        return 0


if __name__ == '__main__':
    sys.exit(main())

