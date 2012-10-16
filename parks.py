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

Requires Python 2.7 with rdflib, requests, and pyproj installed.
"""

# TODO: Would be nice to serialize the graph afterward and be able to use that
# on subsequent runs.
#
# TODO: If fb:location.location.geolocation doesn't exist, this should look at
# owl:sameAs to see if any of them are geo-encoded.


import argparse
import atexit
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
import datetime
from itertools import islice
import logging
import os
import pprint
import re
import sys
import time
from urlparse import urljoin
from xml.sax.saxutils import escape

import pyproj
import rdflib
import requests


__version__ = '0.0'


LOG_FORMAT = (
    '%(asctime)s [%(levelname)s] %(name)s : %(message)s'
    )
LOG_LEVELS = {
    'very-quiet' : logging.CRITICAL,
    'quiet'      : logging.WARNING,
    'normal'     : logging.INFO,
    'verbose'    : logging.DEBUG,
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
BLURB = 'http://www.freebase.com/api/trans/blurb/'
IMG   = 'http://www.freebase.com/api/trans/raw/'

LOG      = None
LOGRDF   = None
LOGOMEKA = None

GOOGLE = pyproj.Proj(
        '+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 '
         '+lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m '
         '+nadgrids=@null +no_defs'
         )


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

    LOGRDF.debug('downloading <%s>.' % uri)

    start_size = len(graph)
    result = graph.parse(uri)
    end_size = len(graph)

    LOGRDF.debug('downloaded %s triples.' % (end_size - start_size,))
    return result


################################
## Graph Navigation Utilities ##
################################


def ensure(graph, uri):
    """This makes sure that there are statements with uri as the subject. """
    if not has_subj(graph, uri):
        graph_parse(graph, uri)


def first(iterator, default=None):
    """\
    This takes an iterator and returns the first item or default, if the
    iterator is empty.

    """

    try:
        item = iter(iterator).next()
    except StopIteration:
        item = default

    return item


def has_subj(graph, uri):
    """This tests whether graph has uri as a subject. """
    return first(graph.triples((uri, None, None))) is not None


def drill(graph, uri, predicates, n=0):
    """\
    This follows a series predicates through the graph and returns an iterator
    over the final targets.

    At each step, it makes sure that the target is in the graph. If it's not,
    it attempts to load it. If at any point, there is no target for a
    predicate, an empty iterator is returned.

    """

    if n >= len(predicates):
        yield uri
        return

    ensure(graph, uri)

    p = predicates[n]
    n += 1
    for o in graph.objects(uri, p):
        for target in drill(graph, o, predicates, n):
            yield target


def isa(graph, uri, rdf_type):
    """This tests whether the uri is defined to have RDF#type. """
    return first(graph.triples((uri, rdflib.RDF.type, rdf_type))) is not None


def get_listed_sites(graph, uri):
    """\
    This looks for fb:protected_sites.site_listing_category.listed_sites and
    gets those graphs. It returns the URI for the child graphs.

    """

    SITES = FB['base.usnationalparks.nps_classification.classification']
    for child in graph.objects(uri, SITES):
        ensure(graph, child)
        yield child


def predicates(graph, uri):
    """\
    This returns an ordered list of the set of predicates edges from this uri.

    """

    return sorted(set( p for (p, _) in graph.predicate_objects(uri) ))


####################################
## Getting Data, Populating Omeka ##
####################################


def load_parks(args):
    """\
    This is the simple entry-point to processing. This populates the graph and
    feeds all the information into Omeak.

    """

    uri = rdflib.URIRef(args.exhibit_uri)

    g = rdflib.Graph()
    graph_parse(g, uri)

    omeka_url = args.omeka_url
    if omeka_url[-1] != '/':
        omeka_url += '/'

    cookies = login(omeka_url, args.omeka_user, args.omeka_passwd)
    populate_exhibit(g, uri, omeka_url, cookies)


def login(omeka_url, user, passwd):
    """This logs into the Omeka admin site and returns the cookies. """
    url  = urljoin(omeka_url, 'admin/users/login')
    auth = {
            'username': user,
            'password': passwd,
            'remember': '1',
            }

    LOGOMEKA.info('logging into %s' % (url,))
    resp = requests.post(url, data=auth)
    assert resp.ok, 'login: %s' % (resp.status_code,)
    return resp.cookies


def populate_exhibit(graph, uri, omeka_url, cookies):
    """\
    This takes an RDF graph, and entry URI, and information about an Omeka
    installation, and it creates an exhibit with the items from the graph in
    it.

    """

    data = {}

    # title
    # slug
    for o in graph.objects(uri, FB['type.object.name']):
        if getattr(o, 'language', None) == u'en':
            data['title'] = unicode(o)
            data['slug']  = re.sub(r'\W', '-', o.lower())

    # credit
    for name in graph.objects(uri, CC['attributionName']):
        data['credits'] = name

    # description
    descr = get_topic_article(graph, uri)
    if descr is not None:
        data['description'] = descr

    exhibit_add = urljoin(omeka_url, 'admin/exhibits/add')
    resp = requests.post(exhibit_add, cookies=cookies, data=data)

    LOGOMEKA.info('created exhibit: %(title)s' % data)
    LOGOMEKA.debug(pprint.pformat(data))
    LOGOMEKA.debug('response: %s %s' % (resp.status_code, resp.url))

    for child_uri in get_listed_sites(graph, uri):
        populate_item(graph, child_uri, omeka_url, cookies)

    LOGOMEKA.info('done with exhibit items')


def populate_field(graph, uri, predicate, params, element_id, language=None,
                   html=False):
    """\
    This queries the field and loads the objects of (uri, predicate) into the
    parameters. If the language option is given (i.e., not None), then only
    values with a matching language property will be used.

    """

    text_key = 'Elements[%d][0][text]' % (element_id,)
    html_key = 'Elements[%d][0][html]' % (element_id,)
    for o in graph.objects(uri, predicate):
        if language is None or getattr(o, 'language', None) == language:
            params[text_key] = unicode(o)
            params[html_key] = (html and '1') or '0'


def populate_item(graph, uri, omeka_url, cookies):
    """\
    This creates an item for the URI and returns the Omeka admin URL for it.

    """

    params = {}

    # public
    params['public'] = ['0', '1']

    # title
    populate_field(graph, uri, FB['type.object.name'], params, 50, u'en')
    title = params.get('Elements[50][0][text]', '???')

    # subject
    types = [
            u'<a href="%s">%s</a>' % (o, o.split(u'/')[-1])
            for o in graph.objects(uri, rdflib.RDF.type)
            ]
    params['Elements[49][0][text]'] = ', '.join(types)
    params['Elements[49][0][html]'] = '1' 

    # description
    descr = get_topic_article(graph, uri)
    if descr is not None:
        params['Elements[41][0][text]'] = descr
        params['Elements[41][0][html]'] = '0'

    # source
    populate_field(graph, uri, CC['attributionName'], params, 48)

    # date
    populate_field(graph, uri,
                   FB['protected_sites.protected_site.date_established'],
                   params, 40)

    # rights
    populate_field(graph, uri, XHTML['license'], params, 47)

    # identifier
    params['Elements[43][0][text]'] = unicode(uri)
    params['Elements[43][0][html]'] = '0'

    # coverage
    populate_coverage(graph, uri, title, params)

    # image file
    files = {}
    for (n, img_uri) in enumerate(graph.objects(uri, FB['common.topic.image'])):
        ensure(graph, img_uri)
        populate_file(graph, n, img_uri, files)

    # submit
    LOGOMEKA.debug('creating item: %(Elements[50][0][text])s' % params)
    LOGOMEKA.debug('params: ' + pprint.pformat(params))
    LOGOMEKA.debug('files : ' + pprint.pformat(files))

    item_add = urljoin(omeka_url, 'admin/items/add')
    resp = requests.post(item_add, cookies=cookies, data=params, files=files,
                         allow_redirects=False,
                         # config={ 'verbose': sys.stderr },
                         )

    LOGOMEKA.info('created item: %(Elements[50][0][text])s' % params)
    LOGOMEKA.debug('response: %s %s' % (resp.status_code, resp.url))


def populate_file(graph, n, uri, files):
    """\
    This adds information about a file from an fb:common.image graph node.

    The file is read and the data inserted into a files dict for passing into
    the request.

    """

    gid     = uri.rsplit(u'/', 1)[-1]
    raw_uri = IMG + gid.replace(u'.', u'/')

    name = gid
    for o in graph.objects(uri, FB['type.object.name']):
        lang = getattr(o, 'language', None)
        if lang is None or lang == u'en':
            name = unicode(o)

    for o in graph.objects(uri, FB['type.content.media_type']):
        if o.endswith(u'.jpeg') and (not name.endswith(u'.jpg') or
                                     not name.endswith(u'.jpeg')):
            name += '.jpg'
        elif o.endswith(u'.png') and not name.endswith(u'.png'):
            name += '.png'

    resp       = requests.get(raw_uri)
    key        = 'file[%d]' % (n,)
    files[key] = (name, StringIO(resp.content))


def populate_coverage(graph, uri, title, params):
    """This populates a coverage field. """
    geolocs = list(graph.objects(uri, FB['location.location.geolocation']))
    LOGRDF.debug(
            'Downloaded geolocation information: {0}.'.format(geolocs)
            )
    for o_ in geolocs:
        ensure(graph, o_)

        lon = lat = None
        for (p, o) in graph.predicate_objects(o_):
            if p == FB['location.geocode.longitude']:
                lon = float(o)
            elif p == FB['location.geocode.latitude']:
                lat = float(o)

        if lon is not None and lat is not None:
            (x, y) = GOOGLE(lon, lat)
            wkt    = 'POINT(%f %f)' % (x, y)

            params['Elements[38][0][geo]']        = wkt
            params['Elements[38][0][zoom]']       = '10'
            params['Elements[38][0][center_lon]'] = unicode(x)
            params['Elements[38][0][center_lat]'] = unicode(y)
            params['Elements[38][0][base_layer]'] = 'gphy'
            params['Elements[38][0][text]']       = '%s/10/%f/%f/gphy\n%s' % (
                    wkt, x, y, title,
                    )
            params['Elements[38][0][html]']       = '0'
            params['Elements[38][0][mapon]']      = '1'


def get_topic_article(graph, uri):
    """This downloads and returns the fb:common.topic.article for the item. """
    text = None

    for o in graph.objects(uri, FB['common.topic.article']):
        ensure(graph, o)
        if isa(graph, o, FB['common.document']):
            blurb = urljoin(BLURB, o.rsplit('/', 1)[-1].replace('.', '/'))
            resp = requests.get(blurb, params={ 'maxlength': '6400' })
            if resp.ok:
                text = resp.text
            else:
                LOGRDF.debug(
                        'trying to download the description. status = %s' % (
                            resp.status_code,))

    return text


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

    config = {}
    if opts.log_file == 'STDOUT' or opts.log_file == '-':
        config['stream'] = sys.stdout
    elif opts.log_file == 'STDERR':
        config['stream'] = sys.stderr
    else:
        config['filename'] = opts.log_file
    config['level'] = LOG_LEVELS[opts.log_level]

    logging.basicConfig(**config)
    atexit.register(logging.shutdown)

    attach_loggers()


def attach_loggers():
    """This attaches the appropriate loggers to the global variables. """
    global LOG, LOGRDF, LOGOMEKA
    LOG      = logging.getLogger('parks')
    LOGRDF   = logging.getLogger('parks.rdf')
    LOGOMEKA = logging.getLogger('parks.omeka')


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

