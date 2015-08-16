#!/usr/bin/python

import data
import urllib2
import csv
import time
import json
import argparse
from dateutil.parser import parse
from data.tables import *
from urlparse import urlparse
from sqlalchemy import func
from py4j.java_gateway import JavaGateway
from py4j.protocol import Py4JNetworkError

def main(args, locationApp):
    config = json.load(args.config)

    def normalizeUrl(url):
        if not url:
            return None
        if '://' not in url:
            url = 'http://' + url
        scheme, netloc, path, params, query, fragment, = urlparse(url)
        return '%s://%s/%s%s' % (scheme, netloc.rstrip('/'), path.lstrip('/'), '?' + query if query else '')

    def queryResults():
        reader = csv.reader(args.organizations)
        next(reader)
        return [(name, int(forks)) for name, forks in reader]

    def obeyRateLimit(headers):
        remaining = int(headers['X-RateLimit-Remaining'])
        reset = long(headers['X-RateLimit-Reset'])
        if remaining < 3:
            tts = reset - time.time()
            print 'sleeping for %f sec due to GitHub rate limit' % tts
            time.sleep(tts)

    def createOrganization(name, forks):
        request = urllib2.urlopen('https://api.github.com/orgs/%s?access_token=%s' % (name, config['access_token']))
        obeyRateLimit(request.headers)
        response = json.load(request)
        return Organization(
            id = response['id'],
            name = response['login'],
            location = response.get('location', None),
            public_repos = response.get('public_repos', 0),
            forks_2014 = forks,
            url_github = response.get('html_url', None),
            url_site = normalizeUrl(response.get('blog', None)),
            created_at = parse(response['created_at']),
            suggested_country = locationApp.countryCode(response.get('location', None))
        )

    db = data.connect(config['db'])
    with db.session(autocommit = True) as session:
        disappeared = set([v for v, in session.query(NotFound.name).all()])
        rows = queryResults()
        for pos, (name, forks) in enumerate(rows, start = 1):
            startTime = time.time()
            org = session.query(Organization).filter(func.lower(Organization.name) == name).first()
            if org:
                org.forks_2014 = forks
                org.suggested_country = locationApp.countryCode(org.location)
            elif name not in disappeared:
                try:
                    org = createOrganization(name, forks)
                    session.add(org)
                except urllib2.HTTPError as e:
                    if e.code <> 404:
                        raise
                    print e
                    session.add(NotFound(name = name))

            elapsed = (time.time() - startTime) * 1000
            print '%d. %s, elapsed %dms, left %d' % (pos, name, elapsed, len(rows) - pos)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('config', type = argparse.FileType('r'), help = 'config file in JSON format')
    parser.add_argument('organizations', type = argparse.FileType('r'), help = 'GitHub organizations CSV [name, #forks]')
    args = parser.parse_args()
    gateway = JavaGateway()
    locationApp = gateway.entry_point

    try:
        main(args, locationApp)
    except Py4JNetworkError:
        print 'Make sure that locationparser is running'
