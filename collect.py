#!/usr/bin/python

import data
import urllib2
import csv
import time
import json
from dateutil.parser import parse
from data.tables import *
from urlparse import urlparse
from sqlalchemy import func
from py4j.java_gateway import JavaGateway

def getConfig():
    with open('config.json', 'rb') as fin:
        return json.load(fin)

def normalizeUrl(url):
    if not url:
        return None
    if '://' not in url:
        url = 'http://' + url
    scheme, netloc, path, params, query, fragment, = urlparse(url)
    return '%s://%s/%s%s' % (scheme, netloc.rstrip('/'), path.lstrip('/'), '?' + query if query else '')

def getQueryResults():
    print 'fuck'
    with open('gharchive/query-results.csv', 'rb') as fin:
        reader = csv.reader(fin)
        next(reader)
        return [(name, int(forks)) for name, forks in reader]

def main():
    config = getConfig();
    db = data.connect(config['db'])

    gateway = JavaGateway()
    locationApp = gateway.entry_point
    with db.session(autocommit = True) as session:
        disappearedOrgs = set([v for v, in session.query(NotFound.name).all()])
        lines = getQueryResults()
        for pos, (name, forks) in enumerate(lines, start = 1):
            startTime = time.time()
            org = session.query(Organization).filter(func.lower(Organization.name) == name).first()
            if org:
                org.forks_2014 = forks
                org.suggested_country = locationApp.countryCode(org.location)
            elif name not in disappearedOrgs:
                try:
                    request = urllib2.urlopen('https://api.github.com/orgs/%s?access_token=%s' % (name, config['access_token']))
                    response = json.load(request)
                    session.add(
                        Organization(
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
                    )
                except urllib2.HTTPError as e:
                    if e.code <> 404:
                        raise
                    print e
                    session.add(NotFound(name = name))

            elapsed = (time.time() - startTime) * 1000
            print '%d. %s, elapsed %dms, left %d' % (pos, name, elapsed, len(lines) - pos)


if __name__ == '__main__':
    main()
