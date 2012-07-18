#!/usr/bin/env python
import sys, json, re

tld_pattern = re.compile(".*://(.*?)/.*")

def tld_for(url):
    match = tld_pattern.match(url.lower())
    return match.group(1) if match else None

for line in sys.stdin:
    try:
        # split line
        from_url, meta_data_json = line.strip().split("\t")
        # parse meta data and extract hrefs from any links
        meta_data = json.loads(meta_data_json)
        to_links = [link['href'] for link in meta_data['content']['links']]
        # map href to it's top level domain using the insanely overly simple tld regex
        from_tld = tld_for(from_url)
        to_tlds = filter(None, map(tld_for, to_links))
        # remove to_links to from_tld
        to_tlds = filter(lambda to_tld: to_tld != from_tld, to_tlds)
        # emit
        for to_tld in to_tlds:
            print "\t".join([from_tld, to_tld])
    except:
        # can't win em all...
        pass
