#!/usr/bin/env python 
import sys, urllib, re
title_re = re.compile("<title>(.*?)</title>", 
re.MULTILINE | re.DOTALL | re.IGNORECASE)
for line in sys.stdin:
    url = line.strip()
    match = 
title_re.search(urllib.urlopen(url).read())
    if match:
        print "mapper", url, "\t", 
match.group(1).strip()  
