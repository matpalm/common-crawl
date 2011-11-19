#!/usr/bin/env python
import sys, re
from collections import *

freqs = defaultdict(int)

for job_file in sys.argv: 
    ignoring = True
    for line in open(job_file,'r'):
        line = line.strip()
        if ignoring:
            if line == 'mime_types':
                ignoring = False
                continue
            else:
                continue        
        if line == 'Map-Reduce Framework':
            break
        match = re.match("(.*)=(\d+)$", line)
        if not match:
            print "????????????????????????????", line
            continue
        mime_type, freq = match.groups()
        freq = int(freq)
        freqs[mime_type] += freq

for k in freqs:
    print k, freqs[k]

    
