#!/usr/bin/env python
import sys
from collections import *
freqs = defaultdict(int)
for line in sys.stdin:
    mime_type, freq = line.strip().split("=")
    freq = int(freq)
    freqs[mime_type] += freq
for k in freqs:
    print k, freqs[k]

    
