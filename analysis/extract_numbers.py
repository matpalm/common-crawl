#!/usr/bin/env python
import sys

def parse(x):
    try:
        return str(int(x))
    except ValueError:
        try :
            return str(float(x))
        except ValueError:
            return None

num_tokens = 0
for line in sys.stdin:    
    try:
        tokens = line.split()
        num_tokens += len(tokens)
        numbers = filter(None, map(parse, tokens))
        for number in numbers:
            print(number)
    except:
        sys.stderr.write("reporter:counter:numbers,exception_"+str(sys.exc_info()[0])+",1\n")

sys.stderr.write("reporter:counter:numbers,num_tokens,"+num_tokens+"\n")

        
