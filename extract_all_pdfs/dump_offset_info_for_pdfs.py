#!/usr/bin/env python
import json, sys, pprint
for line in map(str.strip, sys.stdin):
    try:
        file_name, metadata = line.split("\t")
        if file_name.endswith('pdf'):  # clumsy, perhaps a content-type check would be cleaner...
            metadata = json.loads(metadata)
            pprint.pprint(metadata)
            archive_info = metadata['archiveInfo']
            segment_date_partition = ( archive_info['arcSourceSegmentId'], archive_info['arcFileDate'], archive_info['arcFileParition'] )
            s3_path = "s3://aws-publicdatasets/common-crawl/parse-output/segment/%s/%s_%s.arc.gz" % segment_date_partition
            offset, length = archive_info['arcFileOffset'], archive_info['compressedSize']
            print "\t".join(map(str, [file_name, s3_path, offset, length]))        
    except:
        # for this simple demo just ignore anything that gives us grief...
        pass
