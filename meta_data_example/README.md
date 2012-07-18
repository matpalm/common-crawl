common crawl recently released some preprocessed versions of their 2012 crawl data. a great overview of the dataset is available
on theif <a href="https://commoncrawl.atlassian.net/wiki/display/CRWL/About+the+Data+Set">wiki</a>

let's review one part of it, the link data...

first we need to check the manifests

    s3cmd ls s3://aws-publicdatasets/common-crawl/parse-output/valid_segments/
      DIR   s3://aws-publicdatasets/common-crawl/parse-output/valid_segments/1341690147253/
      DIR   s3://aws-publicdatasets/common-crawl/parse-output/valid_segments/1341690148298/
      DIR   s3://aws-publicdatasets/common-crawl/parse-output/valid_segments/1341690149519/
      ....

reviewing just one, say the first, we can check the data available, there's three types, the arc files with the actual crawl data as well as some
parsed text files and some meta data. for now let's just check the meta data...

    s3cmd ls s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/ | grep metadata
      2012-07-08 00:53  41937063   s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/metadata-00000
      2012-07-08 00:54  42539894   s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/metadata-00001
      2012-07-08 00:57  32111625   s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/metadata-00002
      ...
      2012-07-08 01:32   5717068   s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/metadata-04379
      2012-07-08 01:32   5608750   s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/metadata-04380

so for this segment there are 4381 files totalling about 170gb. 

since these files are hadoop sequence files, the easiest way to have a review them is using `hadoop fs -text`. eg, from an elastic mapreduce cluster

    hadoop fs -text s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/metadata-00000 2>/dev/null | head -n1
      http://www.museo-cb.com/museo-cb/audio-y-video/frequency-vhs/   {"attempt_time":1328767770376,"disposition":"SUCCESS","server_ip":"77.229.63.16","http_result":200,"http_headers":{"response":"HTTP/1.1 200 OK","server":"Zope/(Zope 2.9.6-final, python 2.4.4...

this is standard hadoop key/value data where the first field is the crawl url and the second field is the json metadata. we can get a formatted version of the
meta data pretty simply...

    hadoop fs -text s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/metadata-00000 2>/dev/null \
     | head -n1 | cut -f2 | python -mjson.tool > meta_data_example.json

<a href="meta_data_example.json">this meta data file</a> is included in this repo.







