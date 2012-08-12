common crawl recently released some preprocessed versions of their 2012 crawl data. see their <a href="https://commoncrawl.atlassian.net/wiki/display/CRWL/About+the+Data+Set">wiki</a> for a great overview of the dataset

let's review one part of it, the link data...

first we need to know where to find it. as described in the wiki it's chunked into segments with all sorts of preprocessed data available. for this quick hack all i want is the meta data...

    $ s3cmd ls s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/ | grep metadata
      2012-07-08 00:53  41937063   s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/metadata-00000
      2012-07-08 00:54  42539894   s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/metadata-00001
      2012-07-08 00:57  32111625   s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/metadata-00002
      ...
      2012-07-08 01:32   5717068   s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/metadata-04379
      2012-07-08 01:32   5608750   s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/metadata-04380

since these files are hadoop sequence files, the easiest way to have a review them is using `hadoop fs -text`. 

    $ hadoop fs -text s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/metadata-00000 2>/dev/null | head -n1
      http://www.museo-cb.com/museo-cb/audio-y-video/frequency-vhs/   {"attempt_time":1328767770376,"disposition":"SUCCESS","server_ip":"77.229.63.16","http_result":200,"http_headers":{"response":"HTTP/1.1 200 OK","server":"Zope/(Zope 2.9.6-final, python 2.4.4...

this is standard hadoop key/value data where the first field is the crawl url and the second field is the json metadata. we can get a formatted version of the
meta data pretty simply...

    $ hadoop fs -text s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/metadata-00000 2>/dev/null \
     | head -n1 | cut -f2 | python -mjson.tool > meta_data_example.json

the <a href="https://github.com/matpalm/common-crawl/blob/master/meta_data_example/meta_data_example.json">meta_data_example.json</a> file is availble in this repo.

or particular interest is the link section...

    {
      ...
      "content": {
        "links": [
          {
            "href": "http://www.museo-cb.com/portal_javascripts/Plone%20Default/ploneScripts5575.js", 
            "text": "", 
            "type": "text/javascript"
          },
          {
            "href": "http://www.museo-cb.com/portal_css/Plone%20Default/ploneStyles8116.css", 
            "media": "screen", 
            "rel": "alternate stylesheet", 
            "text": "", 
            "title": "Small Text", 
            "type": "text/css"
          }, 
       ...
    }

if we want to do link analysis we just need to extract these

first we get an example row from a metadata file

    $ hadoop fs -text s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/metadata-00000 2>/dev/null
     | head -n1 > meta_data_single_row.tsv

then we feed it through a <a href="https://github.com/matpalm/common-crawl/blob/master/meta_data_example/links_extractor.py">links extraction script</a> which, in this simple case, just emits two columns; the source top level domain and the top level domain of each other link that's not the same. 

for our simple example it's just the one link...

    $ ./links_extractor.py < meta_data_single_row.tsv 
    www.museo-cb.com       contadores.miarroba.com

trying a slightly larger dataset...

    $ hadoop fs -text s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/metadata-00000 2>/dev/null | head -n100 | gzip > meta_data.100.tsv.gz
    $ zcat meta_data.100.tsv.gz | ./links_extractor.py | sort | uniq -c | sort -nr | head
     326 www.softmix.org	       softmix.org
      48 store.shopping.yahoo.co.jp	i.yimg.jp
      41 www.thenoiseacademy.com		thenoiseacademy.com
      31 www.softmix.org			depositfiles.com
      26 store.shopping.yahoo.co.jp	shopping.c.yimg.jp
      25 www.nissanvillage.com		server8.crucialnetworking.com
      25 www.maadvisor.com		api.ning.com
      22 www.sms.at			i.sms.at
      21 real-estate-atlanta.toddwalters.com	www.toddwalters.com
      15 www.wumpus-gollum-forum.de		www.oldradioworld.de

so my naive top level domain extraction was as naive as i expected (ie very) but you get the idea...

now that we have a working script we run it via hadoop streaming against an even larger dataset. let's just do the first metadata file.

    $ hadoop jar ./.versions/0.20.205/share/hadoop/contrib/streaming/hadoop-streaming.jar \
     -input s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/metadata-00000 \
     -output metadata-00000.links.gz \
     -inputformat SequenceFileAsTextInputFormat \
     -mapper 'python links_extractor.py' \
     -file links_extractor.py

changing the input to `-input s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/metadata-*` would do the entire 170gb of the first segment (if you've got a big enough cluster)

we've then got some link data to play with...

    $ hadoop fs -ls metadata-00000.links.gz
     Found 6 items
     -rw-r--r--   1 hadoop supergroup          0 2012-07-18 04:16 /user/hadoop/metadata-00000.links.gz/_SUCCESS
     -rw-r--r--   1 hadoop supergroup    2028125 2012-07-18 04:16 /user/hadoop/metadata-00000.links.gz/part-00000
     -rw-r--r--   1 hadoop supergroup    1961080 2012-07-18 04:16 /user/hadoop/metadata-00000.links.gz/part-00001
     -rw-r--r--   1 hadoop supergroup    2041350 2012-07-18 04:16 /user/hadoop/metadata-00000.links.gz/part-00002
     -rw-r--r--   1 hadoop supergroup    1960942 2012-07-18 04:16 /user/hadoop/metadata-00000.links.gz/part-00003
     -rw-r--r--   1 hadoop supergroup    2009801 2012-07-18 04:16 /user/hadoop/metadata-00000.links.gz/part-00004

    $ hadoop fs -text /user/hadoop/metadata-00000.links.gz/part-00002 | head -n5 
     1002.women-blog.net     www.illiweb.com
     1002.women-blog.net     d2.zedo.com
     1002.women-blog.net     www.illiweb.com
     2012theawakening.net    wordpress.org
     2012theawakening.net    laptopinsurancei.co.uk

to get a set of frequencies (like how we used `sort | uniq -c | sort -nr`) above is a trivial pig script...

    $ pig 
     set default_parallel 3;
     links = load '/user/hadoop/metadata-00000.links.gz' as (from_tld:chararray, to_tld:chararray);
     freqs = foreach (group links by (from_tld, to_tld)) generate flatten(group), SIZE(links) as freq;
     ordered = order freqs by freq desc;
     store ordered into 'link_freqs.gz';

    $ hadoop fs -text /link_freqs.gz/part-r-00000.gz | head
     www.quiosquedasideias.com        quiosquedasideias.com     2160
     www.akkuwelt-si.de               www.akkuwelt-netphen.de   1055
     www.softmix.org                  softmix.org               977
     www.picamigo.com                 picamigo.com              960
     www.gizmod.ru                    gizmod.ru                 725
     pages.intrinsiccollectibles.com  www.tias.com              708
     nibbler.silktide.com             www.vacando.ch            691
     www.polskielongisland.com        www.golocaljamaica.com    618
     www.soumbala.com                 soumbala.com              612
     www.sakut.jp                     atq.ck.valuecommerce.com  600

so with a bigger cluster, and a better top level domain extraction function, the links of the internet are yours for the taking!!














