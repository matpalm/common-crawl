# getting all the pdfs

let's make it simple and only consider the set with cuurent crawl (2012). it'll be a bit more work to fetch from the archived crawl (2008 - 2010)

we'll do a simple manual one off selection of files (the generalisation to a MR job will be pretty straightforward)


start by listing all valid segments

    $ s3cmd ls s3://aws-publicdatasets/common-crawl/parse-output/valid_segments/
                       DIR   s3://aws-publicdatasets/common-crawl/parse-output/valid_segments/1341690147253/
                       DIR   s3://aws-publicdatasets/common-crawl/parse-output/valid_segments/1341690148298/
    ...

pick one and from it list the metadata...

    $ s3cmd ls s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/ | grep metadata | head -n1
    2012-07-08 00:53  41937063   s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/metadata-00000

get it...

    $ s3cmd get s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/metadata-00000

since it's a sequence file we need hadoop to unpack it and pass it through a simple python script to dump offset info

    $ hadoop fs -text metadata-00000 | ./dump_offset_info_for_pdfs.py | head
    http://www.cambridgestudents.org.uk/subjectpages/physics/igscephysics/physicsmodelanswers/0625_s03_qp_3%20model%20answers%20final.pdf	s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/1341708194671_0.arc.gz	207287	224093
    http://www.idwlcms.org/publications/the_monthly/june/2009/fan_into_flame_newsletter.pdf	s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/1341708194671_0.arc.gz	820004	45015
    http://www.supremecourtofindia.nic.in/sciannualreport/annualreport2005-2006/Supreme%20Court%20Chapt%201-2.pdf	s3://aws-publicdatasets/common-crawl/parse-output/segment/1341690147253/1341708194671_0.arc.gz	921164	519441

we've know got a list of the pdfs and their locations in the archive
( see https://groups.google.com/d/msg/common-crawl/0bA4ktdovCU/IFizRShl-QsJ for some more info on this )
( and see https://commoncrawl.atlassian.net/wiki/display/CRWL/About+the+Data+Set for an interpretation of the how to unpack the data... )

TODO: the code for offset fetching from s3...

