set -ex
ACCESS_KEY=blah
SECRET_KEY=blah

function run {
 hadoop jar cc.jar cc.FetchTextHtml \
  -libjars commoncrawl-0.1.jar \
  -D mapred.output.compress=true -D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
  -D mapred.reduce.tasks=0 \
  -D jets3t.arc.source.aws.access.key.id=$ACCESS_KEY -D jets3t.arc.source.aws.secret.access.key=$SECRET_KEY \
  -D jets3t.arc.source.max.tries=50 \
  -D arc.input.format.io.block.size=524288 \
  -D jets3t.arc.source.input.prefixes.csv=$1 \
  html/$2
}

run 2010/01 2010_01
run 2010/02 2010_02
run 2010/04 2010_04
run 2010/08 2010_08
run 2010/09 2010_09


