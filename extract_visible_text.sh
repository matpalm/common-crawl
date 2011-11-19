hadoop jar cc.jar cc.ExtractVisibleText \
 -libjars boilerpipe-1.2.0.jar,nekohtml-1.9.13.jar,xerces-2.9.1.jar \
 -D mapred.reduce.tasks=3000 \
 -D mapred.output.compress=true \
 -D mapred.output.compression.type=BLOCK \
 -D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
 html/* visible_text

