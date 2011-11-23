package cc;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.commoncrawl.hadoop.io.ARCInputFormat;
import org.commoncrawl.protocol.shared.ArcFileItem;

/**
 * @deprecated org.commoncrawl.hadoop.io.ARCInputFormat is painful
 */
public class FetchTextHtml extends Configured implements Tool {

  public static void main(String args[]) throws Exception {
    ToolRunner.run(new FetchTextHtml(), args);
  }
    
  public int run(String[] args) throws Exception {
        
    if (args.length!=1) {
      throw new RuntimeException("usage: FetchTextHtml <output>");
    }
    
    JobConf conf = new JobConf(getConf(), getClass());
    conf.set("jets3t.arc.source.bucket.name", "commoncrawl-crawl-002"); 
    conf.setJobName("FetchTextHtml");    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.set("mapred.compress.map.output", "true");
    conf.set("mapred.map.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec");
    conf.setInputFormat(ARCInputFormat.class);
    conf.setMapperClass(ArcProcessor.class);           
    FileOutputFormat.setOutputPath(conf, new Path(args[0]));
    
    JobClient.runJob(conf);

    return 0;
  }

  
  private static class ArcProcessor extends MapReduceBase implements Mapper<Text,ArcFileItem, Text,Text> {
        
    public void map(Text url, ArcFileItem item, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {

      try {
        String mimeType = item.getMimeType();
        reporter.getCounter("mime_types", mimeType).increment(1);           
        if (!"text/html".equals(mimeType)) {
          return;
        }
              
        String html = item.getContent().toString("UTF8").replaceAll("\\s+"," ").trim();
        
        if (html.isEmpty()) {
          reporter.getCounter("parse", "empty_html").increment(1);
        }
        else {
          collector.collect(new Text(url+" "+item.getTimestamp()), new Text(html));
        }
        
      }
      catch(Exception e) {        
        reporter.getCounter("exception", "exception_"+e.getClass().getName()).increment(1);
        System.err.println("exception on "+url.toString());
      }
      
    }
    
  }
  
}
