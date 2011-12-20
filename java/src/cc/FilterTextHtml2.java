package cc;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tika.parser.txt.CharsetDetector;
import org.apache.tika.parser.txt.CharsetMatch;
import org.commoncrawl.hadoop.io.ARCInputFormat;
import org.commoncrawl.hadoop.io.JetS3tARCSource;
import org.commoncrawl.protocol.shared.ArcFileItem;

public class FilterTextHtml2 extends Configured implements Tool {

  public static void main(String args[]) throws Exception {
    ToolRunner.run(new FilterTextHtml2(), args);
  }

  public int run(String[] args) throws Exception {
    
    if (args.length!=2) {
      throw new RuntimeException("usage: "+getClass().getName()+" <input> <output>");
    }
    
    JobConf conf = new JobConf(getConf(), getClass());
    
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    
    conf.setJobName(getClass().getName()+" "+args[0]);    
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.set("mapred.output.compress", "true");
    conf.set("mapred.output.compression.type", "BLOCK");
    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");

    conf.set(JetS3tARCSource.P_INPUT_PREFIXES, args[0]);
    conf.set(JetS3tARCSource.P_AWS_ACCESS_KEY_ID, conf.get("fs.s3n.awsAccessKeyId"));
    conf.set(JetS3tARCSource.P_AWS_SECRET_ACCESS_KEY, conf.get("fs.s3n.awsSecretAccessKey"));
    conf.set(JetS3tARCSource.P_BUCKET_NAME, "commoncrawl-crawl-002");   
    
    ARCInputFormat.setARCSourceClass(conf, JetS3tARCSource.class);
    ARCInputFormat inputFormat = new ARCInputFormat();
    inputFormat.configure(conf);
    conf.setInputFormat(ARCInputFormat.class);

    conf.setMapperClass(FilterTextHtmlMapper.class);        
    
    conf.setMaxMapTaskFailuresPercent(100);
    conf.setNumReduceTasks(0);       
    
    JobClient.runJob(conf);

    return 0;
  }  
  
  public static class FilterTextHtmlMapper extends MapReduceBase implements Mapper<Text,ArcFileItem,Text,Text> {
    
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    
    public void map(Text url, ArcFileItem item, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
            
      try {
        // status
        reporter.setStatus("processing "+item.getArcFileName());
        
        // filter out non text/html
        String mime_type = item.getMimeType();
        reporter.getCounter("FilterTextHtml.mime_types", mime_type).increment(1);        
        if (!"text/html".equals(mime_type)) {
          return;
        }
                               
        // try to determine character encoding for content
        CharsetMatch charset = new CharsetDetector().setText(item.getContent().getBytes()).detect();                
        
        // fetch http response as decoded by CharsetDetector 
        String decodedHttpResponse;        
        try {
          decodedHttpResponse = charset.getString();
        }
        catch (NullPointerException e) {
          // unexplainable cases of CharsetMatch throwing null pointer for what looks to be sane text ?
          // just use string as best bet
          decodedHttpResponse = new String(item.getContent().getReadOnlyBytes());
          reporter.getCounter("FilterTextHtml", "nullptr_in_CharsetMatch").increment(1);
        }
        
        // collect some stats on response sizes
        int responseSize = (int)Math.ceil(Math.log10(decodedHttpResponse.length()));
        reporter.getCounter("FilterTextHtml.responseSize", ""+responseSize).increment(1);
        
        // output
        Text key   = new Text(url.toString() + "\t" + sdf.format(item.getTimestamp()));
        Text value = new Text(decodedHttpResponse);
        collector.collect(key, value);
                
      }
      catch(Exception e) {        
        e.printStackTrace();
        reporter.getCounter("FilterTextHtml.exception", e.getClass().getSimpleName()).increment(1);
      }      
     
    }   
      
  }
   
}
