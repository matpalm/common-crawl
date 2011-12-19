package cc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
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
import org.apache.nutch.tools.arc.ArcInputFormat;
import org.apache.tika.parser.txt.CharsetDetector;
import org.apache.tika.parser.txt.CharsetMatch;

public class FilterTextHtml extends Configured implements Tool {

  public static void main(String args[]) throws Exception {
    ToolRunner.run(new FilterTextHtml(), args);
  }
    
  public int run(String[] args) throws Exception {
        
    if (args.length!=2) {
      throw new RuntimeException("usage: "+getClass().getName()+" <input> <output>");
    }
    
    JobConf conf = new JobConf(getConf(), getClass());
    conf.setJobName(getClass().getName());
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.set("mapred.output.compress", "true");
    conf.set("mapred.output.compression.type", "BLOCK");
    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
    
    conf.setMaxMapTaskFailuresPercent(100);
    conf.setNumReduceTasks(0);
    
    conf.setInputFormat(ArcInputFormat.class);
    conf.setMapperClass(FilterTextHtmlMapper.class);    
    
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    
    JobClient.runJob(conf);

    return 0;
  }
  
  public static class FilterTextHtmlMapper extends MapReduceBase implements Mapper<Text,BytesWritable,Text,Text> {
    private static final String UNKNOWN = "unknown";

    enum COLUMNS { URL, IP, DTS, MIME_TYPE, SIZE };    
    
    public void map(Text k, BytesWritable v, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
   
      try {
        String headerColumns[] = k.toString().split(" ");      
        if (headerColumns.length != COLUMNS.values().length) {
          System.err.println("dodgy header row? ["+k+"]");
          reporter.getCounter("FilterTextHtml", "dodgy_header").increment(1);
          return;
        }
        
        String mime_type = headerColumns[COLUMNS.MIME_TYPE.ordinal()];
        reporter.getCounter("FilterTextHtml.mime_types", mime_type).increment(1);        
        if (!"text/html".equals(mime_type)) {
          return;
        }
          
        // search for end of header   
        int headerLength = locateEndOfHeader(v.getBytes());
                               
        // try to determine character encoding for rest of response    
        // ( 4 bytes for the header/body seperator 0d 0a 0d 0a
        InputStream responseBytes = new ByteArrayInputStream(v.getBytes(), headerLength + 4, v.getLength() - headerLength - 4);
        CharsetMatch charset = new CharsetDetector().setText(responseBytes).detect();                
        
        // fetch http response as decoded by CharsetDetector 
        String decodedHttpResponse;        
        try {
          decodedHttpResponse = charset.getString();
        }
        catch (NullPointerException e) {
          // unexplainable cases of CharsetMatch throwing null pointer for what looks to be sane text ?
          // just use string as best bet
          decodedHttpResponse = new String(v.getBytes(), headerLength + 4, v.getLength() - headerLength - 4);
          reporter.getCounter("FilterTextHtml", "nullptr_in_CharsetMatch").increment(1);
        }
        
        // emit
        String url = headerColumns[COLUMNS.URL.ordinal()];
        String dts = headerColumns[COLUMNS.DTS.ordinal()];
//        String tld = topLevelDomain(url);
//        String dts = headerColumns[COLUMNS.DTS.ordinal()];
        collector.collect(new Text(url+"\t"+dts), new Text(decodedHttpResponse));
        
      }      
      catch(Exception e) {        
        e.printStackTrace();
        reporter.getCounter("FilterTextHtml.exception", e.getClass().getSimpleName()).increment(1);
      }
      
    }   
    
    private int locateEndOfHeader(byte[] b) {      
      for(int idx=0; idx<b.length-4; idx++) {
        if (b[idx]==0x0d && b[idx+1]==0x0a && b[idx+2]==0x0d && b[idx+3]==0x0a)
          return idx;
      }        
      throw new RuntimeException("couldn't find end of header");
    }   
   
    private String topLevelDomain(String url) {
      url = url.replaceFirst("^http://","");
      
      int firstSlashIdx = url.indexOf("/");
      if (firstSlashIdx!=-1) {
        url = url.substring(0, firstSlashIdx);
      }
      
      return url;
    }    
  }
  
}
