package cc;

import java.io.IOException;
import java.nio.charset.Charset;

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
import org.apache.tika.language.LanguageIdentifier;

public class FilterTextHtml extends Configured implements Tool {

  public static void main(String args[]) throws Exception {
    
//  byte[] test = new byte[] { 0x50, 0x6f, 0x6b, (byte) 0xe9, 0x6d, 0x6f, 0x6e }; // corrupt
//  byte[] test = new byte[] { (byte) 0xfe, (byte) 0xff, 0x00,0x50, 0x00,0x6f, 0x00,0x6b, 0x00,(byte)0xe9, 0x00,0x6d, 0x00,0x6f, 0x00,0x6e }; // utf-16
//  byte[] test = new byte[] { 0x50, 0x6f, 0x6b, (byte) 0xc3, (byte) 0xa9, 0x6d, 0x6f, 0x6e }; // utf-8
//    System.out.println(new String(test, "ISO-8859-1"));
//    System.out.println(hexen(new String(test).getBytes("ISO-8859-1")));
//    
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
//    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
    
    conf.setNumReduceTasks(0);
    
    conf.setInputFormat(ArcInputFormat.class);
    conf.setMapperClass(FilterTextHtmlMapper.class);    
    
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    
    JobClient.runJob(conf);

    return 0;
  }

  static public String hexen(byte[] bs) {
    String o = "";
    for(byte b : bs) o += byteToHex(b) + " ";
    return o;
  }
  
  static public String byteToHex(byte b) {
    // Returns hex String representation of byte b
    char hexDigit[] = {
       '0', '1', '2', '3', '4', '5', '6', '7',
       '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
    };
    char[] array = { hexDigit[(b >> 4) & 0x0f], hexDigit[b & 0x0f] };
    return new String(array);
 }
  
  private static class FilterTextHtmlMapper extends MapReduceBase implements Mapper<Text,BytesWritable,Text,Text> {
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
          
        // strip header off response        
        String httpResponse = new String(v.getBytes(), 0, v.getLength());//, "ISO-8859-1");
        int htmlStartIdx = httpResponse.indexOf("\r\n\r\n"); // ie end of header      
        String html = httpResponse.substring(htmlStartIdx);

        // emit
        String url = headerColumns[COLUMNS.URL.ordinal()];
        String dts = headerColumns[COLUMNS.DTS.ordinal()];
          
        collector.collect(new Text(url+" "+dts), new Text(html));
        
      }      
      catch(Exception e) {        
        reporter.getCounter("FilterTextHtml.exception", e.getClass().getSimpleName()).increment(1);
      }
      
    }   
    
  }
  
}
