package cc;

import java.io.IOException;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.tools.arc.ArcInputFormat;
import org.apache.tika.language.LanguageIdentifier;

import de.l3s.boilerpipe.extractors.ExtractorBase;
import de.l3s.boilerpipe.extractors.KeepEverythingWithMinKWordsExtractor;

public class ExtractVisibleEnglishText extends Configured implements Tool {

  public static void main(String args[]) throws Exception {
    ToolRunner.run(new ExtractVisibleEnglishText(), args);
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
    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
    
    conf.setNumReduceTasks(0);
    
    conf.setInputFormat(ArcInputFormat.class);
    conf.setMapperClass(ExtractVisibleEnglishTextMapper.class);    
    
    FileInputFormat.addInputPaths(conf, args[0]);
    FileOutputFormat.setOutputPath(conf, new Path(args[1]+"/"+System.currentTimeMillis()));

    JobClient.runJob(conf);

    return 0;
  }

  
  private static class ExtractVisibleEnglishTextMapper extends MapReduceBase implements Mapper<Text,BytesWritable,Text,Text> {
    enum COLUMNS { URL, IP, DTS, MIME_TYPE, SIZE };    
    
    private ExtractorBase extractor = new KeepEverythingWithMinKWordsExtractor(5);
    
    public void map(Text k, BytesWritable v, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
   
      try {
        String headerColumns[] = k.toString().split(" ");      
        if (headerColumns.length != COLUMNS.values().length) {
          System.err.println("dodgy header row? ["+k+"]");
          reporter.getCounter("arc_processor", "dodgy_header").increment(1);
          return;
        }
        
        String mime_type = headerColumns[COLUMNS.MIME_TYPE.ordinal()];
        reporter.getCounter("mime_types", mime_type).increment(1);        
        if (!"text/html".equals(mime_type)) {
          return;
        }
          
        // strip header off response
        String httpResponse = new String(v.getBytes(), 0, v.getLength(), "UTF8");
        int htmlStartIdx = httpResponse.indexOf("\r\n\r\n"); // ie end of header      
        String html = httpResponse.substring(htmlStartIdx);

        // pass through parser
        String visibleText = extractor.getText(html).trim();        
        if (visibleText.isEmpty()) {
          reporter.getCounter("parse", "no_content").increment(1);
          return;
        }
        if (visibleText.indexOf(" ")==-1) {
          reporter.getCounter("parse", "no_spaces_in_text").increment(1);
          return;
        }
        
        // filter english
        String language = new LanguageIdentifier(visibleText).getLanguage();
        reporter.getCounter("language", language).increment(1);        
        if (!"en".equals(language)) {
          return;
        }
        
        // emit
        String url = headerColumns[COLUMNS.URL.ordinal()];
        String dts = headerColumns[COLUMNS.DTS.ordinal()];        
//        visibleText = visibleText.replaceAll("\\s+"," ");    
        collector.collect(new Text(url+" "+dts), new Text(visibleText));
        
      }      
      catch(Exception e) {        
        reporter.getCounter("exception", e.getClass().getSimpleName()).increment(1);
      }
      catch(StackOverflowError so) {
        // neko html parser (?)
        reporter.getCounter("exception", "stack_overflow (neko?)").increment(1);        
      }
      
    }   
    
  }
  
}
