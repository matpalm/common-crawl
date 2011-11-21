package cc;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.SkipBadRecords;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import de.l3s.boilerpipe.extractors.ExtractorBase;
import de.l3s.boilerpipe.extractors.KeepEverythingWithMinKWordsExtractor;

public class ExtractVisibleText extends Configured implements Tool {

  public static void main(String args[]) throws Exception {
    ToolRunner.run(new ExtractVisibleText(), args);
  }
    
  public int run(String[] args) throws Exception {
        
    if (args.length!=2) {
      throw new RuntimeException("usage: "+this.getClass().getSimpleName()+" <input> <output>");
    }
    
    JobConf conf = new JobConf(getConf(), getClass());
    
    conf.setJobName(this.getClass().getName());    
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);    
    conf.setMapperClass(ExtractVisibleTextMapper.class);
    
    conf.setInputFormat(KeyValueTextInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    
    FileInputFormat.addInputPath(conf, new Path(args[0]));            
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    // turn up attempts and enable skipping
    conf.set("mapred.map.max.attempts", "30");
    conf.set("mapred.skip.map.max.skip.records", "1000");

    // sequence file output
    conf.set("mapred.output.compress", "true");
    conf.set("mapred.output.compression.type", "BLOCK");
    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

    conf.setNumReduceTasks(0);
    
    JobClient.runJob(conf);
    
    return 0;
  }

  
  private static class ExtractVisibleTextMapper extends MapReduceBase implements Mapper<Text,Text, Text,Text> {

    private ExtractorBase extractor = new KeepEverythingWithMinKWordsExtractor(10); 

    public void map(Text url_dts, Text html, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {

      try {
        
        long start = System.currentTimeMillis();
        String visibleText = extractor.getText(html.toString()).trim();
        long totalTime = System.currentTimeMillis() - start;

        if (totalTime<1000) {
          reporter.getCounter("extractor_time", "< 1s").increment(1); 
        } else if (totalTime<10000) {
          reporter.getCounter("extractor_time", "< 10s").increment(1);           
        } else if (totalTime<100000) {
          reporter.getCounter("extractor_time", "< 100s").increment(1); 
        } else {
          reporter.getCounter("extractor_time", ">= 100s").increment(1);
        }
        
        if (visibleText.indexOf(" ") == -1) {
          reporter.getCounter("parser", "no_spaces_in_visible_text").increment(1);      
          return;
        }
        
        collector.collect(new Text(url_dts), new Text(visibleText));
      }
      catch(StackOverflowError so) {
        // neko html parser (?)
        reporter.getCounter("exception", "stack_overflow").increment(1);        
      }
      catch(OutOfMemoryError oom) {   
        // tread super carefully catching with one!
        reporter.getCounter("exception", "oom").increment(1);
        extractor = new KeepEverythingWithMinKWordsExtractor(10); 
      }      
      catch(Exception e) {        
        reporter.getCounter("exception", "exception_"+e.getClass().getName()).increment(1);
      }
      
    }
    
  }
  
}
