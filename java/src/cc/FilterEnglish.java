package cc;

import java.io.IOException;

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
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tika.language.LanguageIdentifier;

public class FilterEnglish extends Configured implements Tool {

  public static void main(String args[]) throws Exception {
    ToolRunner.run(new FilterEnglish(), args);
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
    
    conf.setNumReduceTasks(0);
    
    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setMapperClass(FilterEnglishMapper.class);    
    
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    
    JobClient.runJob(conf);

    return 0;
  }

  
  public static class FilterEnglishMapper extends MapReduceBase implements Mapper<Text,Text,Text,Text> {
    
    public void map(Text header, Text visibleText, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
   
      try {
        
        String language = new LanguageIdentifier(visibleText.toString()).getLanguage();
        reporter.getCounter("FilterEnglish.language", language).increment(1);        
        
        if ("en".equals(language)) {
          collector.collect(header, visibleText);
        }
        
      }      
      catch(Exception e) {        
        reporter.getCounter("FilterEnglish.exception", e.getClass().getSimpleName()).increment(1);
      }
      
    }   
    
  }
  
}
