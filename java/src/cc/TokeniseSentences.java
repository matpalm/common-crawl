package cc;

import java.io.IOException;
import java.util.List;

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
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.tools.arc.ArcInputFormat;
import org.apache.tika.language.LanguageIdentifier;

import cc.util.SentenceTokeniser;

public class TokeniseSentences extends Configured implements Tool {

  public static void main(String args[]) throws Exception {
    ToolRunner.run(new TokeniseSentences(), args);
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
    
    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setMapperClass(TokeniseSentencesMapper.class);    
    
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    
    JobClient.runJob(conf);

    return 0;
  }

  
  private static class TokeniseSentencesMapper extends MapReduceBase implements Mapper<Text,Text,Text,Text> {
    
    private SentenceTokeniser sentenceTokeniser = new SentenceTokeniser();
    
    public void map(Text url_dts, Text visibleText, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
      
      try {
        
        int sentenceIdx = 0;       
        for (String chunk : visibleText.toString().split("\n")) {          
          try {
            for(String sentence : sentenceTokeniser.extractSentences(chunk)) {
              if (sentence.split(" ").length >= 3) {
                System.err.println("chunk = "+chunk);
                collector.collect(new Text(url_dts.toString()+" "+(sentenceIdx++)), new Text(sentence));
              }
              else {
                reporter.getCounter("TokeniseSentences", "sentence_too_short").increment(1);
              }
            }
          }
          catch(Exception e) {        
            reporter.getCounter("TokeniseSentences.tokenise.exception", e.getClass().getSimpleName()).increment(1);
          }
          
        }
        
      }      
      catch(Exception e) {        
        reporter.getCounter("TokeniseSentences.general.exception", e.getClass().getSimpleName()).increment(1);
      }
      
    }   
    
  }
  
}
