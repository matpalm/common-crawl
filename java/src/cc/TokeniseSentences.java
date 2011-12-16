package cc;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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

import cc.util.SentenceTokeniser;

public class TokeniseSentences extends Configured implements Tool {

  private static final int MIN_TOKENS_IN_SENTENCE = 3;
  
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
    conf.set("mapred.output.compression.type", "BLOCK");
    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
    
    conf.setMaxMapTaskFailuresPercent(100);
    conf.setNumReduceTasks(0);
    
    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setMapperClass(TokeniseSentencesMapper.class);    
    
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    
    JobClient.runJob(conf);

    return 0;
  }

  
  public static class TokeniseSentencesMapper extends MapReduceBase implements Mapper<Text,Text,Text,Text> {
        
    private SentenceTokeniser sentenceTokeniser = new SentenceTokeniser();
    
    public void map(Text header, Text visibleText, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
      
      try {
        Set<String> seenSentences = new HashSet<String>();
        
        int sentencesEmitted = 0;
        int sentencesTooShort = 0;
        int duplicateSentences = 0;

        // a double nested for with a double nested if!
        // i.. am.. going.. straight.. to.. hell :/
        int paragraphIdx = 0;
        int sentenceInParagraphIdx = 0;
        for (String paragraph : visibleText.toString().split("\n")) {
          try {
            sentenceInParagraphIdx = 0;
            for(String sentence : sentenceTokeniser.extractSentences(paragraph)) {
              int numTokens = sentence.split(" ").length;
              if (numTokens >= MIN_TOKENS_IN_SENTENCE) {
                if (seenSentences.contains(sentence)) {
                  duplicateSentences++;
                }
                else {
                  collector.collect(
                      new Text(header.toString()+"\t"+paragraphIdx+"\t"+sentenceInParagraphIdx), 
                      new Text(sentence)
                  );
                  sentencesEmitted++;
                  seenSentences.add(sentence);
                  sentenceInParagraphIdx++;
                }
              }
              else {
                sentencesTooShort++;
              }
            }
            if (sentenceInParagraphIdx != 0) // ie there was at least one sentence
              paragraphIdx++;
          }
          catch(Exception e) {        
            reporter.getCounter("TokeniseSentences.tokenise.exception", e.getClass().getSimpleName()).increment(1);
          }          
        }
        
        reporter.getCounter("TokeniseSentences", "duplicate_sentences").increment(duplicateSentences);
        reporter.getCounter("TokeniseSentences", "num_sentences_too_short").increment(sentencesTooShort);
        reporter.getCounter("TokeniseSentences", "num_input_records").increment(1);
        reporter.getCounter("TokeniseSentences", "num_sentences_emitted").increment(sentencesEmitted);
        
      }      
      catch(Exception e) {        
        reporter.getCounter("TokeniseSentences.general.exception", e.getClass().getSimpleName()).increment(1);
      }
      
    }   
    
  }
  
}
