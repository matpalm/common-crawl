package cc;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import cc.ExtractVisibleText.ExtractVisibleTextMapper;
import cc.FilterEnglish.FilterEnglishMapper;
import cc.TokeniseSentences.TokeniseSentencesMapper;

public class RunAll extends Configured implements Tool {

  public static void main(String args[]) throws Exception {
    ToolRunner.run(new RunAll(), args);
  }
  
  public int run(String[] args) throws Exception {
    
    if (args.length!=2) {
      throw new RuntimeException("usage: "+getClass().getName()+" <input1> <input2> ... <inputN> <output>");
    }
    
    JobConf conf = new JobConf(getConf(), getClass());
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.set("mapred.output.compress", "true");
    conf.set("mapred.output.compression.type", "BLOCK");
    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");

    conf.setMaxMapTaskFailuresPercent(100);
   
    String jobName = getClass().getName() + " ";
    for(int i=0; i<args.length; i++) {
      if (i!=args.length-1) {
        FileInputFormat.addInputPath(conf, new Path(args[i]));
        jobName += args[i] + " ";
      }
      else
        FileOutputFormat.setOutputPath(conf, new Path(args[i]));
    }
    conf.setJobName(jobName);
    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    
//    ChainMapper.addMapper(conf, FilterTextHtmlMapper.class, 
//        Text.class, BytesWritable.class,
//        Text.class, Text.class, true, new JobConf(false));

    ChainMapper.addMapper(conf, ExtractVisibleTextMapper.class, 
        Text.class, Text.class,
        Text.class, Text.class, true, new JobConf(false));
    
    ChainMapper.addMapper(conf, FilterEnglishMapper.class, 
        Text.class, Text.class,
        Text.class, Text.class, true, new JobConf(false));
    
    ChainMapper.addMapper(conf, TokeniseSentencesMapper.class, 
        Text.class, Text.class,
        Text.class, Text.class, true, new JobConf(false));
        
    JobClient.runJob(conf);

    return 0;
  }  
}
