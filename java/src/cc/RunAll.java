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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.tools.arc.ArcInputFormat;

import cc.ExtractVisibleText.ExtractVisibleTextMapper;
import cc.FilterEnglish.FilterEnglishMapper;
import cc.FilterTextHtml.FilterTextHtmlMapper;
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
    conf.setJobName(getClass().getName());
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.set("mapred.output.compress", "true");
    conf.set("mapred.output.compression.type", "BLOCK");
    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

    conf.setMaxMapTaskFailuresPercent(100);
    
    conf.setInputFormat(ArcInputFormat.class);

    for(int i=0; i<args.length; i++) {
      if (i!=args.length-1)
        FileInputFormat.addInputPath(conf, new Path(args[i]));
      else
        FileOutputFormat.setOutputPath(conf, new Path(args[i]));
    }
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    
    JobConf mapAConf = new JobConf(false);
    ChainMapper.addMapper(conf, FilterTextHtmlMapper.class, 
        Text.class, BytesWritable.class,
        Text.class, Text.class, true, mapAConf);

    JobConf mapBConf = new JobConf(false);
    ChainMapper.addMapper(conf, ExtractVisibleTextMapper.class, 
        Text.class, Text.class,
        Text.class, Text.class, true, mapBConf);
    
    JobConf mapCConf = new JobConf(false);
    ChainMapper.addMapper(conf, FilterEnglishMapper.class, 
        Text.class, Text.class,
        Text.class, Text.class, true, mapCConf);
    
    JobConf mapDConf = new JobConf(false);
    ChainMapper.addMapper(conf, TokeniseSentencesMapper.class, 
        Text.class, Text.class,
        Text.class, Text.class, true, mapDConf);
        
    JobClient.runJob(conf);

    return 0;
  }  
}
