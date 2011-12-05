package cc;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Compact extends Configured implements Tool {

  public static void main(String args[]) throws Exception {
    ToolRunner.run(new Compact(), args);
  }
    
  public int run(String[] args) throws Exception {
    if (args.length<2) {
      throw new RuntimeException("usage: "+this.getClass().getSimpleName()+" <input1> <input2> ... <inputN> <output>");
    }
    
    JobConf conf = new JobConf(getConf(), getClass());
    
    conf.setJobName(this.getClass().getName());
    
    for(int i=0; i<args.length; i++) {
      if (i!=args.length-1)
        FileInputFormat.addInputPath(conf, new Path(args[i]));
      else
        FileOutputFormat.setOutputPath(conf, new Path(args[i]));
    }
    
    // sequence file input
    conf.setInputFormat(SequenceFileInputFormat.class);
    
    // sequence file output
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);  
    conf.set("mapred.output.compress", "true");
    conf.set("mapred.output.compression.type", "BLOCK");
    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
    
    JobClient.runJob(conf);
    
    return 0;
  }
  
}
