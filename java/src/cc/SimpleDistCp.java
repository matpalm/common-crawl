package cc;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

public class SimpleDistCp  extends Configured implements Tool {
  
  private static final String CC_HDFS_PATH = "cc.hdfs_path";

  public static void main(String args[]) throws Exception {
    ToolRunner.run(new SimpleDistCp(), args);
  }
    
  public int run(String[] args) throws Exception {
    if (args.length!=2) {
      throw new RuntimeException("usage: SimpleDistCp <mainfest_dir> <stdout>");
    }    
    final Configuration conf = new Configuration(getConf());    
    if (null == conf.get(CC_HDFS_PATH)) {
      throw new RuntimeException("usage: requires -D "+CC_HDFS_PATH+" for crawl output");
    }
    if (!conf.get(CC_HDFS_PATH).endsWith("/")) {
      throw new RuntimeException("usage: you problaby want -D "+CC_HDFS_PATH+"="+conf.get(CC_HDFS_PATH)+" to end in /");
    }
    
    Job job = new Job(conf);
            
    job.setMapperClass(SimpleDistCpMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    conf.set("mapred.compress.map.output", "true");
    conf.set("mapred.map.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec");    
            
    job.setNumReduceTasks(0);
    
    job.setJobName(this.getClass().getName());
    job.setJarByClass(SimpleDistCp.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));        
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
//    FileOutputFormat.setCompressOutput(job, true);
//    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

    job.waitForCompletion(true);
    return 0;
  }    
  
  private static class SimpleDistCpMapper extends Mapper<LongWritable,Text,Text,Text> {
    
    public void map(LongWritable offset, Text s3key, final Context context) throws IOException, InterruptedException {      
      try {
        
        // report some job progress
        context.setStatus("copying "+s3key.toString());
        System.err.println("copying "+s3key.toString());
        
        // fire up a new client; per 100mb this isn't too bad
        AWSCredentials awsCredentials = new AWSCredentials(
            context.getConfiguration().get("fs.s3n.awsAccessKeyId"), 
            context.getConfiguration().get("fs.s3n.awsSecretAccessKey")
          );    
        RestS3Service s3Service = new RestS3Service(awsCredentials);
        s3Service.setRequesterPaysEnabled(true);
        
        // get input stream (s3)
        S3Object object = s3Service.getObject("commoncrawl-crawl-002", s3key.toString());
        InputStream input = object.getDataInputStream();
        
        // create output stream (hdfs)
        FileSystem fs = FileSystem.get(new Configuration());
        // can't work out how to recursively define path in ExtractVisibleTextFromArc 
        // so for now replace / with _#newb
        String hdfsPath = 
          context.getConfiguration().get(CC_HDFS_PATH) + 
          s3key.toString().replaceAll("/","_"); 
        Path path = new Path(hdfsPath);
        FSDataOutputStream output = fs.create(path, true);
        
        // dead simple copy
        byte[] buffer = new byte[1024 * 1024];
        int read = input.read(buffer);
        while(read!=-1) {
          output.write(buffer, 0, read);
          read = input.read(buffer);
          context.progress();
        }
        
        // clean up
        output.close();
        input.close();
        
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    
  }
  
}
