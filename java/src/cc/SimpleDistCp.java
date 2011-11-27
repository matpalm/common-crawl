package cc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.security.AWSCredentials;

public class SimpleDistCp  extends Configured implements Tool {
  
  private static final String CC_HDFS_PATH = "cc.hdfs_path";
  private static final int MAX_RETRIES = 10;
  
  public static void main(String args[]) throws Exception {
    ToolRunner.run(new SimpleDistCp(), args);
  }
    
  public int run(String[] args) throws Exception {
    if (args.length!=2) {
      throw new RuntimeException("usage: SimpleDistCp <mainfest_dir> <hdfs_output_path>");
    }        
    
    String hdfsPath = args[1];
    if (!hdfsPath.endsWith("/")) {
      hdfsPath += "/";
    }
    getConf().set(CC_HDFS_PATH, hdfsPath);
    
    final Configuration conf = new Configuration(getConf());    
    
    Job job = new Job(conf);
    
    job.setMapperClass(SimpleDistCpMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    conf.set("mapred.output.compress", "true");
    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");    
    conf.set("mapred.map.tasks.speculative.execution", "false");
    job.setNumReduceTasks(0);
    
    job.setJobName(this.getClass().getName());
    job.setJarByClass(SimpleDistCp.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));        
    FileOutputFormat.setOutputPath(job, new Path("SimpleDistCp.out"));

    job.waitForCompletion(true);
    return 0;
  }    
  
  private static class SimpleDistCpMapper extends Mapper<LongWritable,Text,Text,Text> {
    
    public void map(LongWritable offset, Text s3key, final Context context) throws IOException, InterruptedException {      
        
      int attempts = 0;
      
      while(attempts < MAX_RETRIES) {
        try {
          context.setStatus("copying "+s3key.toString()+" attempts="+attempts);
          System.err.println("copying "+s3key.toString()+" attempts="+attempts);

          RestS3Service s3Client = newS3Client(context);
          
          InputStream s3object = s3Client.getObject("commoncrawl-crawl-002", s3key.toString()).getDataInputStream();        
          
          FSDataOutputStream hdfsFile = createHdfsFileFor(s3key, context);
          
          copy(s3object, hdfsFile, context);          
                    
          s3Client.shutdown();
        } 
        catch (Exception e) {
          context.getCounter("exception", e.getClass().getSimpleName()).increment(1);
          attempts++;
          try { Thread.sleep(attempts*1000); } catch (InterruptedException ignore) { }
          context.progress();
        }          
      }
      
    }

    private FSDataOutputStream createHdfsFileFor(Text s3key, final Context context) throws IOException {    
      String hdfsPath = context.getConfiguration().get(CC_HDFS_PATH) + s3key.toString();       
      return FileSystem.get(new Configuration()).create(new Path(hdfsPath), (short)2);      
    }

    private RestS3Service newS3Client(final Context context) throws S3ServiceException {
      AWSCredentials awsCredentials = new AWSCredentials(
          context.getConfiguration().get("fs.s3n.awsAccessKeyId"), 
          context.getConfiguration().get("fs.s3n.awsSecretAccessKey")
      );    
      
      RestS3Service s3Service = new RestS3Service(awsCredentials);
      s3Service.setRequesterPaysEnabled(true);
      
      return s3Service;
    }
    
    private void copy(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
      final ReadableByteChannel input  = Channels.newChannel(inputStream);
      final WritableByteChannel output = Channels.newChannel(outputStream);        
      ByteBuffer buffer = ByteBuffer.allocateDirect(64 * 1024);
      while (input.read(buffer) != -1) {
        buffer.flip();
        output.write(buffer);
        buffer.compact();
        context.progress();
      }
      buffer.flip();
      while (buffer.hasRemaining()) {
        output.write(buffer);
       }
      inputStream.close();
      outputStream.close();      
    }
    
  }
  
}
