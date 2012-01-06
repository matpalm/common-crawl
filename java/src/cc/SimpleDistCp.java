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
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.MultithreadedMapRunner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.amazonaws.Request;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.handlers.AbstractRequestHandler;
import com.amazonaws.services.s3.AmazonS3Client;

public class SimpleDistCp extends Configured implements Tool {
  
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
    
    JobConf conf = new JobConf(getConf(), getClass());    
    conf.setJobName(getClass().getName());
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.set("mapred.output.compress", "true");
    conf.set("mapred.output.compression.type", "BLOCK");
    conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

    conf.set("mapred.map.tasks.speculative.execution", "false");

    conf.setNumReduceTasks(0);
    
    conf.setMapperClass(SimpleDistCpMapper.class);    
    conf.setMapRunnerClass(MultithreadedMapRunner.class);    
    
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    
    JobClient.runJob(conf);

    return 0;    
  }    
  
  private static class SimpleDistCpMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text> {
        
    private String ccHdfsPath, awsAccessKeyId, awsSecretAccessKey;
    private FileSystem filesystem;
    
    public void configure(JobConf job) { 
      super.configure(job);
      ccHdfsPath = job.get(CC_HDFS_PATH);
      awsAccessKeyId = job.get("fs.s3n.awsAccessKeyId");
      awsSecretAccessKey = job.get("fs.s3n.awsSecretAccessKey");
      try {
        filesystem = FileSystem.get(new Configuration());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    
    public void map(LongWritable k, Text s3key, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {
        
      int attempts = 0;      
      while(attempts < MAX_RETRIES) {
        
        InputStream s3object = null;
        FSDataOutputStream hdfsFile = null;
        
        try {
          String logmsg = Thread.currentThread().getName()+
                          " copying "+s3key.toString()+
                          " to "+hdfsPathForKey(s3key).toString()+
                          " attempts="+attempts;
          reporter.setStatus(logmsg);
          System.err.println(logmsg);
         
          AmazonS3Client s3Client = newS3Client();          
          s3object = s3Client.getObject("commoncrawl-crawl-002", s3key.toString()).getObjectContent();                  
          hdfsFile = createHdfsFileFor(s3key);   
          
          copy(s3object, hdfsFile, reporter);                              
          
          s3object.close();
          hdfsFile.close();
          s3Client.shutdown();
          
          return;
        } 
        catch (Exception e) {
          // clean up attempt
          if (s3object!=null) s3object.close();
          if (hdfsFile!=null) hdfsFile.close();              
          filesystem.delete(hdfsPathForKey(s3key), true);

          // report
          e.printStackTrace();
          reporter.getCounter("exception", e.getClass().getName()).increment(1);
          attempts++;
          try { Thread.sleep(attempts*1000); } catch (InterruptedException ignore) { }
          reporter.progress();
        }          
      }
      reporter.setStatus("failed to download "+s3key.toString());
      System.err.println("failed to download "+s3key.toString());
      reporter.getCounter("error","exceeded_max_attempts_to_dload").increment(1);
    }

    private FSDataOutputStream createHdfsFileFor(Text s3key) throws IOException {    
      return filesystem.create(hdfsPathForKey(s3key), (short)2);      
    }

    private Path hdfsPathForKey(Text s3key) {
      return new Path(ccHdfsPath + s3key.toString());     
    }
    
    private AmazonS3Client newS3Client() {      
      AmazonS3Client s3Client = new AmazonS3Client(new BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey));
      s3Client.addRequestHandler(new AbstractRequestHandler() {
        public void beforeRequest(Request<?> request) {
          request.addHeader("x-amz-request-payer", "requester");
        }
      });      
      return s3Client;
    }
    
    private void copy(InputStream inputStream, OutputStream outputStream, Reporter reporter) throws IOException {
      final ReadableByteChannel input  = Channels.newChannel(inputStream);
      final WritableByteChannel output = Channels.newChannel(outputStream);        
      ByteBuffer buffer = ByteBuffer.allocateDirect(64 * 1024);
      while (input.read(buffer) != -1) {
        buffer.flip();
        output.write(buffer);
        buffer.compact();
        reporter.progress();
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
