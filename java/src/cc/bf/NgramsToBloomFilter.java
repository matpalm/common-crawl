package cc.bf;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

public class NgramsToBloomFilter extends Configured implements Tool {

  private static final String BF_SIZE       = "BF_SIZE";
  private static final String BF_NUM_HASHES = "BF_NUM_HASHES";
  private static final String BF_HASH_TYPE  = "BF_HASH_TYPE";

  private static final int BF_DFT_SIZE       = 1000;
  private static final int BF_DFT_NUM_HASHES = 10;
  private static final int BF_DFT_HASH_TYPE  = Hash.JENKINS_HASH;
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new NgramsToBloomFilter(), args);    
  }
  
  public int run(String[] args) throws Exception {
    
    if (args.length!=2) {
      throw new RuntimeException("usage: "+getClass().getName()+" <input> <output>");
    }
    
    JobConf conf = new JobConf(getConf(), getClass());
    conf.setJobName(getClass().getName());
    
    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    conf.setMapperClass(BFMapper.class);

    conf.setReducerClass(BFReducer.class);        
    
    conf.setMapOutputKeyClass(NullWritable.class);
    conf.setMapOutputValueClass(BloomFilter.class);       
    conf.setOutputKeyClass(NullWritable.class);
    conf.setOutputValueClass(BloomFilter.class);       

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        
    JobClient.runJob(conf);
    return 0;
  } 

  public static class BFBase extends MapReduceBase {
    private static final NullWritable nullKey = NullWritable.get();
    protected BloomFilter bloomFilter;
    protected OutputCollector<NullWritable, BloomFilter> collector;
    
    public void configure(JobConf conf) {
      super.configure(conf);
      bloomFilter = new BloomFilter(
          conf.getInt(BF_SIZE, BF_DFT_SIZE), 
          conf.getInt(BF_NUM_HASHES, BF_DFT_NUM_HASHES), 
          conf.getInt(BF_HASH_TYPE, BF_DFT_HASH_TYPE));
    }
    
    public void close() throws IOException {
      if (collector!=null) {
        collector.collect(nullKey, bloomFilter);
      }
      System.err.println("TESTTTTTTTTTTTTTTT [organizations elsewhere] "+
          bloomFilter.membershipTest(new Key("organizations elsewhere".getBytes())));
      super.close();
    }
    
  }
  
  public static class BFMapper extends BFBase implements Mapper<Text,DoubleWritable,NullWritable,BloomFilter> {   
        
    public void map(Text key, DoubleWritable value, OutputCollector<NullWritable, BloomFilter> collector, Reporter reporter) throws IOException {
      System.err.println("MAP COLLECT k=["+key+"] v=["+value+"]");
      if (this.collector==null) {
        this.collector = collector; 
      }      
           
      byte[] buff = new byte[key.getLength()];
      System.arraycopy(key.getBytes(), 0, buff, 0, buff.length);
      bloomFilter.add(new Key(buff, value.get()));
    }
           
  }

  public static class BFReducer extends BFBase implements Reducer<NullWritable,BloomFilter,NullWritable,BloomFilter> {
        
    public void reduce(NullWritable nullKey, Iterator<BloomFilter> values, OutputCollector<NullWritable, BloomFilter> collector, Reporter reporter) throws IOException {
      if (this.collector==null) {
        this.collector = collector; 
      }     
      
      while(values.hasNext()) {
        bloomFilter.or(values.next());        
      }        
    }
    
  }
  
}
