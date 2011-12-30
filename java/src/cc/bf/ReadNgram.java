package cc.bf;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

public class ReadNgram {
  
  public static void main(String s[]) throws IOException {
    Configuration conf = new Configuration();
    String filename = "bfngrams/out/part-00000";
    FileSystem fs = FileSystem.get(URI.create(filename), conf);
    Path path = new Path(filename);
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
    NullWritable nullKey = NullWritable.get();
    BloomFilter bloomFilter = new BloomFilter();
    reader.next(nullKey, bloomFilter);
    reader.close();
    
    System.out.println(bloomFilter.toString());
    
    String[] egs = {
        "activities other",
        "membership organizations",
        "organizations elsewhere",
        "4 0",
        "elsewhere classified",
        "other membership",
        "0 activities",
        "20091128093155 4"
    };
    
    for(String eg : egs) {
      Key k = new Key(eg.getBytes());
      System.out.println(eg+"\t"+bloomFilter.membershipTest(k));
    }
    
  }
  
}
