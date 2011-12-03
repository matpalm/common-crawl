package cc.udfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.google.common.collect.Lists;

public class Bigrams extends EvalFunc<DataBag>  {
  
  private BagFactory mBagFactory = BagFactory.getInstance();
  private TupleFactory mTupleFactory = TupleFactory.getInstance();
  
  public static void main(String s[]) {           
    System.out.println(new Bigrams().bigrams("this is a test").toString());
  }
  
  public DataBag exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0)
      return null;    
    
    String tokens = (String)input.get(0);    
    return bigrams(tokens);
  }
    
  private DataBag bigrams(String sentence) { 
    DataBag bag = mBagFactory.newDefaultBag();
    String[] tokens = sentence.split(" ");
    for(int i=0; i<tokens.length-1; i++) {
      ArrayList<String> bigram = Lists.newArrayList(tokens[i], tokens[i+1]);
      bag.add(mTupleFactory.newTuple(bigram));      
    }   
    return bag;
  } 
    
}
