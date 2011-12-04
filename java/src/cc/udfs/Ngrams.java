package cc.udfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.google.common.collect.Lists;

public class Ngrams extends EvalFunc<DataBag>  {
  
  private BagFactory mBagFactory = BagFactory.getInstance();
  private TupleFactory mTupleFactory = TupleFactory.getInstance();
  
  private int ngramLength;  // for now either 1 or 2
  private boolean distinct; // true if we only care about unique ngrams
  
  public static void main(String s[]) {           
    System.out.println(new Ngrams(1,true).ngrams("this is a is a test").toString());
    System.out.println(new Ngrams(1,false).ngrams("this is a is a test").toString());
    System.out.println(new Ngrams(2,true).ngrams("this is a is a test").toString());
    System.out.println(new Ngrams(2,false).ngrams("this is a is a test").toString());
  }
  
  public Ngrams(int ngramLength, boolean distinct) {
    if (ngramLength!=1 && ngramLength!=2) 
      throw new RuntimeException("only support uni/bigrams");
    this.ngramLength = ngramLength;
    this.distinct = distinct;
  }
  
  public DataBag exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0)
      return null;        
    String tokens = (String)input.get(0);    
    return bagify(ngrams(tokens));
  }
   
  private DataBag bagify(Collection<String> ngrams) {
    DataBag bag = mBagFactory.newDefaultBag();
    for (String ngram : ngrams)
      if (ngramLength==1) 
        bag.add(mTupleFactory.newTuple(ngram));
      else
        bag.add(mTupleFactory.newTuple(Lists.newArrayList(ngram.split(" "))));
    return bag;
  }
  
  private Collection<String> ngrams(String sentence) {     
    Collection<String> result = distinct ? new HashSet<String>() : new ArrayList<String>();   
    String[] tokens = sentence.split(" ");
    if (ngramLength==1)
      for(String token : tokens)      
        result.add(token);      
    else // assume bigrams
      for(int i=0; i<tokens.length-1; i++)        
        result.add(tokens[i]+" "+tokens[i+1]);      
    return result;
  } 
    
}
