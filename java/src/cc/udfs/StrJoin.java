package cc.udfs;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.tools.pigstats.PigStatusReporter;

public class StrJoin extends EvalFunc<String>  {

  public StrJoin() {
  }  

  public String exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0)
      return null;
    try {
      DataBag bag = (DataBag)input.get(0);
      StringBuilder sb = new StringBuilder();    
      for(Iterator<Tuple> iter=bag.iterator(); iter.hasNext();) {
        Tuple next = iter.next(); 
        sb.append(next.get(0).toString());
        if (iter.hasNext())
          sb.append(" ");
      }       
      return sb.toString();
    }
    catch (Exception e) {
      System.err.println("StrJoin_Exception "+e.getClass().getName());
      if (PigStatusReporter.getInstance() != null) {
        PigStatusReporter.getInstance().getCounter("StrJoin_Exception",e.getClass().getName()).increment(1);
      }
      return null;
    }
  }

}
