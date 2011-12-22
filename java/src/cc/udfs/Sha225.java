package cc.udfs;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.tools.pigstats.PigStatusReporter;

public class Sha225 extends EvalFunc<String>  {
  
  public Sha225() {      
  }  

  public String exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0)
      return null;
    try {
      return digest((String)input.get(0));
    }
    catch (Exception e) {
      System.err.println("Sha225Exception "+e.getClass().getName());
      if (PigStatusReporter.getInstance() != null) {
        PigStatusReporter.getInstance().getCounter("Sha225Exception",e.getClass().getName()).increment(1);
      }
      return null;
    }
  }

  private String digest(String str) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    md.update(str.getBytes());
    return new BigInteger(1, md.digest()).toString(16);
  }
  
}
