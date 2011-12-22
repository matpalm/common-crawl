package cc.udfs;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.tools.pigstats.PigStatusReporter;

public class TopLevelDomain extends EvalFunc<String>  {
    
  public TopLevelDomain() {      
  }  

  public String exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0)
      return null;
    try {
      return topLevelDomain((String)input.get(0));
    }
    catch (Exception e) {
      System.err.println("TopLevelDomain "+e.getClass().getName());
      if (PigStatusReporter.getInstance() != null) {
        PigStatusReporter.getInstance().getCounter("TopLevelDomain",e.getClass().getName()).increment(1);
      }
      return null;
    }
  }

  private String topLevelDomain(String url) throws Exception {
    if (!url.startsWith("http://"))
      throw new Exception("doesnt start with http");
    
    url = url.replaceFirst("^http://","");

    int firstSlashIdx = url.indexOf("/");
    if (firstSlashIdx!=-1) {
      url = url.substring(0, firstSlashIdx);
    }

    return url;
  }    
  
}
