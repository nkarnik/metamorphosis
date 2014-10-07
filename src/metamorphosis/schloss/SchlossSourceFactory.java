package metamorphosis.schloss;

import org.apache.commons.lang.NotImplementedException;

import metamorphosis.schloss.sources.SchlossS3Source;
import metamorphosis.schloss.sources.SchlossSource;
import net.sf.json.JSONObject;

public class SchlossSourceFactory {

  public static SchlossSource createSource(JSONObject message) {
    String type = message.getJSONObject("source").getString("type");
    SchlossSource schlossSource = null;
    switch(type){
    case "s3":
      schlossSource = new SchlossS3Source(message);
      break;
    case "kinesis":
      
      break;
     default:
      throw new NotImplementedException("Cannot handle source of type: " + type); 

    }
    return schlossSource;
  }

  
}
