package metamorphosis.schloss.sinks;

import java.util.List;

import net.sf.json.JSONObject;

import com.google.common.collect.Lists;

public class SchlossS3Sink extends SchlossSink {

  private JSONObject _message;
  public SchlossS3Sink(JSONObject message){
    _message = message;
  }

  /**
   * For now, return the same messages
   */
  @Override
  public List<String> getWorkerMessages() {
    
    return Lists.newArrayList(_message.toString());
  }

}
