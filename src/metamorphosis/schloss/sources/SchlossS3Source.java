package metamorphosis.schloss.sources;

import java.io.IOException;
import java.util.List;

import metamorphosis.utils.s3.S3Util;
import net.sf.json.JSONObject;
import net.sf.json.util.JSONBuilder;
import net.sf.json.util.JSONStringer;

import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

public class SchlossS3Source implements SchlossSource{

  private JSONObject _message;
  private Logger _log = Logger.getLogger(SchlossS3Source.class);
  public String _bucketName;
  private String _sourceType;
  private String _topicToWrite;
  private String _manifestPath;
  private List<String> _workerMessages;
  
  
  public SchlossS3Source(JSONObject message) {
    _message = message;
    JSONObject sourceObject = _message.getJSONObject("source");
    JSONObject config = sourceObject.getJSONObject("config");
    _bucketName = config.getString("bucket");
    _manifestPath = config.getString("manifest");
    _topicToWrite = message.getString("topic");
    _sourceType = sourceObject.getString("type");
    
  }


  @Override
  public List<String> getWorkerMessages() {
    _workerMessages = null ;
    try {
      String[] split = S3Util.readFile(_bucketName, _manifestPath).split("\n");
      for(String line : split){
     
        //Build JSON to send as message
        JSONBuilder builder = new JSONStringer();
        builder.object()
        .key("topic").value(_topicToWrite)
        .key("source").object()
            .key("type").value(_sourceType)
            .key("config").object()
              .key("manifest").value(line)
              .key("bucket").value(_bucketName)
              .key("credentials").object()
                .key("secret").value("")
                .key("access").value("")
              .endObject()
            .endObject()
          .endObject()
        .endObject();

        String workerMessage = builder.toString();
        _workerMessages.add(workerMessage);
      }

    } catch (IOException e) {
      _log.info("Failed to get s3 manifest path: " + _manifestPath);
    }
    // TODO Auto-generated method stub
    return _workerMessages;
  }

  

}
