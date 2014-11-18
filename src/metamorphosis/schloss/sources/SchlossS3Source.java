package metamorphosis.schloss.sources;

import java.io.IOException;
import java.util.List;

import metamorphosis.utils.s3.S3Exception;
import metamorphosis.utils.s3.S3Util;
import net.sf.json.JSONObject;
import net.sf.json.util.JSONBuilder;
import net.sf.json.util.JSONStringer;

import org.apache.log4j.Logger;
import org.javatuples.Pair;

import com.google.common.collect.Lists;

public class SchlossS3Source extends SchlossSource{

  private JSONObject _message;
  private Logger _log = Logger.getLogger(SchlossS3Source.class);
  public String _bucketName;
  private String _sourceType;
  private String _topicToWrite;
  
  @Deprecated
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
    _workerMessages = Lists.newArrayList();
    try {
      //TODO: Generate manifest here.
      _log.info("Getting worker messages. Reading s3 manifest: " + _manifestPath);
      String[] split = S3Util.readFile(_bucketName, _manifestPath).split("\n");
      _log.info("Manifest length: " + split.length);
      for(String line : split){
        
        Pair<String, String> decomposedPath = S3Util.decomposePath(line);
        String path = decomposedPath.getValue1();
        _log.debug("Path from decomposed is: " + path);
     
        //Build JSON to send as message
        JSONBuilder builder = new JSONStringer();
        builder.object()
          .key("topic").value(_topicToWrite)
          .key("source").object()
            .key("type").value(_sourceType)
            .key("config").object()
              .key("shard_path").value(path)
              .key("bucket").value(_bucketName)
              .key("credentials").object()
                .key("secret").value("")
                .key("access").value("")
              .endObject()
            .endObject()
          .endObject()
        .endObject();

        String workerMessage = builder.toString();
        //_log.info("Build JSON from message: " + workerMessage);
        _workerMessages.add(workerMessage);
      }

    } catch (IOException | S3Exception e) {
      _log.info("Failed to get s3 manifest path: " + _manifestPath);
    }
    // TODO Auto-generated method stub
    return _workerMessages;
  }

  

}
