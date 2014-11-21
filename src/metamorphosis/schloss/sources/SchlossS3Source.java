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
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.model.S3Object;

import com.google.common.collect.Lists;

public class SchlossS3Source extends SchlossSource{

  private JSONObject _message;
  private Logger _log = Logger.getLogger(SchlossS3Source.class);
  private String _bucketName;
  private String _shardPath;
  private String _shardPrefix;
  private String _sourceType;
  private String _topicToWrite;

  private List<String> _workerMessages;
  
  
  public SchlossS3Source(JSONObject message) {
    _message = message;
    _topicToWrite = message.getString("topic");
    JSONObject sourceObject = _message.getJSONObject("source");
    _sourceType = sourceObject.getString("type");
    JSONObject config = sourceObject.getJSONObject("config");
    _bucketName = config.getString("bucket");
    _shardPath = config.getString("shard_path");
    _shardPrefix = config.getString("shard_prefix");
    
    
  }


  @Override
  public List<String> getWorkerMessages() {
    _workerMessages = Lists.newArrayList();
    try {

      _log.info("Getting worker messages. Reading shard list from bucket: " + _bucketName + " pathPrefix: " + _shardPath);
//      String[] split = S3Util.readFile(_bucketName, _manifestPath).split("\n");
      S3Object[] s3Objects = S3Util.listPath(_bucketName, _shardPath);
      
      _log.info("Found shards: " + s3Objects.length);
      for(S3Object s3Object : s3Objects){

        String path = s3Object.getKey();
        
        if(!path.contains(_shardPrefix)){
          continue; //hack check for the file prefix
        }
        
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

    } catch (S3ServiceException e) {
      _log.info("Failed to list path with bucket: " + _bucketName + " pathPrefix: " + _shardPath);
    }
    // TODO Auto-generated method stub
    return _workerMessages;
  }

  

}
