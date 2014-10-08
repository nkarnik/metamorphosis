package metamorphosis.workers.sources;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import metamorphosis.utils.s3.S3Util;
import metamorphosis.workers.ShardProducerService;
import net.sf.json.JSONObject;


public class WorkerS3Source implements WorkerSource {
  
  private Logger _log = Logger.getLogger(WorkerS3Source.class);
  private JSONObject _message;
  private String _bucketName;
  private String _shardPath;
  private String _topicToWrite;
  private String _sourceType;
  private List<String> _shardContents;

  public WorkerS3Source(JSONObject message) {
    _message = message;
    JSONObject sourceObject = _message.getJSONObject("source");
    JSONObject config = sourceObject.getJSONObject("config");
    _bucketName = config.getString("bucket");
    _shardPath = config.getString("manifest");
    _topicToWrite = message.getString("topic");
    _sourceType = sourceObject.getString("type");
    _shardContents = null ;
    try {
      _shardContents = Lists.newArrayList(S3Util.readGzipFile(_bucketName, _shardPath).split("\n"));
      _log.info("First manifest object is " + _shardContents.get(0));
    } catch (IOException e) {
      _log.info("Failed to get s3 manifest path: " + _shardPath);
    }
    
  }

  @Override
  public List<String> getMessages() {

    return _shardContents;
  }

  @Override
  public String getTopic() {

    return _topicToWrite;
  }

}
