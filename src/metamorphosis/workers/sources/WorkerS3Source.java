package metamorphosis.workers.sources;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;

import metamorphosis.utils.BufferedReaderIterable;
import metamorphosis.utils.s3.S3Exception;
import metamorphosis.utils.s3.S3Util;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.google.common.collect.Lists;


public class WorkerS3Source implements WorkerSource {
  
  private Logger _log = Logger.getLogger(WorkerS3Source.class);
  private JSONObject _message;
  private String _bucketName;
  private String _shardPath;
  private String _topicToWrite;
  private String _sourceType;
  private BufferedReader _bufferedShardReader;
//  private List<String> _shardContents;
  private BufferedReaderIterable _brIterable;

  public WorkerS3Source(JSONObject message) {
    _message = message;
    JSONObject sourceObject = _message.getJSONObject("source");
    JSONObject config = sourceObject.getJSONObject("config");
    _bucketName = config.getString("bucket");
    _shardPath = config.getString("manifest");
    _topicToWrite = message.getString("topic");
    _sourceType = sourceObject.getString("type");
    
  }

  @Override
  public Iterable<String> getMessageIterator() {
    
    try {
      _bufferedShardReader = S3Util.getCachedGzipFileReader(_bucketName, _shardPath);
      _brIterable = new BufferedReaderIterable(_bufferedShardReader);
      
    } catch (IOException | InterruptedException | S3Exception e) {
      _log.info("Failed to get s3 manifest path: " + _shardPath);
    }  
    return _brIterable;
  }

  @Override
  public String getTopic() {

    return _topicToWrite;
  }

}
