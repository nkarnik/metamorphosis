package metamorphosis.workers.sources;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;

import metamorphosis.utils.BufferedReaderIterable;
import metamorphosis.utils.s3.S3Exception;
import metamorphosis.utils.s3.S3Util;
import net.sf.json.JSONObject;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.javatuples.Pair;


public class WorkerS3Source extends WorkerSource {
  
  private Logger _log = Logger.getLogger(WorkerS3Source.class);
  private JSONObject _message;
  private String _bucketName;
  private String _shardPath;
  private String _topicToWrite;
  private BufferedReader _bufferedShardReader;
//  private List<String> _shardContents;
  private BufferedReaderIterable _brIterable;

  public WorkerS3Source(JSONObject message) {
    _message = message;
    JSONObject sourceObject = _message.getJSONObject("source");
    JSONObject config = sourceObject.getJSONObject("config");
    _bucketName = config.getString("bucket");
    _shardPath = config.getString("shard_path");
    _topicToWrite = message.getString("topic");
  }

  @Override
  public Pair<File, Iterable<String>> getMessageIterator() {
    Pair<File, BufferedReader> cachedGzipFileReaderPair = S3Util.getCachedGzipFileReader(_bucketName, _shardPath);
    _bufferedShardReader = cachedGzipFileReaderPair.getValue1();
    _brIterable = new BufferedReaderIterable(_bufferedShardReader);

    return new Pair<File, Iterable<String>>(cachedGzipFileReaderPair.getValue0(),_brIterable);
  }
  
  public void shutdown(){
    if(_bufferedShardReader != null){
      try {
        _bufferedShardReader.close();
      } catch (IOException e) {
        _log.error(ExceptionUtils.getStackTrace(e));
      }
    }
  }

  @Override
  public String getTopic() {

    return _topicToWrite;
  }

}
