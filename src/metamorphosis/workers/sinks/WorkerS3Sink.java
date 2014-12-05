package metamorphosis.workers.sinks;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPOutputStream;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.message.MessageAndMetadata;
import metamorphosis.utils.Config;
import metamorphosis.utils.KafkaUtils;
import metamorphosis.utils.s3.S3Exception;
import metamorphosis.utils.s3.S3Util;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.google.common.base.Joiner;

public class WorkerS3Sink extends WorkerSink {
  
  private static Logger _log = Logger.getLogger(KafkaUtils.class);

  private JSONObject _message;
  private String _topicToRead;
  private String _bucketName;
  private String _shardPath;

  private String _shardFull;
  private BufferedWriter _writer;
  private JSONObject _sinkObject;

  private String _shardPrefix;
  //ExponentialBackoffTicker _ticker = new ExponentialBackoffTicker(1000);

  private int _shardNum;

  private long _numMessagesThisShard;

  private String _gzFilePath;

  public WorkerS3Sink(JSONObject message) {
    // TODO Auto-generated constructor stub
    _message = message;
    _sinkObject = _message.getJSONObject("sink");
    _shardNum = _sinkObject.getInt("shard_num");
    JSONObject config = _sinkObject.getJSONObject("config");
    _bucketName = config.getString("bucket");
    _shardPrefix = config.getString("shard_prefix");
    
    _topicToRead = message.getString("topic");
    _shardPath = config.getString("shard_path");
    
    // Sequence of shard sizes: 1,2,4,8,16,32,64,128,256,512,1024,2048,4096,10000,10000...
    // This is considering that tuples are coming in at a steady pace.
    // If tuples pause, then whatever shards are saved, will be flushed every minute.
    _numMessagesThisShard =  _shardNum <= 12 ? (int) Math.pow(2, _shardNum) : 10000;
  }


  @Override
  public String getTopic() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long sink(ConsumerIterator<String, String> iterator, int queueNumber, boolean sinkUntilTimeout) {
    long sunkTuples = 0;
    int shardNum = (queueNumber + 1) * 10000 + _shardNum;
    
    _shardFull = _shardPath + _shardPrefix + shardNum + ".gz";
    String tmpLocation = Config.getOrDefault("tmp_location", "/tmp");
    _gzFilePath = tmpLocation + "/" + _shardFull;

    try{
      File parentDir = new File(tmpLocation + "/" + _shardPath);
      parentDir.mkdirs();
      _writer = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(_gzFilePath)), "UTF-8"));
      _log.debug("Created File locally: " + _gzFilePath);
      while (iterator.hasNext()) {
        MessageAndMetadata<String, String> fetchedMessage = iterator.next();
        String messageBody = fetchedMessage.message();
        sunkTuples += 1;
        _writer.append(messageBody);
        _writer.newLine();
        _writer.flush();
        // Only flush if we're not expected to sinkUntilTimeout
        if (!sinkUntilTimeout && maybeFlush(false, sunkTuples)) {
          _log.debug("Consumer ("+ iterator.clientId() + ") Retreived message offset to sink from topic " + _topicToRead + " is " + fetchedMessage.offset());
          _log.info("Flushed " + sunkTuples + " messages to S3: " + _shardFull );
          _writer.close();
          return sunkTuples;
        }
      }
    }catch(ConsumerTimeoutException e){
      if(sinkUntilTimeout){
        _log.info("Consumer timed out on topic: " + _topicToRead + ". Final flush has #" + sunkTuples + " messages");  
      }
      if(maybeFlush(true, sunkTuples)){
        _log.info("Flushed " + sunkTuples + " messages to S3: " + _shardFull );
      }
    }
    catch (IOException ioe) {
      _log.error("Sink Error !!!");
      _log.error(ioe.getMessage());
      _log.error(Joiner.on("\n\t").join(ioe.getStackTrace()));
      return sunkTuples;//?
    }
    finally {
      if (_writer != null){
        try {
          _writer.close();
        } catch (IOException e) {
          _log.info(e.getStackTrace());
        }
      }
      File gzFile = new File(_gzFilePath);
      if(gzFile != null && gzFile.exists()){
        gzFile.delete();
      }
    }
    return sunkTuples;
  }
  

  
  protected boolean maybeFlush(boolean forceFlush, long sunkTuples) {
    _log.debug("Fetched " +sunkTuples + " so far...");
    
    if (sunkTuples > 0 && (forceFlush || sunkTuples == _numMessagesThisShard)) {
      File gzFile = null;
      try {
        _writer.close();
        
        gzFile = new File(_gzFilePath);
        _log.debug("File path is: " + _gzFilePath + " with length " + gzFile.length());
        S3Util.copyFile(gzFile, _bucketName, _shardFull);
        _log.debug("Copied " + _gzFilePath + " to " + _shardFull);
        return true;
      } catch (S3Exception | IOException e) {
        _log.error("Flush failed: ", e);
        return false;
      } finally {
        if(gzFile != null && gzFile.exists())
          _log.debug("Deleting file: " + _gzFilePath);
          gzFile.delete();
      }
    }
    return false;
  }
  
  

}
