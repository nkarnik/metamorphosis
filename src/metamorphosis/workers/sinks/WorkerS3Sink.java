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
  private int _numMessages;

  private String _shardFull;
  private BufferedWriter _writer;
  private JSONObject _sinkObject;

  private String _shardPrefix;
  //ExponentialBackoffTicker _ticker = new ExponentialBackoffTicker(1000);

  private int _retryNum;

  private int _numMessagesThisShard;

  private String _gzFilePath;

  public WorkerS3Sink(JSONObject message) {
    // TODO Auto-generated constructor stub
    _message = message;
    _sinkObject = _message.getJSONObject("sink");
    _retryNum = _sinkObject.getInt("retry");
    JSONObject config = _sinkObject.getJSONObject("config");
    _bucketName = config.getString("bucket");
    _shardPrefix = config.getString("shard_prefix");
    
    _topicToRead = message.getString("topic");
    _shardPath = config.getString("shard_path");
    
    _numMessages = 0;
    _numMessagesThisShard = 1000; // _retryNum < 10 ? (_retryNum + 1) * 100 : 1000;
  }


  @Override
  public String getTopic() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void sink(ConsumerIterator<String, String> iterator, int queueNumber) {
    
    int shardNum = (queueNumber + 1) * 1000 + _retryNum;
    
    _shardFull = _shardPath + _shardPrefix + shardNum + ".gz";
    _gzFilePath = "/tmp/" + _shardFull;
    

    try{
      File parentDir = new File("/tmp/" + _shardPath);
      parentDir.mkdirs();
      _writer = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(_gzFilePath)), "UTF-8"));
      _log.debug("Created File locally: " + _gzFilePath);
      while (iterator.hasNext()) {
        MessageAndMetadata<String, String> fetchedMessage = iterator.next();
        String messageBody = fetchedMessage.message();
        _numMessages += 1;
        _writer.append(messageBody);
        _writer.newLine();
        _writer.flush();
        if (maybeFlush(false)) {
          _log.info("Consumer ("+ iterator.clientId() + ") Retreived message offset to sink from topic " + _topicToRead + " is " + fetchedMessage.offset());
          _log.info("Flushed shard to S3: " + _shardFull);
          _writer.close();
          return;
        }
      }
    }catch(ConsumerTimeoutException e){
      _log.info("Consumer timed out. maybe flush " + _numMessages + " messages");  
      if(maybeFlush(true)){
        _log.info("Flushed shard to S3: " + _shardFull);

      }
    }
    catch (IOException ioe) {
      _log.error("Sink Error !!!");
      _log.error(ioe.getMessage());
      _log.error(Joiner.on("\n\t").join(ioe.getStackTrace()));
      return;
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
  }
  

  
  protected boolean maybeFlush(boolean forceFlush) {
    _log.debug("Fetched " +_numMessages + " so far...");
    
    if (_numMessages > 0 && (forceFlush || _numMessages > _numMessagesThisShard)) {
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
