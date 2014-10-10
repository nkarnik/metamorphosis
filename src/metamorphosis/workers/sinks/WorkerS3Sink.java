package metamorphosis.workers.sinks;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;

import kafka.consumer.ConsumerIterator;
import kafka.message.MessageAndMetadata;
import metamorphosis.utils.KafkaUtils;
import metamorphosis.utils.s3.S3Exception;
import metamorphosis.utils.s3.S3Util;
import net.sf.json.JSONObject;

public class WorkerS3Sink extends WorkerSink {
  
  private static Logger _log = Logger.getLogger(KafkaUtils.class);

  private JSONObject _message;
  private String _topicToRead;
  private String _sinkType;
  private String _bucketName;
  private String _shardPath;
  private int _bytesFetched;

  private String _gzFileToWrite;

  private File _file;

  private String _shardPrefix;

  private String _shardFull;

  private BufferedWriter _writer;

  private GZIPOutputStream _zip;

  private JSONObject _sinkObject;
  
  private static int minShardSize = 50000000;

  public WorkerS3Sink(JSONObject message) {
    // TODO Auto-generated constructor stub
    _message = message;
    _sinkObject = _message.getJSONObject("sink");
    JSONObject config = _sinkObject.getJSONObject("config");
    _bucketName = config.getString("bucket");
    _shardPath = config.getString("shard_path");
    _shardPrefix = config.getString("shard_prefix");
    _shardFull = _shardPath + _shardPrefix + "2";
    _topicToRead = message.getString("topic");
    _sinkType = _sinkObject.getString("type");
    _bytesFetched = 0;

    //_shardFull = _topicToRead + queueNumber + sinkObject.getInt("retry")
    _gzFileToWrite = _topicToRead + ".gz";
    _log.info("Shard name is: " + _gzFileToWrite);
  }


  @Override
  public String getTopic() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void sink(ConsumerIterator<String, String> iterator, String queueNumber) {
    
    _shardFull = _shardPath + _topicToRead + queueNumber + _sinkObject.getInt("retry");
    
    try {
      _file = new File(_gzFileToWrite);
      _log.info("Created File locally...");
    
      _zip = new GZIPOutputStream(new FileOutputStream(_file));
      _writer = new BufferedWriter(new OutputStreamWriter(_zip, "UTF-8"));
      while (iterator.hasNext()) {
        
        _log.info("Consumer iterator info: " + iterator.clientId());
        MessageAndMetadata<String, String> fetchedMessage = iterator.next();
        _log.info("Retreived message offset to sink from topic " + _topicToRead + " is " + fetchedMessage.offset());
        String messageBody = fetchedMessage.message();
        int messageSize = messageBody.getBytes("UTF-8").length;
        _bytesFetched += messageSize;
        _writer.append(messageBody);
        _writer.newLine();
        _writer.flush();
        if (maybeFlush()) {
          _log.info("Flushed shard to S3");
          _writer.close();
          
          return;
        }
        
        
      }
    }
    catch (IOException ioe) {
      _log.info(ioe.getStackTrace());
      return;
    }
    
    finally {
      if (_writer != null)
        try {
          _writer.close();
        } catch (IOException e) {
          _log.info(e.getStackTrace());
        }
    }
    
    
    
  }
  

  
  protected boolean maybeFlush() {
    
    _log.info("Fetched " +_bytesFetched + " so far...");
    
    if (_bytesFetched > minShardSize) {
      try {
        _writer.close();
        _log.info("File path is: " + _file.getAbsolutePath() + " with length " + _file.length());
        S3Util.copyFile(_file, _bucketName, _shardFull);
        Thread.sleep(2000);
        return true;
      } catch (S3Exception e) {
        _log.info(e.getStackTrace());
        return false;
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        _log.info(e.getStackTrace());
      } catch (IOException e) {
        // TODO Auto-generated catch block
        _log.info(e.getStackTrace());
      }
      
      
    }
    
    return false;

    
  }
  
  

}
