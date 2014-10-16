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
  private int _bytesFetched;
  private File _file;

  private String _shardFull;
  private BufferedWriter _writer;
  private GZIPOutputStream _zip;
  private JSONObject _sinkObject;

  private String _shardPrefix;
  private static long MIN_SHARD_SIZE = 50 * 1000 * 1000;

  public WorkerS3Sink(JSONObject message) {
    // TODO Auto-generated constructor stub
    _message = message;
    _sinkObject = _message.getJSONObject("sink");
    JSONObject config = _sinkObject.getJSONObject("config");
    _bucketName = config.getString("bucket");
    _shardPath = config.getString("shard_path");
    _shardPrefix = config.getString("shard_prefix");
    _topicToRead = message.getString("topic");
    _bytesFetched = 0;
  }


  @Override
  public String getTopic() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void sink(ConsumerIterator<String, String> iterator, int queueNumber) {
    
    int shardNum = queueNumber * 1000 + _sinkObject.getInt("retry");
    _shardFull = _shardPath + _shardPrefix + shardNum + ".gz";
    String gzFileToWrite = "/tmp/" + _shardPrefix + shardNum + ".gz";

    try{
      _file = new File(gzFileToWrite);
      _log.debug("Created File locally: " + gzFileToWrite);
      
      _zip = new GZIPOutputStream(new FileOutputStream(_file));
      _writer = new BufferedWriter(new OutputStreamWriter(_zip, "UTF-8"));
      while (iterator.hasNext()) {
        MessageAndMetadata<String, String> fetchedMessage = iterator.next();
        String messageBody = fetchedMessage.message();
        int messageSize = messageBody.getBytes("UTF-8").length;
        _bytesFetched += messageSize;
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
      _log.info("Consumer timed out. maybe flush " + _bytesFetched + " bytes");
      maybeFlush(true);
    }
    catch (IOException ioe) {
      _log.error(Joiner.on("\n").join(ioe.getStackTrace()));
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
      if(_file != null && _file.exists()){
        _file.delete();
      }
    }
  }
  

  
  protected boolean maybeFlush(boolean forceFlush) {
    _log.debug("Fetched " +_bytesFetched + " so far...");
    if (_bytesFetched > 0 && (forceFlush || _bytesFetched > MIN_SHARD_SIZE)) {
      try {
        _writer.close();
        _log.debug("File path is: " + _file.getAbsolutePath() + " with length " + _file.length());
        S3Util.copyFile(_file, _bucketName, _shardFull);
        _log.debug("Copied " + _file.getAbsolutePath() + " to " + _shardFull);
        return true;
      } catch (S3Exception | IOException e) {
        _log.error("Flush failed: ", e);
        return false;
      } finally {
        _log.debug("Deleting file: " + _file.getAbsolutePath());
        _file.delete();
      }
    }
    return false;
  }
  
  

}
