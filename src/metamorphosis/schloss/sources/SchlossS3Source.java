package metamorphosis.schloss.sources;

import java.io.IOException;
import java.util.List;

import metamorphosis.utils.S3Util;
import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

public class SchlossS3Source implements SchlossSource{

  private JSONObject _message;
  private Logger _log = Logger.getLogger(SchlossS3Source.class);
  public String _bucketName;
  private String _sourceType;
  private String _topicToWrite;
  private String _manifestPath;
  private List<String> _manifestContents;
  
  
  public SchlossS3Source(JSONObject message) {
    _message = message;
    JSONObject sourceObject = _message.getJSONObject("source");
    JSONObject config = sourceObject.getJSONObject("config");
    _bucketName = config.getString("bucket");
    _manifestPath = config.getString("manifest");
    _topicToWrite = message.getString("topic");
    _sourceType = sourceObject.getString("type");
    _manifestContents = null ;
    try {
      _manifestContents  = Lists.newArrayList(S3Util.readFile(_bucketName, _manifestPath).split("\n"));
    } catch (IOException e) {
      _log.info("Failed to get s3 manifest path: " + _manifestPath);
    }
  }

  

}
