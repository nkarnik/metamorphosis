package metamorphosis.utils.s3;

import java.io.Serializable;

/****
 * 
 * TODO: wrap all s3 functions inside of here. 
 * 
 * @author jake
 *
 */
public class S3Factory implements Serializable {

  private static final long serialVersionUID = -8064311522478149610L;
  
  private String _prefix;

  public S3Factory(String prefix) {
    this._prefix = prefix + "/";
  }

  public String prefix() {
    return _prefix;
  }
  
  public String prefixKey(String key) {
    String f = _prefix + key.replaceFirst("$/+", "");
    return f.replaceFirst("$/+", ""); // don't start keys with slashes.
  }

  
  
  
}
