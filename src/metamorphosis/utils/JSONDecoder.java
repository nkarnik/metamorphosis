package metamorphosis.utils;

import kafka.serializer.Decoder;
import net.sf.json.JSONObject;

public class JSONDecoder implements Decoder<JSONObject> {

  @Override
  public JSONObject fromBytes(byte[] bytes) {
    String msg = new String(bytes);
    return JSONObject.fromObject(msg);
  }

}
