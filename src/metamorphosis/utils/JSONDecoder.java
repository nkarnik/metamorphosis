package metamorphosis.utils;

import kafka.serializer.Decoder;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class JSONDecoder implements Decoder<JSONObject> {

  @Override
  public JSONObject fromBytes(byte[] bytes) {
    String msg = new String(bytes);
    JSONObject jsonObject = null;
    try{
       jsonObject = JSONObject.fromObject(msg);  
    }catch(JSONException e){
      // Bad object
    }
    return jsonObject;
  }

  public static void main(String[] args) {
    JSONObject fromObject = JSONObject.fromObject("{'nothing': 1}");
    
    System.out.println(fromObject.toString());
    String string = fromObject.getString("topic");
    System.out.println(string);
  }
}
