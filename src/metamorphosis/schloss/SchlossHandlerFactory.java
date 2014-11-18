package metamorphosis.schloss;

import net.sf.json.JSONObject;

public interface SchlossHandlerFactory<T extends SchlossHandler> {
  
   public T createSchlossHandler(JSONObject message);

}
