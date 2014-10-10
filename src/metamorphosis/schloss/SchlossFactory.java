package metamorphosis.schloss;

import net.sf.json.JSONObject;

public interface SchlossFactory<T extends SchlossDistributor> {
  
   public T createSchlossDistributor(JSONObject message);

}
