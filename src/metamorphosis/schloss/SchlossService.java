package metamorphosis.schloss;


import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import metamorphosis.schloss.sinks.SchlossSinkService;
import metamorphosis.schloss.sources.SchlossSourceSevice;
import metamorphosis.utils.Config;
import metamorphosis.utils.Utils;

import org.apache.log4j.Logger;

public class SchlossService {

  protected static final long SLEEP_BETWEEN_READS = 30 * 1000;
  static AtomicBoolean isRunning;
  Logger _log = Logger.getLogger(SchlossService.class);
  private String _sourceTopic;
  private String _sinkTopic;
  private Future<Void> _sourceReadThread;
  private Future<Void> _sinkReadThread;
  //private KafkaService _kafkaService;

  public SchlossService() {

    //This is here to enforce that these configs are set at startup. Should throw exception and die if they are not. 
    Config.singleton().getOrException("kafka.brokers");
    Config.singleton().getOrException("kafka.zookeeper.connect");
    _sourceTopic = Config.singleton().getOrException("schloss.source.queue");
    _sinkTopic = Config.singleton().getOrException("schloss.sink.queue");

  }

  public void start() {
    // Start while loop
    isRunning = new AtomicBoolean(true);
    startSourceReadThread();
    startSinkReadThread();
  }

  public Future<Void> startSinkReadThread() {
    _sinkReadThread = Utils.run(new SchlossSinkService(_sinkTopic));
    return _sinkReadThread;
  }

  public Future<Void> startSourceReadThread() {
    _sourceReadThread = Utils.run(new SchlossSourceSevice(_sourceTopic));
    return _sourceReadThread;
  }

  public void setRunning(boolean state){
    isRunning = new AtomicBoolean(state);
  }
  public void stop(){
    _log.info("Shutting down schloss");
    isRunning.set(false);

    try {
      if(_sourceReadThread != null && !_sourceReadThread.isDone()){
        _log.info("Waiting on source read thread");
        _sourceReadThread.get();
      }
      if(_sinkReadThread != null && !_sinkReadThread.isDone()){
        _log.info("Waiting on sink read thread");
        _sinkReadThread.get();
      }
    } catch (InterruptedException | ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }finally{
      _log.info("Shutdown complete");
    }
  }
}
