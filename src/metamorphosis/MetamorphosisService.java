package metamorphosis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import metamorphosis.kafka.KafkaService;
import metamorphosis.schloss.SchlossService;
import metamorphosis.utils.CommandLine;
import metamorphosis.utils.Config;
import metamorphosis.utils.Utils;
import metamorphosis.workers.sinks.WorkerSinkService;
import metamorphosis.workers.sources.WorkerSourceService;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

public class MetamorphosisService {

  private static Logger _log = Logger.getLogger(MetamorphosisService.class);

  public static String workerSourceQueue = "worker.source.queue";
  public static String workerSinkQueue = "worker.sink.queue";
  public static String schlossSinkQueue = "schloss.sink.queue";
  public static String schlossSourceQueue = "schloss.source.queue";
  public static String workerSinkQueues = "worker.sink.queues";
  public static String workerSourceQueues = "worker.source.queues";

  private static WorkerSourceService _workerSourceService;

  private static WorkerSinkService _workerSinkService;

  private static SchlossService _schlossService;
  /***
   * 
   */
  public static void addCleanupShutdownHooks() {
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          cleanup();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }      
    }));
  }
  
  @SuppressWarnings("static-access")
  public static void main(String[] args) throws ParseException {
    
    System.out.println("Starting metamorphosis");
    _log.info("Starting metamorphosis logs...");
    CommandLineParser parser = new GnuParser();
    Options availOptions = new Options();
    new Config();
    availOptions.addOption(OptionBuilder
        .hasArg()
        .withLongOpt("service")
        .withType(String.class)
        .create()
        );
    availOptions.addOption(OptionBuilder
        .hasArg()
        .withLongOpt("environment")
        .withType(String.class)
        .create()
        );
    
    availOptions.addOption(OptionBuilder
        .hasArg()
        .withLongOpt("kafka.brokers")
        .withType(String.class)
        .create()
        );
    
    availOptions.addOption(OptionBuilder
        .hasArg()
        .withLongOpt("kafka.zookeeper.host")
        .withType(String.class)
        .create()
        );

    availOptions.addOption(OptionBuilder
        .hasArg()
        .withLongOpt("kafka.zookeeper.port")
        .withType(String.class)
        .create()
        );
    
    availOptions.addOption(OptionBuilder
        .hasArg()
        .withLongOpt(schlossSinkQueue)
        .withType(String.class)
        .create()
        );

    availOptions.addOption(OptionBuilder
        .hasArg()
        .withLongOpt(schlossSourceQueue)
        .withType(String.class)
        .create()
        );

    availOptions.addOption(OptionBuilder
        .hasArg()
        .withLongOpt(workerSinkQueues)
        .withType(String.class)
        .create()
        );

    availOptions.addOption(OptionBuilder
        .hasArg()
        .withLongOpt(workerSourceQueues)
        .withType(String.class)
        .create()
        );
    
    
    availOptions.addOption(OptionBuilder
        .hasArg()
        .withLongOpt(workerSinkQueue)
        .withType(String.class)
        .create()
        );

    availOptions.addOption(OptionBuilder
        .hasArg()
        .withLongOpt(workerSourceQueue)
        .withType(String.class)
        .create()
        );
    
    availOptions.addOption(OptionBuilder
        .hasArg()
        .withLongOpt("kafka.consumer.timeout.ms")
        .withType(String.class)
        .create()
        );
    
    // Parse the options...
    final org.apache.commons.cli.CommandLine rawOptions = parser.parse(availOptions, args);
    assert (rawOptions != null);
    final CommandLine options = new CommandLine(rawOptions);

    // Help?
    if (options.hasOption("help")) {
      exitWithHelp(availOptions);
      return;
    }
    
    if(!options.hasOption("service")){
      System.out.println("Missing required parameter 'service'. Available services are 'schloss' or 'worker'");
      exitWithHelp(availOptions);
      return;
    }
    String env = options.getOptionValue("environment");
    Config.singleton().put("environment", env);
    Config.singleton().put("kafka.brokers", options.getOptionValue("kafka.brokers", "192.168.111.107:9092,192.168.111.108:9092"));
    String zkHost = options.getOptionValue("kafka.zookeeper.host", "192.168.111.106");
    String zkPort = options.getOptionValue("kafka.zookeeper.port", "2181");
    Config.singleton().put("kafka.zookeeper.host", zkHost);
    Config.singleton().put("kafka.zookeeper.port", zkPort);
    Config.singleton().put("kafka.zookeeper.connect", zkHost + ":" + zkPort + "/kafka");
    Config.singleton().put("gmb.zookeeper.connect", zkHost + ":" + zkPort + "/gmb");
    String apiHost;
    switch(env){
    case "test":
      apiHost = "test.api.zillabyte.com";
      break;
    case "prod":
      apiHost = "api.zillabyte.com";
      break;
    default:
      apiHost = "localhost";
      break;
    }
    Config.singleton().put("api.host", apiHost);
    int apiPort = 80;
    Config.singleton().put("api.port", apiPort );
    Config.singleton().put("kafka.consumer.timeout.ms", options.getOptionValue("kafka.consumer.timeout.ms"));
    String service = options.getOptionValue("service");

    KafkaService kafkaService = new KafkaService();
    Config.singleton().put("kafka.service", kafkaService);
    
    
    
    System.out.println(Config.singleton().toString());
    _log.info("Starting service: " + service);
    switch(service){
    case "schloss":
      if( !options.hasOption(schlossSourceQueue) || 
          !options.hasOption(schlossSinkQueue) || 
          !options.hasOption(workerSinkQueues) || 
          !options.hasOption(workerSourceQueues)){
        _log.error("Requested schloss service without the schloss source and sink topics");
        exitWithHelp(availOptions);
      }
      String schlossSourceQueueValue = options.getOptionValue(schlossSourceQueue);
      String schlossSinkQueueValue = options.getOptionValue(schlossSinkQueue);
      String workerSourceQueuesValue = options.getOptionValue(workerSourceQueues);
      String workerSinkQueuesValue = options.getOptionValue(workerSinkQueues);

      Config.singleton().put(schlossSourceQueue, schlossSourceQueueValue);
      Config.singleton().put(schlossSinkQueue, schlossSinkQueueValue);
      Config.singleton().put(workerSourceQueues, workerSourceQueuesValue);
      Config.singleton().put(workerSinkQueues, workerSinkQueuesValue);
      
      kafkaService.ensureQueuesExist(schlossSourceQueueValue,schlossSinkQueueValue);
      kafkaService.ensureQueuesExist(workerSourceQueuesValue.split(","));
      kafkaService.ensureQueuesExist(workerSinkQueuesValue.split(","));
      
      startSchlossService();
      break;
      
    case "worker":
      if(!options.hasOption(workerSourceQueue) || !options.hasOption(workerSinkQueue)){
        _log.error("Requested worker service without the worker source and sink queue name");
        exitWithHelp(availOptions);
      }
      String workerSourceQueueValue = options.getOptionValue(workerSourceQueue);
      String workerSinkQueueValue = options.getOptionValue(workerSinkQueue);
      
      Config.singleton().put(workerSourceQueue, workerSourceQueueValue);
      Config.singleton().put(workerSinkQueue, workerSinkQueueValue);
      
      kafkaService.ensureQueuesExist(workerSourceQueueValue, workerSinkQueueValue);

      startWorkerService(kafkaService);
      break;
      
    default:
        _log.error("Bad value required parameter 'service'. Can be schloss or worker. Received: " + service);
        exitWithHelp(availOptions);
        return;
    }
    
    _log.info("Services started... press 'q' <enter> to quit");
    
  }

  private static void startWorkerService(KafkaService kafkaService) {
    _log.info("Starting worker services");
    _workerSourceService = new WorkerSourceService((String)Config.singleton().getOrException(workerSourceQueue), kafkaService);
    _workerSinkService = new WorkerSinkService((String)Config.singleton().getOrException(workerSinkQueue), kafkaService);
    _workerSourceService.start();
    _workerSinkService.start();
    readInput();
    
  }

  private static void readInput() {
    while(true){
      String input = getInput();
      if(input != null && input.equals("q")){
        cleanup();
        System.exit(1);
      }else{
        if(input != null){
          _log.info("Received input: " + input);
        }
      }
      Utils.sleep(3000);
    }
  }

  public static void cleanup() {
    _log.info("Cleaning up for shutdown.");
    if(_workerSourceService != null)
      _workerSourceService.stop();  
    if(_workerSinkService != null)
      _workerSinkService.stop();
    if(_schlossService != null)
      _schlossService.stop();
    System.exit(1);
  }

  private static void startSchlossService() {
    _log.info("Starting Schloss service");
    _schlossService = new  SchlossService();
    _schlossService.start();
    readInput();
  }

  private static String getInput() {
    try {
      BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
      String input = null;
      do{
        System.out.print("> ");
        input = br.readLine();
      }while(input != null && input.trim().length() == 0);
      return input;
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  private static void exitWithHelp(Options availOptions) {
    System.out.println(availOptions.toString());
    System.exit(1);
  }
}
