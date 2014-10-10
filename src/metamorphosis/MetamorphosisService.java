package metamorphosis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import metamorphosis.kafka.KafkaService;
import metamorphosis.schloss.SchlossService;
import metamorphosis.utils.CommandLine;
import metamorphosis.utils.Config;
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

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws ParseException {
 // Command line options
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
        .withLongOpt("schloss.sink.queue")
        .withType(String.class)
        .create()
        );

    availOptions.addOption(OptionBuilder
        .hasArg()
        .withLongOpt("schloss.source.queue")
        .withType(String.class)
        .create()
        );

    availOptions.addOption(OptionBuilder
        .hasArg()
        .withLongOpt("worker.sink.queues")
        .withType(String.class)
        .create()
        );

    availOptions.addOption(OptionBuilder
        .hasArg()
        .withLongOpt("worker.source.queues")
        .withType(String.class)
        .create()
        );
    
    
    availOptions.addOption(OptionBuilder
        .hasArg()
        .withLongOpt("worker.sink.queue")
        .withType(String.class)
        .create()
        );

    availOptions.addOption(OptionBuilder
        .hasArg()
        .withLongOpt("worker.source.queue")
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

    Config.singleton().put("kafka.brokers", options.getOptionValue("kafka.brokers", "192.168.111.107:9092,192.168.111.108:9092"));
    String zkHost = options.getOptionValue("kafka.zookeeper.host", "192.168.111.106");
    String zkPort = options.getOptionValue("kafka.zookeeper.port", "2181");
    Config.singleton().put("kafka.zookeeper.host", zkHost);
    Config.singleton().put("kafka.zookeeper.port", zkPort);
    Config.singleton().put("kafka.zookeeper.connect",zkHost + ":" + zkPort + "/kafka");
    String service = options.getOptionValue("service");

    KafkaService kafkaService = new KafkaService();
    Config.singleton().put("kafka.service", kafkaService);
    
    System.out.println(Config.singleton().toString());
    
    switch(service){
    case "schloss":
      if( !options.hasOption("schloss.source.queue") || 
          !options.hasOption("schloss.sink.queue") || 
          !options.hasOption("worker.sink.queues") || 
          !options.hasOption("worker.source.queues")){
        _log.error("Requested schloss service without the schloss source and sink topics");
        exitWithHelp(availOptions);
      }
      Config.singleton().put("schloss.source.queue", options.getOptionValue("schloss.source.queue"));
      Config.singleton().put("schloss.sink.queue", options.getOptionValue("schloss.sink.queue"));
      Config.singleton().put("worker.source.queues", options.getOptionValue("worker.source.queues"));
      Config.singleton().put("worker.sink.queues", options.getOptionValue("worker.sink.queues"));
      startSchlossService();
      break;
      
    case "worker":
      if(!options.hasOption("worker.source.queue") || !options.hasOption("worker.sink.queue")){
        _log.error("Requested worker service without the worker source and sink queue name");
        exitWithHelp(availOptions);
      }
      Config.singleton().put("worker.source.queue", options.getOptionValue("worker.source.queue"));
      Config.singleton().put("worker.sink.queue", options.getOptionValue("worker.sink.queue"));

      startWorkerService(kafkaService);
      break;
      
    default:
        System.out.println("Bad value required parameter 'service'. Can be schloss or worker. Received: " + service);
        exitWithHelp(availOptions);
        return;
    }
    
    _log.info("Services started... press 'q' <enter> to quit");
    
  }

  private static void startWorkerService(KafkaService kafkaService) {
    _log.info("Starting worker services");
    WorkerSourceService sourceService = new WorkerSourceService((String)Config.singleton().getOrException("worker.source.queue"), kafkaService);
    WorkerSinkService sinkService = new WorkerSinkService((String)Config.singleton().getOrException("worker.sink.queue"), kafkaService);
    sourceService.start();
    sinkService.start();
    while(true){
      String input = getInput();
      if(input != null && input.equals("q")){
        sourceService.stop();
        sinkService.stop();
        System.exit(1);
      }else{
        _log.info("Received input: " + input);
      }
    }
    
  }

  private static void startSchlossService() {
    _log.info("Starting Schloss service");
    SchlossService schlossService = new  SchlossService();
    schlossService.start();
    while(true){
      String input = getInput();
      if(input != null && input.equals("q")){
        schlossService.stop();
        System.exit(1);
      }else{
        _log.info("Received input: " + input);
      }
    }
  }

  private static String getInput() {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    
    try {
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
