package metamorphosis;

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
    Config config = new Config();
    
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
        .withLongOpt("schloss.sink.topic")
        .withType(String.class)
        .create()
        );

    availOptions.addOption(OptionBuilder
        .hasArg()
        .withLongOpt("schloss.source.topic")
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
        .withLongOpt("worker.sink.topic")
        .withType(String.class)
        .create()
        );

    availOptions.addOption(OptionBuilder
        .hasArg()
        .withLongOpt("worker.source.topic")
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

    config.put("kafka.brokers", options.getOptionValue("kafka.brokers", "192.168.111.107:9092,192.168.111.108:9092"));
    String zkHost = options.getOptionValue("kafka.zookeeper.host", "192.168.111.106");
    String zkPort = options.getOptionValue("kafka.zookeeper.port", "2181");
    config.put("kafka.zookeeper.host", zkHost);
    config.put("kafka.zookeeper.port", zkPort);
    config.put("kafka.zookeeper.connect",zkHost + ":" + zkPort + "/kafka");
    String service = options.getOptionValue("service");

    KafkaService kafkaService = new KafkaService();
    config.put("kafka.service", kafkaService);
    
    System.out.println(config.toString());
    
    switch(service){
    case "schloss":
      if( !options.hasOption("schloss.source.topic") || 
          !options.hasOption("schloss.sink.topic") || 
          !options.hasOption("worker.sink.queues") || 
          !options.hasOption("worker.source.queues")){
        _log.error("Requested schloss service without the schloss source and sink topics");
        exitWithHelp(availOptions);
      }
      config.put("schloss.source.topic", options.getOptionValue("schloss.source.topic"));
      config.put("schloss.sink.topic", options.getOptionValue("schloss.sink.topic"));
      startSchlossService();
      break;
      
    case "worker":
      if(!options.hasOption("worker.source.topic") || !options.hasOption("worker.sink.topic")){
        _log.error("Requested schloss service without the schloss source and sink topics");
        exitWithHelp(availOptions);
      }
      config.put("worker.source.topic", options.getOptionValue("worker.source.topic"));
      config.put("worker.sink.topic", options.getOptionValue("worker.sink.topic"));

      startWorkerService(kafkaService);
      break;
      
    default:
        System.out.println("Bad value required parameter 'service'. Can be schloss or worker. Received: " + service);
        exitWithHelp(availOptions);
        return;
    }
    
    _log.info("Services started... ");
    
  }

  private static void startWorkerService(KafkaService kafkaService) {
    _log.info("Starting worker services");
    WorkerSourceService sourceService = new WorkerSourceService((String)Config.singleton().getOrException("worker.source.topic"), kafkaService);
    WorkerSinkService sinkService = new WorkerSinkService((String)Config.singleton().getOrException("worker.sink.topic"), kafkaService);
    sourceService.start();
    sinkService.start();

  }

  private static void startSchlossService() {
    _log.info("Starting Schloss service");
    SchlossService schlossService = new  SchlossService();
    schlossService.start();
    
  }

  private static void exitWithHelp(Options availOptions) {
    System.out.println(availOptions.toString());
    System.exit(1);
  }
}
