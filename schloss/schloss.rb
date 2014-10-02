require "json"
require 'thread'
require "aws-sdk"
require "poseidon"
require "optparse"
require 'logger'
require "./SinkManager.rb"
require "./SourceManager.rb"

require_relative("../common/kafka_utils.rb")

log = Logger.new('| tee schloss.log', 10, 1024000)
log.datetime_format = '%Y-%m-%d %H:%M:%S'

log.info "Starting schloss"

AWS.config(
          :access_key_id    => 'AKIAJWZ2I3PMFF5O6PFA',
          :secret_access_key => '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'
          )

s3 = AWS::S3.new

$options = {}

opt_parser = OptionParser.new do |opt|
  opt.banner = "Usage: --env "

  opt.on("--env ENV", String, "Required. Env? prod,test,local") do |env|
    $options[:env] = env
  end

  opt.on("--source_topic SOURCE", String, "Required. Topic to read from? prod,test,local") do |source|
    $options[:source_topic] = source
  end

  opt.on("--sink_topic SINK",String,"Required, needed for knowing where to sink from") do |sink|
    $options[:sink_topic] = sink
  end
  
  opt.on("--brokers BROKERS",String, "Optional, for local send in the brokers") do |brokers|
    $options[:brokers] = brokers
  end

  opt.on("--queues QUEUES",String, "Optional, for local send in the queues") do |queues|
    $options[:queues] = queues
  end

  opt.on("--offset OFFSET",Integer, "Optional, for testing, don't loop for messages") do |offset|
    $options[:offset] = offset
  end

  opt.on("--runs RUNS",Integer, "Optional, for testing, don't loop for messages") do |runs|
    $options[:runs] = runs
  end

end

opt_parser.parse!
env = $options[:env] || "local"
sourceTopic = $options[:source_topic] 
sinkTopic = $options[:sink_topic] 
brokers = $options[:brokers] || ["localhost:9092"]
total_runs = $options[:runs] || 0
offset = $options[:offset] || nil

if env == "local"
  fqdns = brokers.split(',')
else
  fqdns = find_broker(env)
end
log.info "fqdns: #{fqdns}"

begin
  queues = $options[:queues].split(",")
rescue
  queues = fqdns.map{|n| n.split(":").first}
end

log.info "fqdns: #{fqdns}"
if queues.size != fqdns.size
  log.error "ERROR: Number of queues does not match brokers: Qs: #{queues} vs Broker fqdns: #{fqdns}"
  exit
end

# fqdns[hostnum].split(":").first
log.info "Config:\nenv: #{env}\nsourceTopic: #{sourceTopic}\nbrokers: #{brokers}\nruns: #{total_runs}\nqueues: #{queues}"

# Start SourceManager
sourceManager = SourceManager.new(sourceTopic, fqdns, total_runs, queues, offset)
sourceManager.start()

# Start Sinker
#sinkManager = SinkManager.new(sinkTopic, LOGFILE, fqdns)
#sinkManager.start()

sourceManager.thread.join
