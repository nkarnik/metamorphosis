require "json"
require 'thread'
require "aws-sdk"
require "poseidon"
require "optparse"
require "./Sinker.rb"
require_relative("../common/kafka_utils.rb")

LOGFILE = "kafka_consumer.log"
`touch #{LOGFILE}`

def log(msg)
  if @lf.nil?
    @lf = File.open(LOGFILE, 'a')
  end
  puts "#{Time.now}: #{msg}\n"
  @lf.write "#{Time.now}: #{msg}\n"
  @lf.flush
end

log "Starting read_api"

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

  opt.on("--topic TOPIC", String, "Required. Topic to read from? prod,test,local") do |topic|
    $options[:topic] = topic
  end

  opt.on("--sink_topic SINK",String,"Required, needed for knowing where to sink from") do |sink|
    $options[:sink] = sink
  end
  
  opt.on("--brokers BROKERS",String, "Optional, for local send in the brokers") do |brokers|
    $options[:brokers] = brokers
  end

  opt.on("--queues QUEUES",String, "Optional, for local send in the queues") do |queues|
    $options[:queues] = queues
  end


  opt.on("--runs RUNS",Integer, "Optional, for testing, don't loop for messages") do |runs|
    $options[:runs] = runs
  end

 


end

opt_parser.parse!
env = $options[:env] || "local"
topic = $options[:topic] 
sinkTopic = $options[:sink_topic] 
brokers = $options[:brokers] || ["localhost:9092"]
total_runs = $options[:runs] || 0

if env == "local"
  fqdns = brokers.split(',')
else
  fqdns = find_broker(env)
end
log "fqdns: #{fqdns}"

begin
  queues = $options[:queues].split(",")
rescue
  queues = fqdns.map{|n| n.split(":").first}
end

log "fqdns: #{fqdns}"
if queues.size != fqdns.size
  log "ERROR: Number of queues does not match brokers: Qs: #{queues} vs Broker fqdns: #{fqdns}"
  exit
end
# fqdns[hostnum].split(":").first
log "Config:\nenv: #{env}\ntopic: #{topic}\nbrokers: #{brokers}\nruns: #{total_runs}\nqueues: #{queues}"

leaders_per_partition = get_leaders_for_partitions(topic, fqdns)

log "Leaders: #{leaders_per_partition}"
# Assuming one partition for this topic, find the singular leader
leader = leaders_per_partition.first

consumer = Poseidon::PartitionConsumer.new("topic_consumer", leader.split(":").first, leader.split(":").last, topic, 0, :earliest_offset)

shard_writer = Poseidon::Producer.new(fqdns, "mockwriter", :type => :sync)

con = 0

# Start Sourcer
sourcer = Sourcer.new(topic, LOGFILE, fqdns, total_runs)
sourcer.start()

# Start Sinker
sinker = Sinker.new(sinkTopic, LOGFILE, fqdns)
sinker.start()
