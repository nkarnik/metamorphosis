require "json"
require 'thread'
require "aws-sdk"
require "poseidon"
require "optparse"
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
brokers = $options[:brokers] || ["localhost:9092"]
total_runs = $options[:runs] || 0

if env == "local"
  fqdns = brokers.split(',')
else
  fqdns = find_broker(env)
end
queues = $options[:queues].split(",") || fdns.map{|n| n.split(":").first}

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

local_manifest = "manifest"
unless File.exists? local_manifest
  File.delete local_manifest
end
run_num = 0
loop do
  begin
    log "Waiting on message"
    messages = consumer.fetch({:max_bytes => 100000}) # Timeout? 
    messages.each do |m|
      message = m.value
      message = JSON.parse(message)
      log "Processing  message: #{message}"

      bucket_name = message["source"]["config"]["bucket"]
      manifest_path = message["source"]["config"]["manifest"]
      topic_to_write = message["topic"]
      sourcetype = message["source"]["type"]
      bucket = s3.buckets[bucket_name]
      log "Downloading Manifest from #{manifest_path} for topic: #{topic_to_write} in bucket: #{bucket_name}"
      
      # If source type is s3
      File.open(local_manifest, 'wb') do |file|
        log "Opened file: #{file}"
        bucket.objects[manifest_path].read do |chunk|
          begin
            file.write(chunk)
          rescue
            log "s3 error for path: #{f}"
          end
        end
      end
      # log "Manifest length: #{`wc -l #{local_manifest}`}"
      hosts = fqdns.length
      log "Hosts #{hosts}"
      lines_consumed = 0
      File.open(local_manifest).each do |line|
        config = {:bucket => bucket_name, :shard => line.chomp}
        source = {:type => sourcetype, :config => config}
        info = {:source => source, :topic => topic_to_write}.to_json
        hostnum = lines_consumed % hosts
        broker_topic = queues[hostnum]
        msgs = []
        log "Writing to topic: #{broker_topic} message: #{info}"
        
        msgs << Poseidon::MessageToSend.new(broker_topic, info)
        unless shard_writer.send_messages(msgs)
          log "message send error: #{broker_topic}"
        end
        
        lines_consumed += 1
      end
      log "Number of shards emitted into #{topic}: #{lines_consumed}"


    end
  rescue => e
    log "ERROR: #{e.message}\n#{e.backtrace}"
  end
  
  run_num += 1

  if total_runs > 0
    if run_num >= total_runs
      log "Breaking because we wanted only #{total_runs}, and completed #{run_num} runs"
      break
    end
  end
  log "Looping: #{run_num}"
end
