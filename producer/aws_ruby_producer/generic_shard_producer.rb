require 'json'
require 'thread'
require 'aws-sdk'
require 'poseidon'
require 'optparse'
require 'pry'
require 'logger'
require './SourceQueue'
require './TopicProducerConfig'
require './SourceThread'

require_relative "../../common/kafka_utils.rb"

log = Logger.new('| tee shard_producer.log', 10, 1024000)
log.datetime_format = '%Y-%m-%d %H:%M:%S'

AWS.config(
          :access_key_id    => 'AKIAJWZ2I3PMFF5O6PFA',
          :secret_access_key => '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'
          )


host = `hostname`.chomp

$options = {}

opt_parser = OptionParser.new do |opt|
  opt.banner = "Usage: --env "

  opt.on("--env ENV", String, "Required. Env? prod,test,local") do |env|
    $options[:env] = env
  end

  opt.on("--queue_name QUEUE", String, "Optional, name of the queue to read from. Defaults to host fqdn") do |queue|
    $options[:queue_name] = queue
  end

  opt.on("--threads THREADS",Integer, "Number of threads. Default: 1") do |threads|
    $options[:threads] = threads
  end

  opt.on("--shards SHARDS",Integer, "Optional. Number of shards to download. Default: All") do |shards|
    $options[:num_shards] = shards
  end

  opt.on("--brokers BROKERS",String, "Optional, for local send in the brokers") do |brokers|
    $options[:brokers] = brokers
  end

  opt.on("--no_recovery", "Optional, for local send in the brokers") do
    $options[:no_recovery] = true
  end
end

#Parse and set options
opt_parser.parse!
@env = $options[:env] || "local"

queue_name = $options[:queue_name] || "#{host}.ec2.internal"
if $options[:no_recovery]
  @offset = :earliest_offset
else
  @offset = `cat offset*`.split().min.to_i || :earliest_offset
  log.info "Recovered offset is #{@offset}"
end
num_threads = $options[:threads].to_i || "1"
num_shards = $options[:num_shards].to_i || "0"
brokers = $options[:brokers] || ["localhost:9092"]

$work_q = SourceQueue.new

#pass in env (test/prod) and get fqdns of all kafka brokers

if @env == "local"
  fqdns = brokers.split(",")
else
  fqdns = find_broker(@env)
end

puts "fqdns: #{fqdns}"
consumers = []
producers = []

log.info "Config:::"
log.info "  env: #{@env}"
log.info "  Queue: #{queue_name}"
log.info "  Threads: #{num_threads}"
log.info "  seed brokers: #{fqdns}"

$topic_producer_hash = {}
seed_brokers = fqdns
$broker_pool = Poseidon::BrokerPool.new("fetch_metadata_client", seed_brokers)

#Create array of consumers to read from dedicated kafka worker queue for this broker
con = 0
leaders_per_partition = get_leaders_for_partitions(queue_name, fqdns)

leader = leaders_per_partition.first
log.info "Leader: #{leader}"

@consumer = Poseidon::PartitionConsumer.new(queue_name, leader.split(":").first, leader.split(":").last, queue_name, 0, @offset)
log.info "Consumer created for #{queue_name} #{leader}"


#SET FLAG -> reading from queue should either be one time limited or threaded -- if env is local, thread terminates early
q_thread = Thread.new{
  messages_processed = 0
  loop do
    log.info "Starting loop"
    #Fetch messages from dedicated worker queue, push message to work_q
    # Assuming one partition for this topic, find the singular leader

    if num_shards > 0 and  messages_processed == num_shards
      log.info "Completed processing #{num_shards}. Stopping read queue"
      break
    end
    begin
      log.info "Getting message now... "
      messages = @consumer.fetch({:max_bytes => 1000000})
      #log.info "Messages received: #{messages}"
      log.info "#{messages.length} messages received"
      messages.each do |m|
        message = m.value
        log.info "message: #{message} with offset #{m.offset}"
        message = JSON.parse(message)
        log.info "Processing message: #{message}"
        log.info "topic is #{message["topic"]}"
        info = {:message => message, :offset => m.offset}
        topic = message["topic"]
        $work_q.push(info)
        log.info $work_q.size
        log.info topic
        # build TopicProducer configuration object for topic
        if not $topic_producer_hash.has_key?(topic)
          log.info "Creating new producer for #{topic}"
          topicproducer = TopicProducerConfig.new($broker_pool, topic)
          $topic_producer_hash[topic] = topicproducer
          a = topicproducer.partitions_on_localhost
          log.info "Finished creating producer for #{topic}"
        end
        messages_processed += 1
      end
    rescue Exception => e
      log.error "ERROR: #{e.message}"
      log.error e.backtrace.join("\n")
    end
    sleep 2
    #make sure we're not hammering kafka
  end

}

q_thread.join

# q_thread needs time to start before worker threads can pull from $work_q
# sleep 10

start_time = Time.now
puts "Start time: #{start_time}"

#Spin up selected number of threads, generate sources based on configs
workers = []
num_threads.times do |thread_num|
  log.info "starting Producer thread #{thread_num}"
  t = ProducerThread.new(num_threads, thread_num, start_time)
  t.start($work_q, $topic_producer_hash)
  workers << t.thread
end

workers.map(&:join);

log.info "Done with the threads"
elapsed_time = Time.now - start_time
log.info "Total shards into kafka: #{num_shards} shards with #{num_threads} threads in #{elapsed_time} seconds. Avg time per shard: #{elapsed_time/num_shards}"
