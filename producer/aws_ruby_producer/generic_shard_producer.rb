require 'json'
require 'thread'
require 'aws-sdk'
require 'poseidon'
require 'optparse'
require 'pry'
require './TopicProducerConfig'
require './SourceThread'
require_relative "../../common/kafka_utils.rb"

LOGFILE = "queue_consumer.log"
`touch #{LOGFILE}`

def log(msg)
  if @lf.nil?
    @lf = File.open(LOGFILE, 'a')
  end
  puts "#{Time.now}: #{msg}\n"
  @lf.write "#{Time.now}: #{msg}\n"
  @lf.flush
end

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

end

#Parse and set options
opt_parser.parse!
env = $options[:env] || "local"

queue_name = $options[:queue_name] || "#{host}.ec2.internal"

num_threads = $options[:threads].to_i || "1"
num_shards = $options[:num_shards].to_i || "10"
brokers = $options[:brokers] || ["localhost:9092"]

$work_q = Queue.new



#pass in env (test/prod) and get fqdns of all kafka brokers

if env == "local"
  fqdns = brokers.split(",")
else
  fqdns = find_broker(env)
end

puts "fqdns: #{fqdns}"
consumers = []
producers = []

log "Config:\nenv: #{env}\nQueue: #{queue_name}\nThreads: #{num_threads}\nseed brokers: #{fqdns}"

$topic_producer_hash = {}
seed_brokers = fqdns
$broker_pool = Poseidon::BrokerPool.new("fetch_metadata_client", seed_brokers)

#Create array of consumers to read from dedicated kafka worker queue for this broker
con = 0
leaders_per_partition = get_leaders_for_partitions(queue_name, fqdns)

leader = leaders_per_partition.first
log "Leader: #{leader}"

@consumer = Poseidon::PartitionConsumer.new(queue_name, leader.split(":").first, leader.split(":").last, queue_name, 0, :earliest_offset)

log "Consumer : #{@consumer}"

def read_from_queue(cons, worker_q)
  loop do
    log "Starting loop"
    #Fetch messages from dedicated worker queue, push message to work_q

    # Assuming one partition for this topic, find the singular leader

    begin

      log "Getting message now... consumer: #{@consumer}"
      messages = @consumer.fetch({:max_bytes => 1000000})
      log "Got message?"
      #log "Messages received: #{messages}"
      log "#{messages.length} messages received"
      #sleep 100
      messages.each do |m|
        message = m.value
        log message
        message = JSON.parse(message)
        log "Processing message: #{message}"

        topic = message["topic"]
        worker_q << message
        #log worker_q.size
        #log topic
        # build TopicProducer configuration object for topic
        if not $topic_producer_hash.has_key?(topic)
          log "Creating new producer for #{topic}"
          topicproducer = TopicProducerConfig.new($broker_pool, topic)
          $topic_producer_hash[topic] = topicproducer
          a = topicproducer.partitions_on_localhost
          log "Finished creating producer for #{topic}"
        end
      end
    rescue Exception => e
      log "ERROR: #{e.message}"
      #puts "error"
    end

  if env == "local"
    log "Local environment, so limiting # of shards"
    break
  end 

  end
end

#SET FLAG -> reading from queue should either be one time limited or threaded -- if env is local, thread terminates early
q_thread = Thread.new{read_from_queue(consumers, $work_q)}
# q_thread.join


# q_thread needs time to start before worker threads can pull from $work_q
sleep 10

start_time = Time.now
puts "Start time: #{start_time}"

#Spin up selected number of threads, generate sources based on configs
workers = []
num_threads.times do |thread_num|
  log "starting Producer thread #{thread_num}"
  t = ProducerThread.new(LOGFILE, num_threads, thread_num, start_time)
  t.start($work_q, $topic_producer_hash)
  workers << t.thread
end

workers.map(&:join);

log "Done with the threads"
elapsed_time = Time.now - start_time
log "Total shards into kafka: #{num_shards} shards with #{num_threads} threads in #{elapsed_time} seconds. Avg time per shard: #{elapsed_time/num_shards}"
