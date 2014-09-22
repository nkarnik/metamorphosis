require 'json'
require 'thread'
require 'aws-sdk'
require 'poseidon'
require 'optparse'
require 'pry'
require './TopicProducerConfig'
require './SourceThread'
require_relative "../common/kafka_utils.rb"

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
queue_name =  "#{host}.ec2.internal"
puts queue_name

$options = {}

opt_parser = OptionParser.new do |opt|
  opt.banner = "Usage: --env "

  opt.on("--env ENV", String, "Required. Env? prod,test,local") do |env|
    $options[:env] = env
  end

  opt.on("--topic TOPIC", String, "Required. Topic? prod,test,local") do |topic|
    $options[:topic] = topic
  end

  opt.on("--threads THREADS",Integer, "Number of threads. Default: 1") do |threads|
    $options[:threads] = threads
  end

  opt.on("--shards SHARDS",Integer, "Optional. Number of shards to download. Default: All") do |shards|
    $options[:num_shards] = shards
  end

end

#Parse and set options
opt_parser.parse!
env = $options[:env] || "prod"

# $topic global is just for debugging
$topic = $options[:topic] || "some_homepages"

num_threads = $options[:threads].to_i || "1"
num_shards = $options[:num_shards].to_i || "10"
$bucket_name = "fatty.zillabyte.com"

$work_q = Queue.new

LOGFILE = "queue_consumer.log"
`touch #{LOGFILE}`


#pass in env (test/prod) and get fqdns of all kafka brokers

if env == "local"
  fqdns = ["localhost:9092"]
else
  fqdns = find_broker(env)
end

puts "fqdns: #{fqdns}"
consumers = []
producers = []

$topic_producer_hash = {}
seed_brokers = fqdns
$broker_pool = Poseidon::BrokerPool.new("fetch_metadata_client", seed_brokers)

#Create array of consumers to read from dedicated kafka worker queue for this broker
con = 0
fqdns.each do |host|
  consumer_host = host.split(":")[0]
  begin
    consumer = Poseidon::PartitionConsumer.new("con_#{con}", consumer_host, 9092, queue_name, 0, :earliest_offset)
    consumers << consumer
  rescue
    log "Error: couldn't create Consumer"
  end
  con += 1
end

def read_from_queue(cons, worker_q)
  loop do
    cons.each do |consumer|
      #Fetch messages from dedicated worker queue, push message to work_q
      begin
        messages = consumer.fetch({:max_bytes => 100000})
        messages.each do |m|
          message = m.value
          log message
          message = JSON.parse(message)

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
      rescue
        #puts "error"
      end
    end
  end
end

q_thread = Thread.new{read_from_queue(consumers, $work_q)}
#q_thread.join

sleep 5

# binding.pry

start_time = Time.now
puts "Start time: #{start_time}"

#Spin up selected number of threads, generate sources based on configs
workers = []
num_threads.times do |thread_num|

  t = ProducerThread.new(LOGFILE, num_threads, thread_num, start_time)
  t.start($work_q, $topic_producer_hash)
  workers << t.thread

end

workers.map(&:join);

log "Done with the threads"
elapsed_time = Time.now - start_time
log "Total shards into kafka: #{num_shards} shards with #{num_threads} threads in #{elapsed_time} seconds. Avg time per shard: #{elapsed_time/num_shards}"
