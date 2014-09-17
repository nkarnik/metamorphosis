require 'json'
require 'thread'
require 'aws-sdk'
require 'poseidon'
require 'optparse'
require 'pry'
require './TopicProducerConfig'

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

s3 = AWS::S3.new

host = `hostname`.chomp
queue_name =  "#{host}.ec2.internaltest1"
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

work_q = Queue.new

LOGFILE = "queue_consumer.log"
`touch #{LOGFILE}`

#Class that houses configuration information for building producers for each thread
class TopicProducer
  attr_reader :leaders_per_partition, :partitions_on_localhost, :producer_hash
  def initialize(broker_pool, topic)
    cluster_metadata = Poseidon::ClusterMetadata.new
    cluster_metadata.update(broker_pool.fetch_metadata([topic]))
    metadata = cluster_metadata.topic_metadata[topic]
    num_partitions = metadata.partition_count

    @leaders_per_partition = []
    metadata.partition_count.times do |part_num|
      @leaders_per_partition << cluster_metadata.lead_broker_for_partition(topic, part_num).host
    end

    # log "Leaders per partition: #{leaders_per_partition}"
    partitions_per_leader = {}
    @leaders_per_partition.each_with_index do |ip, index|
      if partitions_per_leader[ip].nil?
        partitions_per_leader[ip] = []
      end
      partitions_per_leader[ip] << index
    end
    
    this_host = `hostname`.chomp() + ".ec2.internal"
    @partitions_on_localhost = partitions_per_leader[this_host] || []
    #log "Partitions on this host: #{@partitions_on_localhost.size}" 

    @producer_hash = Hash.new {|hash, partition| hash[partition] = get_producer_for_partition(partition)}
  end

  def get_producer_for_partition(partition_num)
    single_partitioner = Proc.new { |key, partition_count| partition_num  } # Will always right to a single partition
    producer_fqdn = @leaders_per_partition[partition_num]
    return Poseidon::Producer.new([producer_fqdn.split(".").first.gsub("ip-", "").gsub("-",".") + ":9092"],
                                  "producer_#{partition_num}",
                                  :type => :sync,
                                  :partitioner => single_partitioner,
                                  # :snappy Unimplemented in Poseidon. Should be easy to add into
                                  # https://github.com/bpot/poseidon/blob/master/lib/poseidon/compression/snappy_codec.rb
                                  :compression_codec => :gzip)
  end
end

#pass in env (test/prod) and get fqdns of all kafka brokers
def find_broker(env, attrib = 'fqdn')
  results = `knife search node "role:kafka_broker AND chef_environment:#{env}" -F json -a fqdn  -c ~/zb1/infrastructure/chef/.chef/knife.rb`
  return JSON.parse(results)["rows"].map{ |a| a.map{|k,v| v["fqdn"]}  }.map{|v| "#{v[0]}:9092"}
end

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
          #log message
          message = JSON.parse(message)
  
          topic = message["topic"]          
          worker_q << message
          #log worker_q.size
          
          # build TopicProducer configuration object for topic 
          if not $topic_producer_hash.has_key?(topic)
            log "Creating new producer for #{topic}"
            topicproducer = TopicProducerConfig.new($broker_pool, topic) 
            $topic_producer_hash[topic] = topicproducer 
            a = topicproducer.partitions_on_localhost
            #log "Finished creating producer for #{topic}"
          end
        end
      rescue
        #puts "error"
      end
    end
  end
end

q_thread = Thread.new{read_from_queue(consumers, work_q)}
#q_thread.join

sleep 5

# binding.pry

start_time = Time.now
puts "Start time: #{start_time}"

workers = []
num_threads.times do |thread_num|

  t = Thread.new do
    num_msgs_per_thread = 0
    begin
      # We want producers exclusive to threads.
      # producers_for_thread = @producers.select.with_index{|_,i| i % num_threads == thread_num}
      # log "producer: #{producer} for node: #{producer_fqdn}"
      log "Number of shards left to push into kafka: " + work_q.size.to_s
 
      while s = work_q.pop(true)
        _s3 = AWS::S3.new
        # Bucket per thread
        _bucket = _s3.buckets[s["bucket"]]
        _topic = s["topic"]

        topicproducer = $topic_producer_hash[_topic]      
        partitions_for_thread = topicproducer.partitions_on_localhost.select.with_index{|_,i| i % num_threads == thread_num}

        shard_num = 0

        f = s["shard"].chomp.gsub("s3://fatty.zillabyte.com/", "")
        topic = s["topic"]
        log "Preparing shard: " + f
        
        per_shard_time = Time.now
        path = "/tmp/" + f.split("/").last(2).join("_")

        begin

          File.open(path, 'wb') do |file|
            _bucket.objects[f].read do |chunk|
              begin
                file.write(chunk)
              rescue
                log "s3 error for path: #{f}"
              end
            end
          end
        rescue Exception => e
          log "Failed shard. Moving on: #{f}"
          next
        end
        partition = partitions_for_thread.sample
        producer = topicproducer.get_producer_for_partition(partition)

        s3file = open(path)
        # log "Downloaded: #{path} in #{Time.now - per_shard_time}"
        gz = Zlib::GzipReader.new(s3file)
        msgs = []
        sent_messages = 0
        batch_size = 1000
        gz.each_line do |line|
          begin
            msgs << Poseidon::MessageToSend.new(topic, line)
            # producer.send_messages([Poseidon::MessageToSend.new(topic, line)])
            # binding.pry
          rescue Exception => e
            log "Error: #{e.message}"
          end
        end
        if producer.send_messages(msgs)
          sent_messages += msgs.size
          num_msgs_per_thread += sent_messages
        else
          log "Message failed: Batch size #{msgs.size} Sent from this shard: #{sent_messages}"
        end

        # binding.pry
        # Send each shard to an arbitrary partition
        # producer.send_messages(msgs)
        File.delete path
        shard_num += 1
        log( "#T: #{thread_num} P#: #{partition}\t #{(Time.now - per_shard_time).round(2)}s \t#{num_msgs_per_thread} msgs. #{path.gsub("/tmp/", "")} \tshard: #{} \tRem:#{work_q.size}.  since: #{((Time.now - start_time) / 60.0).round(2)} min")
      end
    rescue SystemExit, Interrupt, Exception => te
      log "Thread Completed: #{te.message}.\n\n\tTotal Messages for this thread: #{num_msgs_per_thread}\n\n"
      puts te.backtrace unless te.message.include?("queue empty")
      Thread.exit
    end
  end
  workers << t
end

workers.map(&:join);

log "Done with the threads"
elapsed_time = Time.now - start_time
log "Total shards into kafka: #{num_shards} shards with #{num_threads} threads in #{elapsed_time} seconds. Avg time per shard: #{elapsed_time/num_shards}"
