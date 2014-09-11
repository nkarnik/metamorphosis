require "json"
require 'thread'
require "aws-sdk"
require "poseidon"
require "optparse"
require 'pry'

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
bucket = s3.buckets["fatty.zillabyte.com"]

$options = {}
opt_parser = OptionParser.new do |opt|
  opt.banner = "Usage: --topic --env --shards --threads"
  opt.separator  ""
  opt.separator  "Options:"
     
  opt.on("--shards SHARDS",Integer, "Number of shards to download") do |shards|
    $options[:num_shards] = shards
  end

  opt.on("--broker_id BROKER",Integer, "Which broker is this?") do |broker|
    $options[:broker_id] = broker.to_i
  end
  
  opt.on("--threads THREADS",Integer, "Number of threads") do |threads|
    $options[:threads] = threads
  end

  opt.on("--env ENV", String, "Env? prod,test,local") do |env|
    $options[:env] = env
  end
  
  opt.on("--topic TOPIC", String, "Set the AMI to use") do |topic|
    $options[:topic] = topic
  end
end

opt_parser.parse!

#Multithreaded
work_q = Queue.new

num_shards    = $options[:num_shards].to_i
env           = $options[:env] || "prod"
topic         = $options[:topic] || "s4"
num_threads   = $options[:threads].to_i || "5"

puts "Options: #{$options}"

LOGFILE = "kafka_producer.log"
`touch #{LOGFILE}`


def find_broker(env, attrib = 'fqdn')
  results = `knife search node "role:kafka_broker AND chef_environment:#{env}" -F json -a fqdn  -c ~/zb1/infrastructure/chef/.chef/knife.rb`
  return JSON.parse(results)["rows"].map{ |a| a.map{|k,v| v["fqdn"]}  }.map{|v| "#{v[0]}:9092"}
end
#parse role => ip mapping to get correct format for kafka broker


if env == "local"
  fqdns = ["localhost:9092"]
else
  fqdns = find_broker(env)
end

puts "fqdns: #{fqdns}"





## Manifest distribution
manifest_path = "data/homepages/2014/0620.manifest"
local_manifest = "manifest"

log "Downloading manifest"

File.open(local_manifest, 'wb') do |file|
  bucket.objects[manifest_path].read do |chunk|
    begin
      file.write(chunk)
    rescue
      log "s3 error for path: #{f}"
    end
  end
end

log "Distributing manifest. Looking for #{num_shards} shards"

line_num = 1
if $options[:broker_id]
  File.open(local_manifest) do |file|
    file.each_line do |line|
      break if work_q.size == num_shards
      if line_num % fqdns.size == $options[:broker_id]
        work_q << line.chomp.gsub("s3://fatty.zillabyte.com/", "") # Only need path
      end
      line_num += 1
    end
  end
end

log "Manifest distributed, in queue: #{work_q.size}"
# binding.pry
return if work_q.size == 0

seed_brokers = fqdns
broker_pool = Poseidon::BrokerPool.new("fetch_metadata_client", seed_brokers)
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
log "Partitions on this host: #{@partitions_on_localhost.size}"
# log @partitions_per_leader

def get_producer_for_partition(partition_num)
  single_partitioner = Proc.new { |key, partition_count| partition_num  } # Will always right to a single partition
  producer_fqdn = @leaders_per_partition[partition_num]
  return Poseidon::Producer.new([producer_fqdn.split(".").first.gsub("ip-", "").gsub("-",".") + ":9092"], "producer_#{partition_num}", :type => :sync, :partitioner => single_partitioner)
end

# @producers = @partitions_on_localhost.map{|partition| get_producer_for_partition(partition)}
@producer_hash = Hash.new {|hash, partition| hash[partition] = get_producer_for_partition(partition)}

# log @producers
# log "#{@producers.size} producers created"
# binding.pry

start_time = Time.now
puts "Start time: #{start_time}"

workers = []
num_threads.times do |thread_num|
  
  t = Thread.new do
    begin
      # We want producers exclusive to threads. 
      # producers_for_thread = @producers.select.with_index{|_,i| i % num_threads == thread_num}
      # log "producer: #{producer} for node: #{producer_fqdn}"
      num = 0
      while f = work_q.pop(true)
        per_shard_time = Time.now
        path = "/tmp/" + f.split("/").last(2).join("_")
        File.open(path, 'wb') do |file|
          bucket.objects[f].read do |chunk|
            begin
              file.write(chunk)
            rescue
              log "s3 error for path: #{f}"
            end
          end
        end

        partition = @partitions_on_localhost.sample
        producer = @producer_hash[partition]

        s3file = open(path)
        log "Downloaded: #{path} in #{Time.now - per_shard_time}"
        gz = Zlib::GzipReader.new(s3file)
        msgs = []
        sent_messages = 0
        batch_size = 100
        gz.each_line do |line|
          begin
            msgs << Poseidon::MessageToSend.new(topic, "#{num}"+line[0..1000])
            num += 1
            if(msgs.size == batch_size)
              unless producer.send_messages(msgs)
                log "Message failed: Line length #{line.size}"
              end
              sent_messages += batch_size
              msgs = []
            end
            # producer.send_messages([Poseidon::MessageToSend.new(topic, line)])
            # binding.pry
          rescue Exception => e
            log "Error: #{e.message}"
          end
        end 

        # binding.pry
        # Send each shard to an arbitrary partition
        # producer.send_messages(msgs)
        File.delete path

        log( "Partition: #{partition} took: #{Time.now - per_shard_time} seconds for #{sent_messages} msgs. #{path} shards remaing: #{work_q.size}.  Since start: #{Time.now - start_time}")
        
            
      end
    rescue ThreadError => te
      log "Thread Errored: #{te.message}"
    end
  end
  workers << t
end

workers.map(&:join); 

log "Done with the threads"
elapsed_time = Time.now - start_time
log "Total shards into kafka: #{num_shards} shards with #{num_threads} threads in #{elapsed_time} seconds. Avg time per shard: #{elapsed_time/num_shards}"
