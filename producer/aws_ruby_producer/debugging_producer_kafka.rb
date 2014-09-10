require "json"
require 'thread'
require "aws"
require "poseidon"
require "optparse"
require 'pry'

$options = {}
opt_parser = OptionParser.new do |opt|
  opt.banner = "Usage: --topic --env --shards --threads"
  opt.separator  ""
  opt.separator  "Options:"
    
  opt.on("--shards NUM",Integer, "Number of shards to download") do |num|
    $options[:num_shards] = num
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

  opt.on("--partition", String, "Produce to a specific partition") do |topic|
    $options[:partition] = partition
  end

end

opt_parser.parse!

#Multithreaded
work_q = Queue.new

#first arg = # of shards to pull
num_shards = $options[:num_shards].to_i || 5

#second arg = environment (test/prod)
env = $options[:env] || "prod"

#third arg = topic name
topic = $options[:topic] || "s4"

num_threads = $options[:threads].to_i || "5"
puts "Options: #{$options}"

LOGFILE = "kafka_producer.log"
`touch #{LOGFILE}`

def log(msg)
  if @lf.nil?
    @lf = File.open(LOGFILE, 'a')
  end
  puts msg
  @lf.write "#{Time.now}: #{msg}\n"
  @lf.flush
end

AWS.config(
          :access_key_id    => 'AKIAJWZ2I3PMFF5O6PFA',
          :secret_access_key => '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'
          )

s3 = AWS::S3.new

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

#create producer with list of domains

bucket = s3.buckets["fatty.zillabyte.com"]
tree = bucket.as_tree(:prefix => 'data/homepages/2014/0620')
directories = tree.children.select(&:branch?).collect(&:prefix)


`rm -r /tmp/kfk2`
`mkdir /tmp/kfk2`

shards = 1
directories.each do |dir|
  # puts "shards: #{shards}"
  # puts "dir: #{dir}"
  break if shards == num_shards
  files = bucket.as_tree(:prefix => dir).children.select(&:leaf?).collect(&:key)
  files.each do |file|
    work_q << file
    # log "Appended shard [#{shards}/#{num_shards}]: #{file}"
    break if shards == num_shards
    shards += 1
  end
end

log "\n#{shards.size} shards appended\n"


st = Time.now

seed_brokers = fqdns
broker_pool = Poseidon::BrokerPool.new("fetch_metadata_client", seed_brokers)
cluster_metadata = Poseidon::ClusterMetadata.new
cluster_metadata.update(broker_pool.fetch_metadata([topic]))

metadata = cluster_metadata.topic_metadata[topic]
num_partitions = metadata.partition_count
@leaders_per_partition = []
metadata.partition_count.times do |part_num|
  @leaders_per_partition << cluster_metadata.lead_broker_for_partition(topic, 0).host
end

log "Leaders per partition: #{@leaders_per_partition}"

# binding.pry

start = Time.now
puts "Start time: #{start}"

workers = []
num_partitions.times do |partition_num|
  
  t = Thread.new do
    begin
      single_partitioner = Proc.new { |key, partition_count| partition_num  } # Will always right to a single partition
      producer_fqdn = @leaders_per_partition[partition_num]
      producer = Poseidon::Producer.new([producer_fqdn], "producer_#{partition_num}", :type => :sync, :partitioner => single_partitioner)
      puts "producer: #{producer} for node: #{producer_fqdn}"
      while f = work_q.pop(true)
        per_shard_time = Time.now
        path = "/tmp/kfk2/" + f.split("/").last(2).join("_")
        File.open(path, 'wb') do |file|
          bucket.objects[f].read do |chunk|
            begin
              file.write(chunk)
            rescue
              log "s3 error"

            end
          end
        end
        s3file = open(path)

        gz = Zlib::GzipReader.new(s3file)
        msgs = []
        gz.each_line do |line|
          begin
            msgs << Poseidon::MessageToSend.new(topic, line)
          rescue Exception => e
            log "empty message error"
          end
        end 
        producer.send_messages(msgs)
        log( "#{Time.now.to_s}:: #{path} shards completed:  #{work_q.size}. per shard time: #{Time.now - per_shard_time}. Total time since start: #{Time.now - st}")

            
      end
    rescue ThreadError
    end
  end
  workers << t
end

workers.map(&:join); 

puts "Done with the threads"

endt = Time.now
puts endt - start

endt = Time.now
puts endt - st
