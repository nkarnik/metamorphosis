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
end

opt_parser.parse!

#Multithreaded
work_q = Queue.new

#first arg = # of shards to pull
shards = $options[:num_shards] || 5

#second arg = environment (test/prod)
e = $options[:env] || "prod"

#third arg = topic name
topic = $options[:topic] || "s4"

num_threads = $options[:threads] || "5"
puts $options

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
  hash = JSON.parse(results)
  hash["rows"].map do |rec|
    rec.values.first[attrib]
  end
  puts hash
  return hash
end

a = find_broker(e)
ips = []

a["rows"]. each do |row|
  row.each do |k, v|
    ips << v["fqdn"]
  end
end

#parse role => ip mapping to get correct format for kafka broker
fqdns = []
ips.each do |ip|
  s = ip.split(".")
  t = s[0].split("-")
  fqdn = t[1, t.length].join(".") + ":9092"
  # puts fqdn
  fqdns << fqdn
end

if e == "local"
  fqdns = ["localhost:9092"]
end

puts "fqdns: #{fqdns}"

#create producer with list of domains


bucket = s3.buckets["fatty.zillabyte.com"]
tree = bucket.as_tree(:prefix => 'data/homepages/2014/0620')
directories = tree.children.select(&:branch?).collect(&:prefix)

st = Time.now

seed_brokers = fqdns
broker_pool = Poseidon::BrokerPool.new("fetch_metadata_client", seed_brokers)
cluster_metadata = Poseidon::ClusterMetadata.new
cluster_metadata.update(broker_pool.fetch_metadata([topic]))

metadata = cluster_metadata.topic_metadata[topic]
num_partitions = metadata.partition_count
leaders_per_partition = []
metadata.partition_count.times do |part_num|
  leaders_per_partition << cluster_metadata.lead_broker_for_partition(topic, 0).host
end

num_threads = num_partitions

log "done"

binding.pry

directories.each do |dir|
  puts "shards: #{shards}"
  puts "dir: #{dir}"
  break if shards == 0

  `rm -r /tmp/kfk2`
  `mkdir /tmp/kfk2`
  files = bucket.as_tree(:prefix => dir).children.select(&:leaf?).collect(&:key)
  files.each do |file|
    break if shards == 0
    work_q << file
    puts file
    shards -= 1
  end

  puts "\n \n"

  start = Time.now
  puts start
  workers = []
  num_threads.times do |thread_num|
    t = Thread.new do
      begin
        single_partitioner = Proc.new { |key, partition_count| thread_num  } # Will always right to a single partition

	producer = Poseidon::Producer.new([fqdns[thread_num % fqdns.size]], "producer_#{thread_num}", :type => :sync)
	puts "producer: #{producer} for node: #{fqdns[thread_num % fqdns.size]}"
        while f = work_q.pop(true)

            puts f
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
            gz.each_line do |line|
              begin
                producer.send_messages([Poseidon::MessageToSend.new(topic, line)])
              rescue Exception => e
                log "empty message error"
              end
            end
            diff = Time.now - st
            log(diff.to_s + "   " + path + "   shards left: " + shards.to_s + " current time: " + Time.now.to_s)
            
        end
      rescue ThreadError
      end
    end
    workers << t
  end

  workers.map(&:join); "ok"
  endt = Time.now
  puts endt - start
end
endt = Time.now
puts endt - st
