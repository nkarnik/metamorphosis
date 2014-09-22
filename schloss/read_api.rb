require "json"
require 'thread'
require "aws-sdk"
require "poseidon"
require "optparse"
require 'pry'
require_relative("../common/kafka_utils.rb")
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

$options = {}

opt_parser = OptionParser.new do |opt|
  opt.banner = "Usage: --env "

  opt.on("--env ENV", String, "Required. Env? prod,test,local") do |env|
    $options[:env] = env
  end

  opt.on("--topic TOPIC", String, "Required. Topic? prod,test,local") do |topic|
    $options[:topic] = topic
  end
end

opt_parser.parse!
env = $options[:env] || "prod"
topic = $options[:topic] || some_homepages

LOGFILE = "kafka_consumer.log"
`touch #{LOGFILE}`

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

leaders_per_partition = get_leaders_for_partitions(topic, fqdns)

log "Leaders: #{leaders_per_partition}"
# Assuming one partition for this topic, find the singular leader
leader = leaders_per_partition.first

consumer = Poseidon::PartitionConsumer.new("topic_consumer", leader.split(":").first, 9092, topic, 0, :earliest_offset)

shard_writer = Poseidon::Producer.new(fqdns, "mockwriter", :type => :sync)

con = 0
# fqdns.each do |host|
#   consumer_host = host.split(":")[0]
#   puts host
#   begin
#     #producer = Poseidon::Producer.new([host],"con_test#{host}", :type => :sync)
#     #producers << producer

#     consumer = Poseidon::PartitionConsumer.new("con_#{con}", consumer_host, 9092, topic, 0, :earliest_offset)
#     consumers << consumer
#   rescue
#     #puts "error"
#   end
#   con += 1
# end

local_manifest = "manifest"

loop do
  begin
    messages = consumer.fetch({:max_bytes => 100000}) # Timeout? 

    messages.each do |m|
      message = m.value
      message = JSON.parse(message)
      bucket_name = message["source"]["config"]["bucket"]
      manifest_path = message["source"]["config"]["manifest"]
      topic_to_write = message["topic"]
      sourcetype = message["source"]["type"]
      bucket = s3.buckets[bucket_name]
      log "Downloading Manifest from #{manifest_path} for topic: #{topic_to_write} in bucket: #{bucket_name}"
      
      # If source type is s3
      File.open(local_manifest, 'wb') do |file|
        bucket.objects[manifest_path].read do |chunk|
          begin
            file.write(chunk)
          rescue
            log "s3 error for path: #{f}"
          end
        end
      end
      log "Manifest length: #{`wc -l #{local_manifest}`}"
      hosts = fqdns.length
      lines_consumed = 0
      File.open(local_manifest).each do |line|
        config = {:bucket => bucket_name, :shard => line}
        source = {:type => sourcetype, :config => config}
        info = {:source => source, :topic => topic_to_write}.to_json
        hostnum = lines_consumed % hosts

        broker_topic = fqdns[hostnum].split(":").first
        msgs = []
        msgs << Poseidon::MessageToSend.new(broker_topic, info)
        shard_writer.send_messages(msgs)
        
        lines_consumed += 1
      end
      log "Number of shards emitted into #{topic}: #{lines_consumed}"
    end
  rescue => e
    log "ERROR: #{e.message}\n#{e.backtrace}"
  end

end
