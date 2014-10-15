require "json"
require 'thread'
require "aws-sdk"
require "poseidon"
require "optparse"
require 'pry'

log = Logger.new('| tee schloss.log', 10, 1024000)
log.datetime_format = '%Y-%m-%d %H:%M:%S'

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
topic = $options[:topic] || "mock"

LOGFILE = "kafka_producer.log"
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
mockapi = Poseidon::Producer.new(fqdns, "mockapi", :type => :sync)

while true do

  puts "Enter type topic bucket shard_path shard_prefix:"
  raw = gets.chomp().split(" ")
  
  if raw[0] == "s3"
    config = {:bucket => raw[2], :shard_path => raw[3], :shard_prefix => raw[4]}
    sink = {:type => raw[0], :config => config}
    message = {:source => sink, :topic => raw[1]}.to_json

  elsif raw[0] == "kinesis"
    config = {:stream => raw[2]}
    source = {:type => raw[0], :config => config}
    message = {:source => source, :topic => raw[1]}.to_json
  end

  messages = []
  messages << Poseidon::MessageToSend.new(topic, message)
  mockapi.send_messages(messages)

end
