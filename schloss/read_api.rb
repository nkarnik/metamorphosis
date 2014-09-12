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
  return JSON.parse(results)["rows"].map{ |a| a.map{|k,v| v["fqdn"]}  }
end

if env == "local"
  fqdns = ["localhost:9092"]
else
  fqdns = find_broker(env)
end

puts "fqdns: #{fqdns}"
consumers = []
con = 0
fqdns.each do |host|
  host = host[0]
  begin
    consumer = Poseidon::PartitionConsumer.new("con_#{con}", host, 9092, topic, 0, :earliest_offset)
    consumers << consumer 
  rescue
    puts "error"
  end
  con += 1
end

loop do
  consumers.each do |consumer|
    puts consumer, consumer.host
    begin
      messages = consumer.fetch({:max_bytes => 100000})
      puts messages
      messages.each do |m|
        puts m.value
        log m.value
      end
    rescue
      puts "error"
    end
  end
end


