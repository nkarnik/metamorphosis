require "json"
require 'thread'
require "aws"
require "poseidon"

#Multithreaded
work_q = Queue.new


#first arg = # of shards to pull
shards = ARGV.first.to_i

#second arg = environment (test/prod)
e = ARGV[1].to_s

#third arg = topic name
topic = ARGV[2].to_s
puts topic


`touch log_kafka`

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
  puts fqdn
  fqdns << fqdn
end

if e == "local"
  fqdns = ["localhost:9092"]
end

puts fqdns

#create producer with list of domains


bucket = s3.buckets["fatty.zillabyte.com"]
tree = bucket.as_tree(:prefix => 'data/homepages/2014/0620')
directories = tree.children.select(&:branch?).collect(&:prefix)

st = Time.now

directories.each do |dir|
  puts shards
  puts dir
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
  10.times do |thread_num|
    t = Thread.new do
      begin
	producer = Poseidon::Producer.new([fqdns[thread_num % fqdns.size]], "st1", :type => :sync)
	puts "producer: #{producer} for node: #{fqdns[thread_num % fqdns.size]}"
        while f = work_q.pop(true)

            puts f
            path = "/tmp/kfk2/" + f.split("/").last(2).join("_")
            File.open(path, 'wb') do |file|
              bucket.objects[f].read do |chunk|
                begin
                  file.write(chunk)
                rescue
                  puts "s3 error"
                  `echo "s3 error!" >> log_kafka`
                end
              end
            end
            s3file = open(path)

            gz = Zlib::GzipReader.new(s3file)
            gz.each_line do |line|
              begin
                producer.send_messages([Poseidon::MessageToSend.new(topic, line)])
              rescue Exception => e
                puts "empty message error"
                `echo "empty message error!" >> log_kafka`
              end
            end
            diff = Time.now - st
            info = diff.to_s + "   " + path + "   shards left: " + shards.to_s + " current time: " + Time.now.to_s
            puts info
            `echo #{info} >> log_kafka`
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
