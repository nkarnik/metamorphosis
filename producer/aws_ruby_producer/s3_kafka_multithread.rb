require 'thread'
require "aws"
require "poseidon"

work_q = Queue.new
shards = ARGV.first.to_i
`touch log_kafka`

AWS.config(
          :access_key_id    => 'AKIAJWZ2I3PMFF5O6PFA',
          :secret_access_key => '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'
          )

s3 = AWS::S3.new

producer = Poseidon::Producer.new(["10.167.177.28:9092", "10.184.165.95:9092" ], "st1", :type => :sync)


bucket = s3.buckets["fatty.zillabyte.com"]
tree = bucket.as_tree(:prefix => 'data/homepages/2014/0620')
directories = tree.children.select(&:branch?).collect(&:prefix)

st = Time.now

directories.each do |dir|
  puts shards
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

  puts "/n /n"

  start = Time.now
  puts start
  workers = (0...15).map do
    Thread.new do
      begin
        while f = work_q.pop(true)
            puts f
            path = "/tmp/kfk2/" + f.split("/").last(2).join("_")
            File.open(path, 'wb') do |file|
              bucket.objects[f].read do |chunk|
                file.write(chunk)
              end
            end
            s3file = open(path)

            gz = Zlib::GzipReader.new(s3file)
            gz.each_line do |line|
              producer.send_messages([Poseidon::MessageToSend.new("nk8", line)])
            end
            diff = Time.now - st
            info = diff.to_s + "   " + path + "   shards left:   " + shards.to_s
            puts info
            `echo #{info} >> log_kafka`
        end
      rescue ThreadError
      end
    end
  end

  workers.map(&:join); "ok"
  endt = Time.now
  puts endt - start
end
endt = Time.now
puts endt - st
