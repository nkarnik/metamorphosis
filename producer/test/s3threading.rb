require 'thread'
require "aws"

work_q = Queue.new
threads = ARGV.first

AWS.config(
          :access_key_id    => 'AKIAJWZ2I3PMFF5O6PFA',
          :secret_access_key => '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'
          )

s3 = AWS::S3.new

`rm -r /tmp/kfk2`
`mkdir /tmp/kfk2`

bucket = s3.buckets["fatty.zillabyte.com"]
tree = bucket.as_tree(:prefix => 'data/homepages/2014/0822')
directories = tree.children.select(&:branch?).collect(&:prefix)


files = bucket.as_tree(:prefix => directories.first).children.select(&:leaf?).collect(&:key)
files.each do |file|
  work_q << file
  puts file
end

puts "/n /n"

start = Time.now
puts start
workers = (0...5).map do
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
      end
    rescue ThreadError
    end
  end
end

workers.map(&:join); "ok"
endt = Time.now
puts endt - start
