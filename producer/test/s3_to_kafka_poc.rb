# /usr/bin/ruby
require 'poseidon'

start = Time.now
puts start
`rm -r /tmp/kfk`
`mkdir /tmp/kfk`
`s3cmd ls --recursive s3://fatty.zillabyte.com/data/homepages/2014/0822 > temp_list`
`head -n 100 temp_list | awk '{print $4}' > shards_list`
`while read l; do
  echo $l
  s3cmd get $l /tmp/kfk
 done <shards_list
`
second = Time.now
puts second
puts second - start

producer = Poseidon::Producer.new(["10.167.177.28:9092", "10.184.165.95:9092" ], "st1", :type => :sync)

files = Dir["/tmp/kfk/*"]
count = 0

files.each do |file|
  puts file
  count += 1
  s3file = open(file)

  gz = Zlib::GzipReader.new(s3file)
  gz.each_line do |line|
    producer.send_messages([Poseidon::MessageToSend.new("some_homepages", line)])
  end
  puts count
  finish = Time.now
  puts finish
  puts finish - second
  second = finish

end
