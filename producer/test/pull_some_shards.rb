# /usr/bin/ruby
require 'poseidon'
require 'aws-sdk'

num_shards_to_pull = ARGV[0]
num_threads = ARGV[1]


AWS.config(
           :access_key_id     => 'AKIAJWZ2I3PMFF5O6PFA',
           :secret_access_key => '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'
          )
s3 = AWS::S3.new
producer = Poseidon::Producer.new(["10.167.177.28:9092", "10.184.165.95:9092" ], "st1", :type => :sync)

puts "Reading dir list from s3."

bucket = s3.buckets["fatty.zillabyte.com"]
tree = bucket.as_tree(:prefix => 'data/homepages/2014/0620')
directories = tree.children.select(&:branch?).collect(&:prefix)
puts "Found top level dirs: #{directories.length}"
puts "Pumping in the first #{num_shards_to_pull} shards"

shard_count = 0
directories.each do |dir|
  bucket.as_tree(:prefix => dir).children.select(&:leaf?).collect(&:key).each do |shard_path|
    shard_count += 1
    temp_file_name = shard_path.split("/").last(2).join("_")
    File.open(temp_file_name, 'wb') do |file|
      bucket.objects[shard_path].read do |chunk|
        file.write(chunk)
      end
    end
    # File is downloaded 
    gz = Zlib::GzipReader.new(open(temp_file_name))
    gz.each_line do |line|
      producer.send_messages([Poseidon::MessageToSend.new("some_homepages", line)])
    end
    puts "Shard #{temp_file_name} completed. Total files done: #{shard_count}"
    File.delete(temp_file_name)
    if shard_count == num_shards_to_pull
      abort("Completed #{shard_count} shards. Goodbye!")
    end
  end
end