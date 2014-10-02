require "aws-sdk"
require "thread"
require "json"
require "poseidon"
require "logger"

require_relative "../logging.rb"
#sinks to S3
module Metamorphosis
module Schloss
class Sink

  attr_reader :topic
  def initialize(topic, fqdns, bucket, key)

    AWS.config(
               :access_key_id    => 'AKIAJWZ2I3PMFF5O6PFA',
               :secret_access_key => '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'
               ) 

    @_key = key
    @topic = topic
    @thread = nil
    @shardNum = 0
    
    leaders_per_partition = get_leaders_for_partitions(@topic, fqdns)
    leader = leaders_per_partition.first
    info "Leader: #{leader}"

    @consumer = Poseidon::PartitionConsumer.new(@topic, leader.split(":").first, leader.split(":").last, queue_name, 0, :earliest_offset)

    info "Consumer : #{@consumer}"

    @_s3 = AWS::S3.new
    @_bucket = @_s3.buckets[bucket] 

  end

  def start()
    
    @thread = Thread.new do
      #read from topic and sink
      loop do
        begin
          info "Getting message now... consumer: #{@consumer}"
          
          #TODO set batch size for max_bytes
          messages = @consumer.fetch({:max_bytes => 1000000})
      
          info "Messages received: #{messages}"
          info "#{messages.length} messages received"
          msgsToSink = []
          messages.each do |m|
            message = m.value
            message = JSON.parse(message)
            #TODO parse message into writable tupl
            msgsToSink << message 
          end
          sink(msgsToSink)

        rescue Exception => e
          error "ERROR: #{e.message}"
        end

      end
    end
  end

  #sink to S3 or whatever here, after batching messages
  def sink(messages)
    
    #first write messages to local file
    @path = "/tmp/" + @_key + ".gz"
    s3path = @_key + @shardNum.to_s + ".gz"
    
    begin
      
      File.open(@path, 'wb') do |file|
        messages.each do |tuple|
          file.write(tuple)
          file.write("\n")
        end
      end

      s3writer = @_bucket.objects[s3path]
      s3writer.write(Pathname.new(@path))

      File.delete @path
      @shardNum += 1

    rescue Exception => e
      error "S3 shard upload error: #{e.message}"

    end

  end
end
end
end
