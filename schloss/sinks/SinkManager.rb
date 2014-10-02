require "aws-sdk"
require "thread"
require "json"
require "poseidon"
require "logger"

require_relative "../logging.rb"
module Metamorphosis
module Schloss
  include Logging
class SinkManager
  
  attr_reader :sinkTopic, :thread
  def initialize(sinktopic, fqdns, queues=[])
 
    @sinkTopic = sinktopic
    @thread = nil
    @sinkConsumer = nil
    @fqdns = fqdns
    @sinkQueues = queues

    leaders_per_partition = get_leaders_for_partitions(@sinkTopic, @fqdns)

    leader = leaders_per_partition.first
    info "Leader: #{leader}"

    @sinkConsumer = Poseidon::PartitionConsumer.new(@sinkTopic, leader.split(":").first, leader.split(":").last, queue_name, 0, :earliest_offset)

    info "Consumer : #{@sinkConsumer}"


  end

  def start()
    @thread = Thread.new do
      loop do
        begin
          info "Getting message now... consumer: #{@consumer}"
          messages = @sinkConsumer.fetch({:max_bytes => 1000000})
      
          info "Messages received: #{messages}"
          info "#{messages.length} messages received"
          messages.each do |m|
            message = m.value
            message = JSON.parse(message)
            topic = message["topic"]
            bucket = message["bucket"]
            key = message["key"]
            sink = Sink.new(topic, @fqdns, bucket, key)
            sink.start()
          end
        rescue Exception => e
          info "ERROR: #{e.message}"

        end


      end
    end
  end
end

#sinks to S3
class Sink

  attr_reader :topic
  def initialize(topic, fqdns, bucket, key)
    @log = Logger.new('| tee schloss.log', 10, 1024000)
    @log.datetime_format = '%Y-%m-%d %H:%M:%S'
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


