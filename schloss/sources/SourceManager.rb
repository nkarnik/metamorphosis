require "aws-sdk"
require "thread"
require "json"
require "poseidon"
require "logger"

require_relative "../../common/kafka_utils.rb"

require_relative "SourceFromS3.rb"
require_relative "SourceFromKinesis.rb"
require_relative "../logging.rb"

module Metamorphosis
module Schloss
  include Logging
class SourceManager

  attr_reader :sourceTopic, :thread
  def initialize(sourcetopic, fqdns, total_runs=0, queues, offset)

    AWS.config(
          :access_key_id    => 'AKIAJWZ2I3PMFF5O6PFA',
          :secret_access_key => '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'
          )

    @_s3 = AWS::S3.new

    @offset = offset || :earliest_offset
    @queues = queues
    @sourceTopic = sourcetopic
    @thread = nil
    @sourceConsumer = nil
    @fqdns = fqdns
    @total_runs = total_runs

    leaders_per_partition = get_leaders_for_partitions(@sourceTopic, @fqdns)
    
    info "Leaders: #{leaders_per_partition}"
    # Assuming one partition for this topic, find the singular leader
    leader = leaders_per_partition.first
    
    @sourceConsumer = Poseidon::PartitionConsumer.new("topic_consumer", leader.split(":").first, leader.split(":").last, @sourceTopic, 0, @offset)
    
    @shardWriter = Poseidon::Producer.new(@fqdns, "mockwriter", :type => :sync)
    
    con = 0
    
    
    @local_manifest = "manifest"
    if File.exists? @local_manifest
      File.delete @local_manifest
    end

    @run_num = 0
    @messages_consumed = 0
    info "Finished Creating source manager"
  end

  def start()
 
    begin
    @thread = Thread.new do
      info "Starting source manager"
      loop do
        begin
          messages = @sourceConsumer.fetch({:max_bytes => 100000}) # Timeout? 
          if messages.length == 0
            info "Waiting on message"
            sleep 3
            next
          end
          info " Got #{messages.length} messages and topic: #{@sourceTopic}"
          info "The next offset for the source consumer is: #{@sourceConsumer.next_offset}"
          messages.each do |m|
            message = m.value
            message = JSON.parse(message)
            info "Processing  message: #{message}"
      
            #bucket_name = message["source"]["config"]["bucket"]
            #manifest_path = message["source"]["config"]["manifest"]
            topic_to_write = message["topic"]

            @source = create_source(message)
            @source.write_to_manifest(@local_manifest)

            hosts = @fqdns.length
            info "Hosts #{hosts}"
            lines_consumed = 0
            File.open(@local_manifest).each do |line|
              #config = {:bucket => bucket_name, :shard => line.chomp}
              #source = {:type => sourcetype, :config => config}

              sourceinfo = @source.parse(line)
              info = {:source => sourceinfo, :topic => topic_to_write}.to_json
              #info = {:source => source, :topic => topic_to_write}.to_json
              hostnum = @messages_consumed % hosts
              broker_topic = @queues[hostnum]
              msgs = []
              info "Writing to topic: #{broker_topic} message: #{info}"
              
              msgs << Poseidon::MessageToSend.new(broker_topic, info)
              unless @shardWriter.send_messages(msgs)
                info "message send error: #{broker_topic}"
              end
              
              lines_consumed += 1
              @messages_consumed += 1
            end
            info "Number of shards emitted into #{@sourceTopic}: #{lines_consumed}"
            info "Total # of shards into #{@sourceTopic}: #{@messages_consumed}"
      
          end
        rescue => e
          error "ERROR: #{e.message}\n#{e.backtrace}"
        end
        
        @run_num += 1
      
        if @total_runs > 0
          if @run_num >= @total_runs
            info "Breaking because we wanted only #{@total_runs}, and completed #{@run_num} runs"
            break
          end
        end
        
        #info "Looping: #{run_num}"
      end
    end
    rescue Exception => e
      error  "error :: #{e.message}"
    end
  end

  def create_source(message)

    sourcetype = message["source"]["type"]
    if (sourcetype == "s3")
      return Metamorphosis::Schloss::SourceFromS3.new(message)
    elsif (sourcetype == "kinesis")
      return Metamorphosis::Schloss::SourceFromKinesis.new(message)
    end

  end

end
end
end
