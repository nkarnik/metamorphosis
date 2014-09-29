require "aws-sdk"
require "thread"
require "json"
require "poseidon"
require "logger"

require_relative "../common/kafka_utils.rb"

class SourceManager

  attr_reader :sourceTopic, :thread
  def initialize(sourcetopic, logfile, fqdns, total_runs=0, queues)

    AWS.config(
          :access_key_id    => 'AKIAJWZ2I3PMFF5O6PFA',
          :secret_access_key => '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'
          )

    @_s3 = AWS::S3.new

    @queues = queues
    @sourceTopic = sourcetopic
    @thread = nil
    @logfile = logfile
    @sourceConsumer = nil
    @fqdns = fqdns
    @total_runs = total_runs

    leaders_per_partition = get_leaders_for_partitions(@sourceTopic, @fqdns)
    
    log "Leaders: #{leaders_per_partition}"
    # Assuming one partition for this topic, find the singular leader
    leader = leaders_per_partition.first
    
    @sourceConsumer = Poseidon::PartitionConsumer.new("topic_consumer", leader.split(":").first, leader.split(":").last, @sourceTopic, 0, :earliest_offset)
    
    @shardWriter = Poseidon::Producer.new(@fqdns, "mockwriter", :type => :sync)
    
    con = 0
    
    
    @local_manifest = "manifest"
    if File.exists? @local_manifest
      File.delete @local_manifest
    end

    @run_num = 0
    log "finished creating"
  end

  def log(msg)
    if @lf.nil?
      @lf = File.open(@logfile, 'a')
    end
    puts "#{Time.now}: #{msg}\n"
    @lf.write "#{Time.now}: #{msg}\n"
    @lf.flush
  end

  def start()
    log "about to start"
    begin
    @thread = Thread.new do
      log "starting source manager"
      loop do
        begin
          log "Waiting on message"
          messages = @sourceConsumer.fetch({:max_bytes => 100000}) # Timeout? 
          messages.each do |m|
            message = m.value
            message = JSON.parse(message)
            log "Processing  message: #{message}"
      
            bucket_name = message["source"]["config"]["bucket"]
            manifest_path = message["source"]["config"]["manifest"]
            topic_to_write = message["topic"]
            sourcetype = message["source"]["type"]
            bucket = @_s3.buckets[bucket_name]
            log "Downloading Manifest from #{manifest_path} for topic: #{topic_to_write} in bucket: #{bucket_name}"
            
            # If source type is s3 ... needed classes for this
            File.open(@local_manifest, 'wb') do |file|
              log "Opened file: #{file}"
              bucket.objects[manifest_path].read do |chunk|
                begin
                  file.write(chunk)
                rescue
                  log "s3 error for path: #{f}"
                end
              end
            end
            # log "Manifest length: #{`wc -l #{local_manifest}`}"
            hosts = @fqdns.length
            log "Hosts #{hosts}"
            lines_consumed = 0
            File.open(@local_manifest).each do |line|
              config = {:bucket => bucket_name, :shard => line.chomp}
              source = {:type => sourcetype, :config => config}
              info = {:source => source, :topic => topic_to_write}.to_json
              hostnum = lines_consumed % hosts
              broker_topic = @queues[hostnum]
              msgs = []
              log "Writing to topic: #{broker_topic} message: #{info}"
              
              msgs << Poseidon::MessageToSend.new(broker_topic, info)
              unless @shardWriter.send_messages(msgs)
                log "message send error: #{broker_topic}"
              end
              
              lines_consumed += 1
            end
            log "Number of shards emitted into #{@sourceTopic}: #{lines_consumed}"
      
      
          end
        rescue => e
          log "ERROR: #{e.message}\n#{e.backtrace}"
        end
        
        run_num += 1
      
        if @total_runs > 0
          if run_num >= total_runs
            log "Breaking because we wanted only #{total_runs}, and completed #{run_num} runs"
            break
          end
        end
        #log "Looping: #{run_num}"
      end
    end
    rescue
      log "error somewhere"
    end
  end
end
