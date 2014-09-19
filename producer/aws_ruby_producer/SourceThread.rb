require 'thread'
require 'aws-sdk'
require 'poseidon'
require 'json'
require './TopicProducerConfig'

class ProducerThread
  attr_reader :thread, :sourcetype, :sourceconfig, :source, :topic
  def initialize(logfile)
    @sourcetype = nil
    @sourceconfig = nil
    @source = nil
    @topic = nil
    @thread = nil
    @logfile = LOGFILE

  end

  def log(msg)
    if @lf.nil?
      @lf = File.open(@logfile, 'a')
    end
    puts "#{Time.now}: #{msg}\n"
    @lf.write "#{Time.now}: #{msg}\n"
    @lf.flush
  end

  def createSource()
    if @sourcetype == "s3"
      source = S3Source.new(@sourceconfig, @logfile)
      return source
    end
  end

  def start(work_q, topic_producer_hash)
    @thread = Thread.new do
      num_msgs_per_thread = 0
      begin
        # We want producers exclusive to threads.
        # producers_for_thread = @producers.select.with_index{|_,i| i % num_threads == thread_num}
        # log "producer: #{producer} for node: #{producer_fqdn}"
        log "Number of shards left to push into kafka: " + work_q.size.to_s

        while s = work_q.pop(true)
          @sourcetype = s["sourcetype"]
          @sourceconfig = s["sourceconfig"]
          @source = createSource()
          @topic = s["topic"]

          topicproducer = topic_producer_hash[@topic]
          partitions_for_thread = topicproducer.partitions_on_localhost.select.with_index{|_,i| i % num_threads == thread_num}

          #Get path of file where data is written to locally, and write data there
          #If the d
          path = @source.path

          begin
            @source.getData()
          rescue Exception => e
            log "Failed data read. Moving on: #{path}"
            next
          end


          #Select partition by sampling
          partition = partitions_for_thread.sample
          producer = topicproducer.get_producer_for_partition(partition)

          datafile = open(path)
          # log "Downloaded: #{path} in #{Time.now - per_shard_time}"
          gz = Zlib::GzipReader.new(datafile)
          msgs = []
          sent_messages = 0
          batch_size = 1000
          gz.each_line do |line|
            begin
              msgs << Poseidon::MessageToSend.new(@topic, line)
              # producer.send_messages([Poseidon::MessageToSend.new(topic, line)])
              # binding.pry
            rescue Exception => e
              log "Error: #{e.message}"
            end
          end
          if producer.send_messages(msgs)
            sent_messages += msgs.size
            num_msgs_per_thread += sent_messages
          else
            log "Message failed: Batch size #{msgs.size} Sent from this shard: #{sent_messages}"
          end

          # binding.pry
          # Send each shard to an arbitrary partition
          # producer.send_messages(msgs)
          File.delete path
          shard_num += 1
          log( "#T: #{thread_num} P#: #{partition}\t #{(Time.now - per_shard_time).round(2)}s \t#{num_msgs_per_thread} msgs. #{path.gsub("/tmp/", "")} \tshard: #{} \tRem:#{work_q.size}.  since: #{((Time.now - start_time) / 60.0).round(2)} min")
        end
      rescue SystemExit, Interrupt, Exception => te
        log "Thread Completed: #{te.message}.\n\n\tTotal Messages for this thread: #{num_msgs_per_thread}\n\n"
        puts te.backtrace unless te.message.include?("queue empty")
        Thread.exit
      end
    end
  end
end

class KafkaSource

  def initialize(sourceconfig, logfile)
    #validate input of

    @logfile = logfile

  end

  def log(msg)
    if @lf.nil?
      @lf = File.open(@logfile, 'a')
    end
    puts "#{Time.now}: #{msg}\n"
    @lf.write "#{Time.now}: #{msg}\n"
    @lf.flush
  end

  def getData()
    puts @logfile
    #do some generic stuff
  end

end


class S3Source < KafkaSource
  attr_reader :path
  def initialize(sourceconfig, logfile)
    AWS.config(
              :access_key_id    => 'AKIAJWZ2I3PMFF5O6PFA',
              :secret_access_key => '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'
              )

    @logfile = logfile
    @_s3 = AWS::S3.new
    @_bucket = @_s3.buckets[sourceconfig["bucket"]]

    f = sourceconfig["shard"].chomp.gsub("s3://#{sourceconfig["bucket"]}/", "")
    log "Preparing shard"
    per_shard_time = Time.now
    @path = "/tmp/" + f.split("/").last(2).join("_")

  end

  def getData()

    begin

      File.open(@path, 'wb') do |file|
        @_bucket.objects[f].read do |chunk|
          begin
            file.write(chunk)
          rescue
            log "s3 error for path: #{f}"
          end
        end
      end
    rescue Exception => e
      log "Failed shard. Moving on: #{f}"
    end
  end
end
