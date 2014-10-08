require 'thread'
require 'aws-sdk'
require 'poseidon'
require 'json'
require 'logger'

require_relative 'SourceFromKinesis.rb'
require_relative 'SourceFromS3.rb'
require_relative 'RoundRobinByTopicMessageQueue.rb'
require_relative '../TopicProducerConfig.rb'
require_relative "../../common/kafka_utils.rb"
module Metamorphosis
module Workers
module Sources
  include Logging

class SourceThread
  attr_reader :thread, :sourcetype, :sourceconfig, :source, :topic

  SLEEP_BETWEEN_WORK_Q_LOOP = 3
  def initialize(num_threads, thread_num, start_time, max_loops = 0)

    @start_time = start_time
    @sourcetype = nil
    @sourceconfig = nil
    @source = nil
    @topic = nil
    @kafka_worker_topic_read_thread = nil
    @num_threads = num_threads
    @thread_num = thread_num
    @max_loops = max_loops
  end

  def createSource()
    if @sourcetype == "s3"
      source = Metamorphosis::Workers::Sources::SourceFromS3.new(@sourceconfig)
      return source

    elsif @sourcetype == "kinesis"
      source = Metamorphosis::Workers::Sources::SourceFromKinesis.new(@sourceconfig)
      return source
    end
  end

  def thread
    @kafka_worker_topic_read_thread
  end

  def start(work_q, topic_producer_hash)
    @work_q = work_q
    @kafka_worker_topic_read_thread = Thread.new do
      num_msgs_per_thread = 0
      num_loops = 0
      loop do
        num_loops += 1
        debug "Starting loop ..."
        if @max_loops > 0 and num_loops == @max_loops
          info "Reached #{@max_loops } loops, Exitting thread #{@thread_num}"
          info ""
          info " \tTotal Messages for this thread: #{num_msgs_per_thread}"
          info ""
          Thread.exit
        end
        if work_q.size == 0
          info "Num loops: #{num_loops} Waiting for more messages on work_q"
          sleep SLEEP_BETWEEN_WORK_Q_LOOP
          next
        end
        begin
          # We want producers exclusive to threads.
          # producers_for_thread = @producers.select.with_index{|_,i| i % num_threads == thread_num}
          # info "producer: #{producer} for node: #{producer_fqdn}"

          info "Number of shards left to push into kafka: #{work_q.size}"
          
          while work_q.size > 0 and message_and_offset = work_q.pop(true)
            message = message_and_offset[:message]
            offset = message_and_offset[:offset]
            info "Popped message with offset #{offset}"
            `echo #{offset} > offset#{@thread_num}`
            info "Queue size is #{work_q.size} with num: #{work_q.num}, and this many queues: #{work_q.queues.length} with info #{work_q.details}"
            @sourcetype = message["source"]["type"]
            @sourceconfig = message["source"]["config"]
            @source = createSource()
            @topic = message["topic"]
            debug "Source config: #{@sourceconfig}"
            debug " Source type: #{@sourcetype}"
            debug " Topic: #{@topic}"

            topicproducer = topic_producer_hash[@topic]
            partitions_for_thread = topicproducer.partitions_on_localhost.select.with_index{|_,partition_num| partition_num % @num_threads == @thread_num}

            #Get path of file where data is written to locally, and write data there
            #If the data doesn't write then exit
            per_shard_time = Time.now
            path = @source.local_file_path
            
            begin
              @source.get_data
            rescue Exception => e
              error "Failed data read. Moving on: #{path}"
              next
            end

            # Only does work for certain types of source (e.g. Kinesis)
            @source.push_next(@topic, @sourcetype, work_q, offset)

            #Select partition by sampling
            partition = partitions_for_thread.sample
            info "Writing to partition: #{partition}"
            producer = topicproducer.get_producer_for_partition(partition)

            datafile = open(path)
            info "Downloaded: #{path} in #{Time.now - per_shard_time}"
            gz = Zlib::GzipReader.new(datafile)
            msgs = []
            sent_messages = 0
            gz.each_line do |line|
              msgs << Poseidon::MessageToSend.new(@topic, gzip(line))
              # producer.send_messages([Poseidon::MessageToSend.new(topic, line)])
              # binding.pry
            end
            begin
              if producer.send_messages(msgs)
                sent_messages += msgs.size
                num_msgs_per_thread += sent_messages
              else
                info "Message failed: Batch size #{msgs.size} Sent from this shard: #{sent_messages}"
              end
            rescue Exception => e
              error "Error: #{e.message}"
            end

            # binding.pry
            # Send each shard to an arbitrary partition
            # producer.send_messages(msgs)
            File.delete path
            #shard_num += 1
            info( "#T: #{@thread_num} P#: #{partition}\t #{(Time.now - per_shard_time).round(2)}s \t#{num_msgs_per_thread} msgs. #{path.gsub("/tmp/", "")} \tshard: #{} \tRem:#{work_q.size}.  since: #{((Time.now - @start_time) / 60.0).round(2)} min")
          end #While end     
        rescue SystemExit, Interrupt, Exception => te
          info "Thread Completed: #{te.message}."
          info " "
          info " \tTotal Messages for this thread: #{num_msgs_per_thread}"
          info " "
          info " "
          if te.message.include? "queue empty"
            next
          else
            error "Error with thread #: #{@thread_num}"
            error te.message
            error te.backtrace.join("\n")
            Thread.exit
          end
        end
      end
    end
  end

end
end
end
end

