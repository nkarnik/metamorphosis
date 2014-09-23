workers = []
num_threads.times do |thread_num|

  t = Thread.new do
    num_msgs_per_thread = 0
    begin
      # We want producers exclusive to threads.
      # producers_for_thread = @producers.select.with_index{|_,i| i % num_threads == thread_num}
      # log "producer: #{producer} for node: #{producer_fqdn}"
      log "Number of shards left to push into kafka: " + work_q.size.to_s

      while s = work_q.pop(true)
        _s3 = AWS::S3.new
        # Bucket per thread
        _bucket = _s3.buckets[s["bucket"]]
        _topic = s["topic"]

        topicproducer = $topic_producer_hash[_topic]
        partitions_for_thread = topicproducer.partitions_on_localhost.select.with_index{|_,i| i % num_threads == thread_num}

        shard_num = 0

        f = s["shard"].chomp.gsub("s3://fatty.zillabyte.com/", "")
        topic = s["topic"]
        log "Preparing shard: " + f

        per_shard_time = Time.now
        path = "/tmp/" + f.split("/").last(2).join("_")

        begin

          File.open(path, 'wb') do |file|
            _bucket.objects[f].read do |chunk|
              begin
                file.write(chunk)
              rescue
                log "s3 error for path: #{f}"
              end
            end
          end
        rescue Exception => e
          log "Failed shard. Moving on: #{f}"
          next
        end
        partition = partitions_for_thread.sample
        producer = topicproducer.get_producer_for_partition(partition)

        s3file = open(path)
        # log "Downloaded: #{path} in #{Time.now - per_shard_time}"
        gz = Zlib::GzipReader.new(s3file)
        msgs = []
        sent_messages = 0
        batch_size = 1000
        gz.each_line do |line|
          begin
            msgs << Poseidon::MessageToSend.new(topic, line)
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
  workers << t
end

workers.map(&:join);
