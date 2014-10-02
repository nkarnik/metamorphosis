#!/usr/bin/env ruby
require 'json'
require 'thread'
require 'aws-sdk'
require 'poseidon'
require 'optparse'
require 'pry'
require 'logger'

require './TopicProducerConfig'
require_relative 'sources/SourceThread.rb'
require_relative 'sources/RoundRobinByTopicMessageQueue.rb'
require_relative "../common/kafka_utils.rb"
include Metamorphosis::Workers::Logging


SLEEP_BETWEEN_LOOP = 3

AWS.config(
          :access_key_id    => 'AKIAJWZ2I3PMFF5O6PFA',
          :secret_access_key => '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'
          )


host = `hostname`.chomp

$options = {}

opt_parser = OptionParser.new do |opt|
  opt.banner = "Usage: --env "

  opt.on("--env ENV", String, "Required. Env? prod,test,local") do |env|
    $options[:env] = env
  end

  opt.on("--queue_name QUEUE", String, "Optional, name of the queue to read from. Defaults to host fqdn") do |queue|
    $options[:queue_name] = queue
  end

  opt.on("--threads THREADS",Integer, "Number of threads. Default: 1") do |threads|
    $options[:threads] = threads
  end

  opt.on("--shards SHARDS",Integer, "Optional. Number of shards to download. Default: All") do |shards|
    $options[:num_shards] = shards
  end

  opt.on("--runs RUNS",Integer, "Optional. Number of shards to download. Default: All") do |runs|
    $options[:runs] = runs
  end

  opt.on("--brokers BROKERS",String, "Optional, for local send in the brokers") do |brokers|
    $options[:brokers] = brokers
  end

  opt.on("--no_recovery", "Optional, for local send in the brokers") do
    $options[:no_recovery] = true
  end
end

#Parse and set options
opt_parser.parse!
@env = $options[:env] || "local"

queue_name = $options[:queue_name] || "#{host}.ec2.internal"
if $options[:no_recovery]
  @offset = :earliest_offset
else
  @offset = `cat offset*`.split().min.to_i || :earliest_offset
  log.info "Recovered offset is #{@offset}"
end
num_threads = $options[:threads].to_i || "1"
num_shards = $options[:num_shards].to_i || 0
brokers = $options[:brokers] || ["localhost:9092"]
runs = $options[:runs] || 0
$work_q = Metamorphosis::Workers::Sources::RoundRobinByTopicMessageQueue.new

#pass in env (test/prod) and get fqdns of all kafka brokers

if @env == "local"
  fqdns = brokers.split(",")
else
  fqdns = find_broker(@env)
end


consumers = []
producers = []

log.info "Config:::"
log.info "  env: #{@env}"
log.info "  Queue: #{queue_name}"
log.info "  Threads: #{num_threads}"
log.info "  seed brokers: #{fqdns}"
log.info "  runs: #{runs}"
$topic_producer_hash = {}
seed_brokers = fqdns
$broker_pool = Poseidon::BrokerPool.new("fetch_metadata_client", seed_brokers)

#Create array of consumers to read from dedicated kafka worker queue for this broker
con = 0
leaders_per_partition = get_leaders_for_partitions(queue_name, fqdns)

leader = leaders_per_partition.first
log.info "Leader: #{leader}"

@consumer = Poseidon::PartitionConsumer.new(queue_name, leader.split(":").first, leader.split(":").last, queue_name, 0, @offset)
log.info "Consumer created for #{queue_name} #{leader}"


#SET FLAG -> reading from queue should either be one time limited or threaded -- if env is local, thread terminates early
q_thread = Thread.new{
  thread_start_time = Time.now
  messages_processed = 0
  num_loops = 0
  loop do
    #Fetch messages from dedicated worker queue, push message to work_q
    # Assuming one partition for this topic, find the singular leader
    # if num_shards > 0 and  messages_processed == num_shards
    #   log.info "Completed processing #{num_shards}. Stopping read queue"
    #   Thread.exit
    # end
    if runs > 0 and num_loops == runs
      log.info "Completed #{runs} runs of the Kafka topic read thread. Exitting..."
      Thread.exit
    end
    begin
      messages = @consumer.fetch({:max_bytes => 1000000})
      messages.each do |m|
        message = m.value
        message = JSON.parse(message)
        message_and_offset = {:message => message, :offset => m.offset}
        topic = message["topic"]
        log.info "Processing message for topic: #{topic}"
        log.info "  message: #{message[0..30]}..."
        $work_q.push(message_and_offset)
        # build TopicProducer configuration object for topic
        if not $topic_producer_hash.has_key?(topic)
          log.debug "Creating new producer for #{topic}"
          topicproducer = Metamorphosis::Workers::TopicProducerConfig.new($broker_pool, topic)
          $topic_producer_hash[topic] = topicproducer
          a = topicproducer.partitions_on_localhost
          log.debug "Finished creating producer for #{topic}"
        end
        messages_processed += 1

      end
    rescue Exception => e
      log.error "ERROR: #{e.message}"
      log.error e.backtrace.join("\n")
    end
    num_loops += 1
    log.info "Request Queue Msgs Processed: #{messages_processed}, Num Loops: #{num_loops} Waiting for more messags... "
    sleep SLEEP_BETWEEN_LOOP

    #make sure we're not hammering kafka
  end

}

#q_thread.join
# sleep 1
# q_thread needs time to start before worker threads can pull from $work_q

start_time = Time.now
puts "Start time: #{start_time}"

#Spin up selected number of threads, generate sources based on configs
workers = []
num_threads.times do |thread_num|
  log.info "starting Producer thread #{thread_num}"
  source_thread = Metamorphosis::Workers::Sources::SourceThread.new(num_threads, thread_num, start_time, runs)
  source_thread.start($work_q, $topic_producer_hash)
  workers << source_thread.thread
end
# sleep 1
workers.map(&:join);

log.info "Done with the threads"
elapsed_time = Time.now - start_time
log.info "Total shards into kafka: #{num_shards} shards with #{num_threads} threads in #{elapsed_time} seconds. Avg time per shard: #{elapsed_time/num_shards}"
