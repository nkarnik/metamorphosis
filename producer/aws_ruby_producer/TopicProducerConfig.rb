#Class that houses configuration information for building producers for each thread
class TopicProducerConfig
  attr_reader :leaders_per_partition, :partitions_on_localhost, :producer_hash
  def initialize(broker_pool, topic)
    @log = Logger.new('| tee shard_producer.log', 10, 1024000)
    @log.datetime_format = '%Y-%m-%d %H:%M:%S'

    cluster_metadata = Poseidon::ClusterMetadata.new
    cluster_metadata.update(broker_pool.fetch_metadata([topic]))
    metadata = cluster_metadata.topic_metadata[topic]
    num_partitions = metadata.partition_count

    @leaders_per_partition = []
    metadata.partition_count.times do |part_num|
      h = cluster_metadata.lead_broker_for_partition(topic, part_num)
      @leaders_per_partition << "#{h.host}:#{h.port}"
    end

    @log.info "Leaders per partition: #{leaders_per_partition}"
    partitions_per_leader = {}
    @leaders_per_partition.each_with_index do |ip, index|
      if partitions_per_leader[ip].nil?
        partitions_per_leader[ip] = []
      end
      partitions_per_leader[ip] << index
    end
    
    this_host = `hostname`.chomp() + ".ec2.internal"
    
    @partitions_on_localhost = partitions_per_leader[this_host] || []
    
    if(@partitions_on_localhost.size == 0)
      @log.info "Partitions on localhost is null because no partitions found on #{this_host}. Using sampling instead"
      @partitions_on_localhost  = partitions_per_leader[partitions_per_leader.keys.sample]
    end
    @log.info "Partitions on this host: #{@partitions_on_localhost}" 

    @producer_hash = Hash.new {|hash, partition| hash[partition] = get_producer_for_partition(partition)}
  end

  def get_producer_for_partition(partition_num)
    single_partitioner = Proc.new { |key, partition_count| partition_num  } # Will always right to a single partition
    producer_fqdn = @leaders_per_partition[partition_num]
    if(producer_fqdn.include? "localhost")
      producer_connection_string = producer_fqdn
    else
      producer_connection_string = producer_fqdn.split(".").first.gsub("ip-", "").gsub("-",".") + ":9092"
    end
    @log.info "producer_connection_string for getProducerForPartition: #{producer_connection_string}"

    return Poseidon::Producer.new([producer_connection_string],
                                  "producer_#{partition_num}",
                                  :type => :sync,
                                  :partitioner => single_partitioner,
                                  # :snappy Unimplemented in Poseidon. Should be easy to add into
                                  # https://github.com/bpot/poseidon/blob/master/lib/poseidon/compression/snappy_codec.rb
                                  :compression_codec => :gzip)
  end
end
