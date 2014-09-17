#Class that houses configuration information for building producers for each thread
class TopicProducerConfig
  attr_reader :leaders_per_partition, :partitions_on_localhost, :producer_hash
  def initialize(broker_pool, topic)
    cluster_metadata = Poseidon::ClusterMetadata.new
    cluster_metadata.update(broker_pool.fetch_metadata([topic]))
    metadata = cluster_metadata.topic_metadata[topic]
    num_partitions = metadata.partition_count

    @leaders_per_partition = []
    metadata.partition_count.times do |part_num|
      @leaders_per_partition << cluster_metadata.lead_broker_for_partition(topic, part_num).host
    end

    # log "Leaders per partition: #{leaders_per_partition}"
    partitions_per_leader = {}
    @leaders_per_partition.each_with_index do |ip, index|
      if partitions_per_leader[ip].nil?
        partitions_per_leader[ip] = []
      end
      partitions_per_leader[ip] << index
    end
    
    this_host = `hostname`.chomp() + ".ec2.internal"
    @partitions_on_localhost = partitions_per_leader[this_host] || []
    #log "Partitions on this host: #{@partitions_on_localhost.size}" 

    @producer_hash = Hash.new {|hash, partition| hash[partition] = get_producer_for_partition(partition)}
  end

  def get_producer_for_partition(partition_num)
    single_partitioner = Proc.new { |key, partition_count| partition_num  } # Will always right to a single partition
    producer_fqdn = @leaders_per_partition[partition_num]
    return Poseidon::Producer.new([producer_fqdn.split(".").first.gsub("ip-", "").gsub("-",".") + ":9092"],
                                  "producer_#{partition_num}",
                                  :type => :sync,
                                  :partitioner => single_partitioner,
                                  # :snappy Unimplemented in Poseidon. Should be easy to add into
                                  # https://github.com/bpot/poseidon/blob/master/lib/poseidon/compression/snappy_codec.rb
                                  :compression_codec => :gzip)
  end
end
