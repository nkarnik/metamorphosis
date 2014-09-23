require "poseidon"

## Get Cluster metadata for topic

def get_leaders_for_partitions(topic, seed_brokers)
  broker_pool = Poseidon::BrokerPool.new("fetch_metadata_client", seed_brokers)
  cluster_metadata = Poseidon::ClusterMetadata.new
  cluster_metadata.update(broker_pool.fetch_metadata([topic]))
  metadata = cluster_metadata.topic_metadata[topic]
  # num_partitions = metadata.partition_count
  leaders_per_partition = []
  metadata.partition_count.times do |part_num|
    l = cluster_metadata.lead_broker_for_partition(topic, part_num)
    leaders_per_partition << "#{l.host}:#{l.port}"
  end
  return leaders_per_partition
end

def find_broker(env, attrib = 'fqdn')
  results = `knife search node "role:kafka_broker AND chef_environment:#{env}" -F json -a fqdn  -c ~/zb1/infrastructure/chef/.chef/knife.rb`
  return JSON.parse(results)["rows"].map{ |a| a.map{|k,v| v["fqdn"]}  }.map{|v| "#{v[0]}:9092"}
end

