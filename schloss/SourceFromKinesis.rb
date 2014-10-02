require "aws-sdk"
require "poseidon"
require "json"
require 'logger'
class SourceFromKinesis
  

  def initialize(message, credentials="TODO")
    @log = Logger.new('| tee schloss.log', 10, 1024000)
    @log.datetime_format = '%Y-%m-%d %H:%M:%S'

    AWS.config(
          :access_key_id    => 'AKIAJWZ2I3PMFF5O6PFA',
          :secret_access_key => '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'
          )

    @_kinesis = AWS::Kinesis::Client.new
    @local_manifest = nil
    @shardIDs = []

    @stream_name = message["source"]["config"]["stream"]

    @topic_to_write = message["topic"]
    @sourcetype = message["source"]["type"]

    @log.info "Downloading Kinesis Manifest from #{@stream_name} for topic: #{@topic_to_write} in bucket: #{@bucket_name}"
  end      

  def write_to_manifest(manifest)
    @local_manifest = manifest
    
    @_kinesis.describe_stream(:stream_name => @stream_name).stream_description.shards.each do |shard|
      @log.info shard.shard_id
      @shardIDs << shard.shard_id
    end

    File.open(@local_manifest, 'wb') do |file|
      @log.info "Opened file: #{file}"
      @shardIDs.each do |shard|
        begin
          file.write(shard)
          file.write("\n")
        rescue
          @log.info "kinesis error for path: #{file}"
        end
      end
    end
  end

  def parse(line)
    config = {:stream => @stream_name, :shard => line.chomp}
    source = {:type => @sourcetype, :config => config, :offset => 0}
    return source
  end

end
