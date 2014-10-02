module Metamorphosis
module Workers
module Sources
  include Logging

class SourceFromKinesis

  attr_reader :path
  def initialize(sourceconfig)
    AWS.config(
              :access_key_id    => 'AKIAJWZ2I3PMFF5O6PFA',
              :secret_access_key => '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'
              )

    info "Creating kinesis source"
 
    @_kinesis = AWS::Kinesis::Client.new
    @stream_name = sourceconfig["stream"].to_s
    @offset = sourceconfig["offset"].to_s
    @shard = sourceconfig["shard"].to_s

    info "Getting shard iterator for stream #{@stream_name}, shard #{@shard} with offset #{@offset}"

    @fetch_size = 20

    if @offset == "0"
      @shard_iter = @_kinesis.get_shard_iterator(:stream_name => @stream_name, :shard_id => @shard, :shard_iterator_type => "LATEST")
    else
      @shard_iter = @_kinesis.get_shard_iterator(:stream_name => @stream_name, :shard_id => @shard, :shard_iterator_type => "AFTER_SEQUENCE_NUMBER", :starting_sequence_number => @offset)
    end

    @local_file_path = "/tmp/" + @stream_name + @shard + ".gz"
    info "Writing to path: #{@local_file_path}"

    # NEED to sleep, otherwise we won't get any messages
    sleep 3
  end


  def local_file_path
    @local_file_path
  end

  def get_data
    info "getting data"
    @_sqnum = @offset
    begin
      info "opening path: " + @local_file_path
      File.open(@local_file_path, 'wb') do |file|
        results = @_kinesis.get_records(:shard_iterator => @shard_iter.shard_iterator, :limit => @fetch_size)
        info "Retreived #{results.records.length} records for #{@shard} shard" 
        gz = Zlib::GzipWriter.new(file)
        results.records.each do |record|
          gz.write record.data.to_s
          gz.write "\n"
          info "data written"
          @_sqnum = record.sequence_number
        end
        gz.close
        info "Retreived #{results.records.length} records for #{@shard} shard"
      end
    rescue Exception => e
      error "Shard error: #{e.message} for path #{@shard}"
      # TODO Retry
    end
    

  end

  def push_next(topic, type, work_q, q_offset)
    
    config = {:stream => @stream_name, :shard => @shard, :offset => @_sqnum}
    source = {:type => type, :config => config}
    source_topic = {:source => source, :topic => topic}.to_json
    message = JSON.parse(source_topic)
    message_and_offset = {:message => message, :offset => q_offset}
    work_q.push(message_and_offset)

  end
end
end
end
end
