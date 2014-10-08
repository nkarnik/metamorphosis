module Metamorphosis
module Workers
module Sources
  include Logging

class SourceFromS3
  attr_reader :path
  def initialize(sourceconfig)
    AWS.config(
              :access_key_id    => 'AKIAJWZ2I3PMFF5O6PFA',
              :secret_access_key => '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'
              )

    @_s3 = AWS::S3.new
    @_bucket = @_s3.buckets[sourceconfig["bucket"]]

    @s3_path = sourceconfig["shard"].chomp.gsub("s3://#{sourceconfig["bucket"]}/", "")
    per_shard_time = Time.now
    @local_file_path = "/tmp/" + @s3_path.split("/").last(2).join("_")
    info "Preparing shard #{@s3_path}"
    debug @local_file_path
  end

  def local_file_path
    @local_file_path
  end

  def get_data
    begin
      info "Downloading file #{@local_file_path} ... "
      File.open(@local_file_path, 'wb') do |file|
        @_bucket.objects[@s3_path].read do |chunk|
          begin 
            file.write(chunk)
          rescue
            info "s3 error for path: #{f}"
          end
        end
      end
    rescue Exception => e
      error "Shard error: #{e.message} for path #{f}"
      # TODO Retry
    end
  end

  def push_next(topic, type, work_q, q_offset)
    return
  end

end
end
end
end
