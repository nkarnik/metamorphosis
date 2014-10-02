require "aws-sdk"
require "poseidon"
require "json"
require 'logger'

class SourceFromS3
  

  def initialize(message)
    @log = Logger.new('| tee schloss.log', 10, 1024000)
    @log.datetime_format = '%Y-%m-%d %H:%M:%S'

    AWS.config(
          :access_key_id    => 'AKIAJWZ2I3PMFF5O6PFA',
          :secret_access_key => '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'
          )

    @_s3 = AWS::S3.new

    @local_manifest = nil

    @bucket_name = message["source"]["config"]["bucket"]
    @manifest_path = message["source"]["config"]["manifest"]
    @topic_to_write = message["topic"]
    @sourcetype = message["source"]["type"]
    @bucket = @_s3.buckets[@bucket_name]
    @log.info "Downloading Manifest from #{@manifest_path} for topic: #{@topic_to_write} in bucket: #{@bucket_name}"
  end      


  def write_to_manifest(manifest)
    @local_manifest = manifest

    File.open(@local_manifest, 'wb') do |file|
      @log.info "Opened file: #{file}"
      begin
        @bucket.objects[@manifest_path].read do |chunk|
          file.write(chunk)
        end
      rescue 
        @log.error "s3 error for path: #{f}"
      end
    end
  end

  def parse(line)
    config = {:bucket => @bucket_name, :shard => line.chomp}
    source = {:type => @sourcetype, :config => config}
    return source
  end

end
