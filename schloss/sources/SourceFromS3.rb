require "aws-sdk"
require "poseidon"
require "json"
require 'logger'

require_relative "../logging.rb"
module Metamorphosis
module Schloss
  include Logging

class SourceFromS3

  def initialize(message)


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
    info "Downloading Manifest from #{@manifest_path} for topic: #{@topic_to_write} in bucket: #{@bucket_name}"
  end      


  def write_to_manifest(manifest)
    @local_manifest = manifest
    info "Writing manifest: #{manifest}"
    File.open(@local_manifest, 'wb') do |file|
      begin
        @bucket.objects[@manifest_path].read do |chunk|
          file.write(chunk)
        end
      rescue => e
        error "s3 error for path: #{f}"
        error e.message
        error e.backtrace
      end
    end
  end

  def parse(line)
    config = {:bucket => @bucket_name, :shard => line.chomp}
    source = {:type => @sourcetype, :config => config}
    return source
  end

end
end
end
