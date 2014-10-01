require "aws-sdk"
require "poseidon"
require "json"

class SourceFromS3
  

  def initialize(message, logfile)

    AWS.config(
          :access_key_id    => 'AKIAJWZ2I3PMFF5O6PFA',
          :secret_access_key => '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'
          )

    @_s3 = AWS::S3.new

    @local_manifest = nil
    @logfile = logfile

    @bucket_name = message["source"]["config"]["bucket"]
    @manifest_path = message["source"]["config"]["manifest"]
    @topic_to_write = message["topic"]
    @sourcetype = message["source"]["type"]
    @bucket = @_s3.buckets[@bucket_name]
    log "Downloading Manifest from #{@manifest_path} for topic: #{@topic_to_write} in bucket: #{@bucket_name}"
  end      

  def log(msg)
    if @lf.nil?
      @lf = File.open(@logfile, 'a')
    end
    puts "#{Time.now}: #{msg}\n"
    @lf.write "#{Time.now}: #{msg}\n"
    @lf.flush
  end

  def write_to_manifest(manifest)
    @local_manifest = manifest

    File.open(@local_manifest, 'wb') do |file|
      log "Opened file: #{file}"
      @bucket.objects[@manifest_path].read do |chunk|
        begin
          file.write(chunk)
        rescue
          log "s3 error for path: #{f}"
        end
      end
    end
  end

  def parse(line)
    config = {:bucket => @bucket_name, :shard => line.chomp}
    source = {:type => @sourcetype, :config => config}
    return source
  end

end
