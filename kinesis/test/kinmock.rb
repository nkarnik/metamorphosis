require "aws-sdk"

def log(msg)
  if @lf.nil?
    @lf = File.open(LOGFILE, 'a')
  end
  puts "#{Time.now}: #{msg}\n"
  @lf.write "#{Time.now}: #{msg}\n"
  @lf.flush
end

LOGFILE = 'kinesis_test.log'

if File.exists? LOGFILE
  File.delete LOGFILE
end

AWS.config(
          :access_key_id    => 'AKIAJWZ2I3PMFF5O6PFA',
          :secret_access_key => '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'
          )

kclient = AWS::Kinesis::Client.new
testream = kclient.list_streams.stream_names[0]

shardids = []

kclient.describe_stream(:stream_name => testream).stream_description.shards.each do |shard|
  log shard.shard_id
end

