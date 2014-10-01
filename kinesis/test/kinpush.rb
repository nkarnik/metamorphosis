require "aws-sdk"
require "thread"

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

kClient = AWS::Kinesis::Client.new

loop do

  partition = "asdfdsafdsa" + rand(50).to_s
  data = rand(1000).to_s + "qwertywewrw"
  kClient.put_record(:stream_name => "TestStream", :data => data, :partition_key => partition)
  puts data, partition

  sleep 1 

end


