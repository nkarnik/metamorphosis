require "aws-sdk"
require "thread"
require 'optparse'

$options = {}

opt_parser = OptionParser.new do |opt|
  opt.on("--threads THREADS", Integer, "Required: number of threads to spin up, default 1") do |threads|
    $options[:threads] = threads
  end
end

opt_parser.parse!

num_threads = $options[:threads].to_i || "1"

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

workers = []
begin
  log "Spinning up #{num_threads} threads"
  num_threads.times do |num|
    thread = Thread.new do
      loop do

        partition = "asdfdsafdsa" + rand(50).to_s
        data = rand(1000).to_s + "qwertywewrw"
        kClient.put_record(:stream_name => "TestStream", :data => data, :partition_key => partition)
        puts data, partition

        sleep 0.01

      end
    end
    workers << thread
  end
rescue Exception => te
  log "Weird error #{te.backtrace}"
end

workers.map(&:join);
