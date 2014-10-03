require "twitter"
require "json"
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


client = Twitter::Streaming::Client.new do |config|
  config.consumer_key    = "737IyfUGGdLiC1cU5CFzvKOSb"
  config.consumer_secret = "KcFewn98ii6Bj3V5qkg14OSGS1IZve3Ed3C1ZiIxao9On9cCMv"
  config.access_token = "358948091-N57ziKU0NO4NYchA9KUF84YqQrfXFHDDvjP62HED"
  config.access_token_secret = "4miC0VGIOgNMFttMmTVhAnJ3kJm4z9SfUMhKybVC3W2pO"
end

tweets = []
start = Time.now

t = Thread.new do
begin
client.sample do |object|
  if object.is_a?(Twitter::Tweet) and object.lang == "en"
    puts object.retweet_count.to_s
    tweet = {:text => object.text.to_s, :favorite_count => object.favorite_count.to_i, :retweet_count => object.retweet_count.to_i, :in_response => object.in_reply_to_screen_name.to_s}

    if object.retweet_count.to_i > 0
      puts object.text
    end

    tweets << tweet.to_json

  end
end
rescue Exception => e
  puts tweets.size
  finish = Time.now
  elapsed = finish - start
  puts "elapsed: #{finish - start}"
  tps = tweets.size / elapsed
  puts "tweets per second: #{tps}"
end
end



sleep 1


kClient = AWS::Kinesis::Client.new

workers = []
begin
  log "Spinning up #{num_threads} threads"
  num_threads.times do |num|
    thread = Thread.new do
      tweet_num = 0
      loop do

        partition = "asdfdsafdsa" + rand(100).to_s
        #data = rand(1000).to_s + "qwertywewrw"
        data = tweets[tweet_num].to_s
        data.force_encoding("iso-8859-1").encode("utf-8").encode("ASCII",:invalid => :replace, :undef => :replace, :replace => '', :universal_newline => true).gsub("\n","")
        kClient.put_record(:stream_name => "TestStream", :data => data, :partition_key => partition)
        puts data, partition, tweet_num, data.class

        sleep 0.01
        tweet_num += 1

      end
    end
    workers << thread
  end
rescue Exception => te
  log "Weird error #{te.backtrace}"
end

workers.map(&:join);
