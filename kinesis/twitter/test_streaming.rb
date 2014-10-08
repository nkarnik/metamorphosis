require "twitter"
require "json"
require "thread"


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
    puts object.text
    tweets << object.text

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

t.join
