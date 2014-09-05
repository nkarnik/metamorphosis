require 'poseidon'

producer = Poseidon::Producer.new(["10.167.177.28:9092", "10.184.165.95:9092" ], "st1", :type => :sync)
s3file = File.open("/mnt/data/part_0001")

s3file.each do |line|
  producer.send_messages([Poseidon::MessageToSend.new("nktest", line)])
  puts line
end
