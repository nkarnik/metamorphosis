require 'poseidon'

producer = Poseidon::Producer.new(["10.167.177.28:9092", "10.184.165.95:9092" ], "st1", :type => :sync)

files = Dir["/mnt/data/*"]
count = 0

files.each do |file|

  s3file = File.open(file)

  s3file.each do |line|
    producer.send_messages([Poseidon::MessageToSend.new("nktest", line)])
    #puts line
    count += 1
  end
  puts count
end
