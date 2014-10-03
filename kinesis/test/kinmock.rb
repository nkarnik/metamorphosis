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
testream = kClient.list_streams.stream_names[1]

#Get all shardids from stream description
shardids = []

kClient.describe_stream(:stream_name => testream).stream_description.shards.each do |shard|
  log shard.shard_id
  shardids << shard.shard_id
end

#get shard iterator for new messages
newshardIterators = []
shardids.each do |id|
  obj = {}
  obj[:si] =  kClient.get_shard_iterator(:stream_name => testream, :shard_id => id, :shard_iterator_type => "LATEST")
  obj[:sqn] = 0
  obj[:sId] = id
  newshardIterators << obj
end

sqnum = 0

shardIterators = []

shardIterators.each do |si|
  log "Shard Iterator for #{si[:sId]}"
end

newshardIterators.each do |nsi|
  found = false
  log "This many shardIterators #{shardIterators.length}"
  shardIterators.each do |si|
    log nsi[:sId]
    log si[:sId]
    if si[:sId] == nsi[:sId]
      found = true
      next
    end
  end

  if found == true
    log "found for #{nsi[:sId]}"
    next
  else
    shardIterators << nsi
  end
end


workers = []
begin
  log shardIterators.length.to_s
  shardIterators.length.times  do |num|
    thread = Thread.new do 
      loop do
        begin
          si = shardIterators[num]
          log "Got Shard Iterator for: #{si[:sId]} starting from sequence number #{si[:sqn]}"
   
          # If that particular shard hasn't had any records received yet, then start from "LATEST" otherwise
          # start from that shard's last sequence number
          if si[:sqn] == 0        
            sharditer =  kClient.get_shard_iterator(:stream_name => testream, :shard_id => si[:sId], :shard_iterator_type => "LATEST")
            log "Created Shard Iterator for shard id: #{si[:sId]} with stream #{testream}"
            sleep 10
          else
#            sharditer = kClient.get_shard_iterator(:stream_name => testream, :shard_id => si[:sId], :shard_iterator_type => "AFTER_SEQUENCE_NUMBER", :starting_sequence_number => si[:sqn])
            sharditer =  kClient.get_shard_iterator(:stream_name => testream, :shard_id => si[:sId], :shard_iterator_type => "LATEST")
            sleep 10
          end
 
          records = kClient.get_records(:shard_iterator => sharditer.shard_iterator)
          log "Retreived #{records.records.length} records for #{si[:sId]} shard"
          sleep 5
     
          records.records.each do |record|
            log "Using a shard iterator for shard #{si[:sId]} \nwith sequence number #{record.sequence_number}\nreceived message #{record.data} data"
            sqnum = record.sequence_number
            si[:sqn] = sqnum.to_s 
           
          end
      
          #log "tried records"
        rescue
          # Just for debugging
          sleep 3
          log "symbol error"
        end
      end
    end
    workers << thread
  end
rescue
  log "weird error"
end

workers.map(&:join);
