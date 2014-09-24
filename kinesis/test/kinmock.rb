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
idhash = {}

kclient.describe_stream(:stream_name => testream).stream_description.shards.each do |shard|
  log shard.shard_id
  shardids << shard.shard_id
end

#get old messages and shard iterator for new messages
oldsharditers = []
newsharditers = []

shardids.each do |id|
  obj = {}
  obj[:si] =  kclient.get_shard_iterator(:stream_name => testream, :shard_id => id, :shard_iterator_type => "LATEST")
  obj[:sqn] = 0
  obj[:sid] = id
  newsharditers << obj
  oldsharditers <<  kclient.get_shard_iterator(:stream_name => testream, :shard_id => id, :shard_iterator_type => "TRIM_HORIZON")
end

sqnum = 0

sharditers = []
br = false
loop do

  if br == true
    log "breaking"
    break
  end

  newsharditers.each do |si|
    records = kclient.get_records(:shard_iterator => si[:si].shard_iterator)
    records.records.each do |record|
      log "#{record.sequence_number} #{record.data}"
      sqnum = record.sequence_number
      si[:sqn] = sqnum.to_s
       
      br = true
      log si[:sqn]
      sharditers << si
    end
    si = records.next_shard_iterator

  end

  oldsharditers.each do |si|
    records = kclient.get_records(:shard_iterator => si.shard_iterator).records
    records.each do |record|
      log "#{si} #{record.data}"
    end
  end
  
end


#loop do
#  sharditers.each do |si|
#    log si[:sqn]
#    log si[:sid].class
#    sharditer = kclient.get_shard_iterator(:stream_name => "TestStream", :shard_id => si[:sid], :shard_iterator_type => "LATEST") 
#    records = kclient.get_records(:shard_iterator => sharditer.shard_iterator)
#    log records.records.length
#    records.records.each do |record|
#      log "#{record.sequence_number} #{record.data}"
#      sqnum = record.sequence_number
#      si[:sqn] = sqnum.to_s
#      #br = true
#      log "#{si[:sqn]} #{si[:sid]}"
#    end
#    si = records.next_shard_iterator
#    log "tried records"
#  end
#end


loop do
  sharditers.each do |si|
    log si[:sqn]
    log si[:sid].class
    sharditer = kclient.get_shard_iterator(:stream_name => "TestStream", :shard_id => si[:sid], :shard_iterator_type => "AFTER_SEQUENCE_NUMBER", :starting_sequence_number => si[:sqn]) 
    records = kclient.get_records(:shard_iterator => sharditer.shard_iterator)
    log records.records.length
    records.records.each do |record|
      log "#{record.sequence_number} #{record.data}"
      sqnum = record.sequence_number
      si[:sqn] = sqnum.to_s
      br = true
      log "#{si[:sqn]} #{si[:sid]}"
    end
    si = records.next_shard_iterator
    log "tried records"
  end
end
