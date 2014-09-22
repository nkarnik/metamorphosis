require "aws"

AWS.config(
          :access_key_id    => 'AKIAJWZ2I3PMFF5O6PFA',
          :secret_access_key => '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'
          )

s3 = AWS::S3.new

`rm -r /tmp/kfk2`
`mkdir /tmp/kfk2`

bucket = s3.buckets["fatty.zillabyte.com"]
tree = bucket.as_tree(:prefix => 'data/homepages/2014/0822')
directories = tree.children.select(&:branch?).collect(&:prefix)


files = bucket.as_tree(:prefix => directories.first).children.select(&:leaf?).collect(&:key)
count = 0

a = Time.now
puts a

files.each do |f|
  puts f
  path = "/tmp/kfk2/" + f.split("/").last(2).join("_")
  File.open(path, 'wb') do |file|
    bucket.objects[f].read do |chunk|
      file.write(chunk)
    end
  end
  count += 1
  puts count
  puts Time.now
end

b = Time.now
puts b - a

puts `ls /tmp/kfk2`
