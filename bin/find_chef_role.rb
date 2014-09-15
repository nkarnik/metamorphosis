#/usr/bin/env ruby
require "json"

e = ARGV.first.to_s
puts ARGV[2]

def find_broker(env, attrib = 'fqdn')
  results = `knife search node "role:kafka_broker AND chef_environment:#{env}" -F json -a fqdn  -c ~/zb1/infrastructure/chef/.chef/knife.rb`
  hash = JSON.parse(results)
  hash["rows"].map do |rec|
    rec.values.first[attrib]
  end
  puts hash
  return hash
end

a = find_broker(e)
puts a.class
puts a
puts a["rows"]

ips = []

a["rows"]. each do |row|
  row.each do |k, v|
    ips << v["fqdn"]
  end
end

puts ips.class
puts ips

fqdns = []
ips.each do |ip|
  s = ip.split(".")
  t = s[0].split("-")
  fqdn = t[1, t.length].join(".") + ":9092"
  puts fqdn
  fqdns << fqdn
end

puts fqdns