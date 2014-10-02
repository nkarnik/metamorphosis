require "thread"
require_relative "../logging.rb"
module Metamorphosis
module Workers
module Sources
  include Logging

#class that houses hash of queues
class RoundRobinByTopicMessageQueue
 
  attr_reader :queues, :num, :size
  def initialize
    @queues = {}
    @num = 1
    @size = 0
  end

  def increment_size(n)
    @size += n
  end

  #pushes to existing queue dedicated to topic, otherwise creates and pushes to it
  def push(message)
    found = false
    topic = message[:message]["topic"]
    queues.each do |key, queue|
      if key == topic
        found = true
        queue << message
        @size += 1
      end
    end

    if found == false
      newQueue = Queue.new
      newQueue << message
      queues[topic] = newQueue
      @size += 1
    end
  end

  #pops off in round robin fashion ... this can be made more robust later
  def pop(non_block=false)
    if @queues.length <= 0
      #can't pop from no queues
      return
    end

    currentQueue = @queues.keys[@num % @queues.length ]
    @num += 1
    @size -= 1
    return @queues[currentQueue].pop(true)
  end

  def details
  
    currentQueue = @queues.keys[@num % @queues.length ]
    return @queues[currentQueue].size() + 1

  end

end
end
end
end
