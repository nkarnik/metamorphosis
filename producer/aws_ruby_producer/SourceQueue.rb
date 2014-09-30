require "thread"

#class that houses hash of queues
class SourceQueue
 
  attr_reader :queues, :num, :size
  def initialize
    @queues = {}
    @num = 1
    @size = 0
  end

  #pushes to existing queue dedicated to topic, otherwise creates and pushes to it
  def push(message)
    found = false
    puts "message is #{message}"
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
    end
  end

  #pops off in round robin fashion ... this can be made more robust later
  def pop(non_block=false)
    if @queues.length == 0
      #can't pop from no queues
      return
    end

    currentQueue = @queues.keys[@num % @queues.length ]
    @num += 1
    @size -= 1
    return @queues[currentQueue].pop(true)
  end

  def info
  
    currentQueue = @queues.keys[@num % @queues.length ]
    @num += 1
    return @queues[currentQueue].size()

  end

end
