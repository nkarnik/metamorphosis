require "thread"

#class that houses hash of queues
class SourceQueue
 
  attr_reader :queues, :num
  def initialize
    @queues = {}
    @num = 1
  end

  #pushes to existing queue dedicated to topic, otherwise creates and pushes to it
  def push(message)
    found = false
    topic = message["topic"]
    queues.each do |key, queue|
      if key == topic
        found = true
        queue << message
      end
    end

    newQueue = Queue.new
    newQueue << message
    queues[topic] = newQueue
  end

  #pops off in round robin fashion ... this can be made more robust later
  def pop(non_block=false)
    if @queues.length == 0
      #can't pop from no queues
      break
    end

    currentQueue = @queues.keys[@num % @queues.length ]
    @num += 1
    return @queues[currentQueue].pop(true)
  end

end
