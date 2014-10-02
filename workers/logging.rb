require 'logger'

module Metamorphosis
module Workers
module Logging
  @@logger = Logger.new('| tee ../logs/workers.log')
  @@logger.formatter = proc do |severity, datetime, progname, msg|
    "#{severity.ljust(6)}[#{datetime.strftime('%Y-%m-%d %H:%M:%S')}]: #{msg}\n"
  end

  @@logger.level = Logger::DEBUG
  def log
    return @@logger
  end

  def info(msg)
    @@logger.info("[#{self.class.to_s.split("::").last}]\t#{msg}")
  end

  def error(msg)
    @@logger.error("[#{self.class.to_s.split("::").last}]\t#{msg}")
  end

  def debug(msg)
    @@logger.debug("[#{self.class.to_s.split("::").last}]\t#{msg}")
  end
end
end 
end
