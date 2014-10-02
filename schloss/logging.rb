require 'logger'

module Metamorphosis
module Schloss
module Logging
  @@logger = Logger.new('| tee schloss.log')
  @@logger.formatter = proc do |severity, datetime, progname, msg|
    "#{severity} [#{datetime.strftime('%Y-%m-%d %H:%M:%S')}]: #{msg}\n"
  end
  
  @@logger.info "Starting schloss"

  def log
    return @@logger
  end

  def info(msg)
    @@logger.info("[#{self.class.to_s.split("::").last}]\t#{msg}")
  end

  def error(msg)
    @@logger.error("[#{self.class.to_s.split("::").last}]\t#{msg}")
  end
end
end 
end
