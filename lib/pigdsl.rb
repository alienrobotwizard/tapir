require 'java'
require 'json'

#
# Ensure pig jar is in the classpath
#
jars = Dir[File.join(ENV['PIG_HOME'], 'pig*.jar')].reject{|j| j =~ /withouthadoop/}
jars.each{|j| require j}

require 'expressions'
require 'operators'
require 'compiler'
