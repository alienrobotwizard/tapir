#!/usr/bin/env jruby

require 'java'

PIG_JAR = File.join(ENV['PIG_HOME'], 'pig.jar')

require PIG_JAR

import 'java.util.Properties'
import 'java.io.FileReader'
import 'java.io.BufferedReader'

import 'org.apache.pig.ExecType'
import 'org.apache.pig.PigServer'
import 'org.apache.pig.impl.PigContext'
import 'org.apache.pig.tools.grunt.GruntParser'

class LogicalPlanServer < PigServer

  #
  # Kind of a hack. Bypasses the regular PigServer
  # behavior in the same way that ToolsPigServer for
  # penny does. Probably isn't great going forward
  # but works for now
  #
  def get_logical_plan script
    parser = GruntParser.new(BufferedReader.new(FileReader.new(script)))
    parser.set_interactive(false)
    parser.set_params(self)
    set_batch_on
    parser.parse_only
    currentDAG.get_logical_plan
  end
  
end

class PigParser

  attr_accessor :pig_server, :pig_context

  #
  # Update this in the future to actually do parameter substitution
  #
  def initialize
    @pig_context = get_pig_context
    @pig_server  = get_pig_server(pig_context)
  end

  def get_pig_context
    PigContext.new(ExecType::LOCAL, Properties.new)
  end

  #
  # Returns hacked PigServer subclass that'll divulge
  # the logical plan
  #
  def get_pig_server context
    LogicalPlanServer.new(context)
  end
  
  def get_logical_plan script
    pig_server.get_logical_plan(script)
  end
  
end

parser = PigParser.new
puts parser.get_logical_plan(ARGV[0])
