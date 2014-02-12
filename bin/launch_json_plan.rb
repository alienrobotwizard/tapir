#!/usr/bin/env jruby

require 'rubygems'
require 'json'

$: << File.dirname(__FILE__)+'/../lib'
require 'pigdsl'

PIG_HOME = ENV['PIG_HOME']
jars  = Dir[File.join(PIG_HOME, 'pig*.jar')].reject{|j| j =~ /withouthadoop/}
jars += Dir[File.join(PIG_HOME, 'build/ivy/lib/Pig/*jruby*.jar')]
jars += Dir[File.join(PIG_HOME, 'build/ivy/lib/Pig/*jython*.jar')] 
jars.each{|j| require j}

import 'java.util.Properties'
import 'org.apache.pig.impl.PigContext'

def to_properties conf  
  props = Properties.new
  conf.each do |k,v|
    props.put(k.to_s, v)
  end  
  props
end

def pig_context props
  context = PigContext.new(props)
  context.connect()
  context
end


def execution_engine pc
  engine = pc.get_execution_engine
  
  # Fooabr
  script_state = engine.instantiateScriptState
  org.apache.pig.tools.pigstats.ScriptState.start(script_state)
  
  engine
end

json = JSON.parse(File.read(File.expand_path(ARGV[0])), {:symbolize_names => true})

props  = to_properties(json[:properties])
pc     = pig_context(props)
engine = execution_engine(pc)

compiler = LogicalPlanCompiler.new(pc)
plan     = compiler.compile(json)

puts "Lanching #{plan}"
engine.launch_pig(plan, '', pc)
