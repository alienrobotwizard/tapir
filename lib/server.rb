#!/usr/bin/env jruby

require 'sinatra'
require 'pigdsl'

jars = Dir[File.join(ENV['PIG_HOME'], 'pig*.jar')].reject{|j| j =~ /withouthadoop/}     
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

post '/plan' do
  request.body.rewind
  plan = JSON.parse(request.body.read, {:symbolize_names => true})

  props = to_properties(plan[:properties])
  pc    = pig_context(props)
  
  compiler = LogicalPlanCompiler.new(pc)
  compiled = compiler.compile(plan)

  puts "Launching #{compiled}"
  
  engine = execution_engine(pc)

  engine.launch_pig(compiled, '', pc)
  "success"
end
