#!/usr/bin/env jruby

require 'sinatra'
require 'pigdsl'

PIG_HOME = ENV['PIG_HOME']
jars  = Dir[File.join(PIG_HOME, 'pig*.jar')].reject{|j| j =~ /withouthadoop/}
jars += Dir[File.join(PIG_HOME, 'build/ivy/lib/Pig/*jruby*.jar')]
jars += Dir[File.join(PIG_HOME, 'build/ivy/lib/Pig/*jython*.jar')] 
jars.each{|j| require j}

import 'java.util.Properties'
import 'org.apache.pig.PigServer'
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

def pig_server pc
  PigServer.new(pc, false)
end

def execution_engine pc
  engine = pc.get_execution_engine
  
  # Fooabr
  script_state = engine.instantiateScriptState
  org.apache.pig.tools.pigstats.ScriptState.start(script_state)
  
  engine
end

#
# ==== These should go somewhere else ====
#

# Register jars or scripting udfs
def process_registers pig_server, registers=[]
  registers.each do |register|
    resource = register[:resource]
    if register.has_key? :using
      using     = register[:using]
      namespace = register[:namespace]
      pig_server.register_code(resource, using, namespace)
    elsif resource.end_with? '.jar'
      pig_server.register_jar(resource)
    else
      raise "Can't process register with resource #{resource}"
    end    
  end  
end

def process_defines defines
end




post '/plan' do
  request.body.rewind
  plan = JSON.parse(request.body.read, {:symbolize_names => true})

  props = to_properties(plan[:properties])
  pc    = pig_context(props)
  ps    = pig_server(pc)

  process_registers(ps, plan[:registers])
  
  if plan[:graph]
    compiler = LogicalPlanCompiler.new(pc)
    compiled = compiler.compile(plan)

    puts "Launching #{compiled}"
  
    engine = execution_engine(pc)

    engine.launch_pig(compiled, '', pc)
  end
  "success"
end
