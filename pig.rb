#!/usr/bin/env jruby

require 'sinatra'
require 'logical_plan_builder'

jars = Dir[File.join(ENV['PIG_HOME'], 'pig*.jar')].reject{|j| j =~ /withouthadoop/}     
jars.each{|j| require j}

import 'java.util.Properties'
import 'org.apache.pig.impl.PigContext'

def to_properties conf
  props = Properties.new
  props.put_all(conf)
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
  plan = JSON.parse(request.body.read)

  props = to_properties(plan['properties'])
  pc    = pig_context(props)
  
  builder = LogicalPlanSerializer.new(pc)
  builder.build_from_obj(plan)

  engine = execution_engine(pc)

  engine.launch_pig(builder.plan, '', pc)
  "success"
end
