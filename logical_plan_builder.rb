#!/usr/bin/env jruby

require 'java'
require 'rubygems'
require 'json'

jars = Dir[File.join(ENV['PIG_HOME'], 'pig*.jar')].reject{|j| j =~ /withouthadoop/}
jars.each{|j| require j}

require 'foreach_plan_builder'
require 'expressions'
require 'operators'

import 'org.codehaus.jackson.map.ObjectMapper'

import 'org.apache.pig.impl.PigContext'
import 'org.apache.pig.FuncSpec'
import 'org.apache.pig.ResourceSchema'
import 'org.apache.pig.impl.io.FileSpec'
import 'org.apache.pig.parser.LogicalPlanBuilder'
import 'org.apache.pig.parser.QueryParserUtils'
import 'org.apache.pig.newplan.logical.Util'
import 'org.apache.pig.impl.logicalLayer.schema.Schema'
import 'org.apache.pig.newplan.logical.relational.LOLoad'
import 'org.apache.pig.newplan.logical.relational.LOFilter'
import 'org.apache.pig.newplan.logical.relational.LOStore'
import 'org.apache.pig.newplan.logical.relational.LogicalPlan'
import 'org.apache.pig.newplan.logical.relational.LogicalSchema'
import 'org.apache.pig.newplan.logical.visitor.CastLineageSetter'
import 'org.apache.pig.newplan.logical.visitor.ColumnAliasConversionVisitor'
import 'org.apache.pig.newplan.logical.visitor.DuplicateForEachColumnRewriteVisitor'
import 'org.apache.pig.newplan.logical.visitor.ImplicitSplitInsertVisitor'
import 'org.apache.pig.newplan.logical.visitor.ScalarVariableValidator'
import 'org.apache.pig.newplan.logical.visitor.ScalarVisitor'
import 'org.apache.pig.newplan.logical.visitor.SchemaAliasVisitor'
import 'org.apache.pig.newplan.logical.visitor.TypeCheckingRelVisitor'
import 'org.apache.pig.newplan.logical.visitor.UnionOnSchemaSetter'
import 'org.apache.pig.newplan.logical.optimizer.SchemaResetter'
import 'org.apache.pig.newplan.logical.optimizer.AllExpressionVisitor'
import 'org.apache.pig.newplan.logical.optimizer.DanglingNestedNodeRemover'
import 'org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil'
import 'org.apache.pig.impl.plan.CompilationMessageCollector'

class LogicalPlanSerializer 

  attr_accessor :plan, :pig_context, :file_name_map,
                :load_index, :store_index, :operators

  class OperatorMap
    
    def initialize
      @data = {}
    end
        
    def get k
      @data[k]
    end
    
    def put k, v
      @last_rel = k
      @data[k] = v
    end    
  end
    
  def initialize pig_context
    @plan          = LogicalPlan.new
    @pig_context   = pig_context
    @file_name_map = {}
    @load_index    = 0
    @store_index   = 0
    @operators     = OperatorMap.new
  end

  def build_from_json json
    lpj = JSON.parse(json, {:symbolize_names => true})
    build_from_obj(lpj)
  end
  
  def build_from_obj obj

    # how to walk the structure in the right order is important
    obj[:graph].each do |op|
      case op[:operator]
      when 'load' then
        build_load_op(op)
      when 'filter' then
        build_filter_op(op[:input], op[:alias], op[:condition])
      when 'foreach' then
        build_foreach_op(op)
      when 'store' then
        build_store_op(op[:data][:input], op[:data][:filename])
      end      
    end
    compile_plan
  end

  #
  # FIXME: Needs its own class
  #
  def compile_plan

    DanglingNestedNodeRemover.new(plan).visit
    ColumnAliasConversionVisitor.new(plan).visit
    SchemaAliasVisitor.new(plan).visit
    ScalarVisitor.new(plan, pig_context, '').visit
    ImplicitSplitInsertVisitor.new(plan).visit
    DuplicateForEachColumnRewriteVisitor.new(plan).visit
    
    collector = CompilationMessageCollector.new
    TypeCheckingRelVisitor.new(plan, collector).visit

    UnionOnSchemaSetter.new(plan).visit
    CastLineageSetter.new(plan, collector).visit
    ScalarVariableValidator.new(plan).visit
  end
  
  #
  # FIXME: Create resource schema without first serializing as json.
  # This is hacky.
  # Return logical schema from json serialization via pig storage
  #
  def schema_from_json json
    # First get resource schema
    rs = (ObjectMapper.new()).readValue(json.to_java(:string), ResourceSchema.java_class);
    Util.translateSchema(Schema.getPigSchema(rs))
  end

  #
  # Get FuncSpec for class name.
  # FIXME: Cache these
  #
  def spec_for_name name
    FuncSpec.new(name)
  end
  
  #
  # Instantiate a Pig function (Class) from the class path
  #
  def func_for_name name
    PigContext.instantiateFuncFromSpec(spec_for_name(name))
  end  
  
  #
  # Get absolute path for filename from load func
  #
  def absolute_load_path func_name, filename
    spec = spec_for_name(func_name)
    func = func_for_name(func_name)
    
    file_name_key = QueryParserUtils.constructFileNameSignature(filename, spec) + "_" + load_index.to_s
    @load_index   = load_index + 1
    absolute_path = file_name_map[file_name_key]

    if !absolute_path
      absolute_path = func.relativeToAbsolutePath(filename, QueryParserUtils.getCurrentDir(pig_context))
      if absolute_path
        QueryParserUtils.setHdfsServers(absolute_path, pig_context)
      end
      file_name_map[file_name_key] = absolute_path
    end
    
    absolute_path
  end

  #
  # Get absolute path for filename from store func
  #
  def absolute_store_path func_name, input_alias, filename

    func = func_for_name(func_name)
    
    file_name_key = input_alias + store_index.to_s
    @store_index  = store_index + 1
    absolute_path = file_name_map[file_name_key]
    
    if !absolute_path
      absolute_path = func.relativeToAbsolutePath(filename, QueryParserUtils.getCurrentDir(pig_context))
      if absolute_path
        QueryParserUtils.setHdfsServers(absolute_path, pig_context)        
      end
      file_name_map[file_name_key] = absolute_path
    end
    
    absolute_path
  end

  
  #
  # Adds a load op to the current logical plan.
  # FIXME: Allow passing in optional LoadFunc class
  #
  def build_load_op op

    load     = LogicalOperator::Load.from_hash(op)
    pig_load = load.to_pig(pig_context, plan, nil) 
    
    return build_op(pig_load, op[:alias], [], nil)
  end

  def build_filter_op inputs, aliaz, graph
    
    op  = LOFilter.new(plan)
    lep = LogicalExpression::Plan.new(pig_context, op)
    lep.build(graph)    
    
    op.set_filter_plan(lep.to_pig(lep.graph))
    
    aliaz = build_op(op, aliaz, inputs, nil)
    SchemaResetter.new(op.getPlan, true).visit(op)
    return aliaz
  end

  def build_foreach_op op
    foreach     = LogicalOperator::ForEach.from_hash(op)
    pig_foreach = foreach.to_pig(pig_context, plan, nil)

    # Connect to rest of plan
    aliaz = build_op(pig_foreach, op[:alias], op[:input], nil)
    SchemaResetter.new(pig_foreach.get_plan, true).visit(pig_foreach)
    return aliaz
  end
  
  def build_store_op inputs, filename

    # Get store func
    store_func = func_for_name("PigStorage")
    
    # set udf signature (for uniqueness)
    signature = inputs.first + LogicalPlanBuilder.newOperatorKey('')
    store_func.setStoreFuncUDFContextSignature(signature)

    # Get absolute path
    absolute_path = absolute_store_path("PigStorage", inputs.first, filename)

    # Build LOStore
    file_spec = FileSpec.new(absolute_path, spec_for_name("PigStorage"))
    op        = LOStore.new(plan, file_spec, store_func, signature)
    
    return build_op(op, nil, inputs, nil)
  end
  
  def set_alias(op, aliaz)
    if (!aliaz)
      aliaz = LogicalPlanBuilder.newOperatorKey('')
    end
    op.setAlias(aliaz)
  end

  def set_partitioner(op, partitioner)
    if (partitioner)
      op.setCustomPartitioner(partitioner)
    end
  end 
        
  def build_op op, aliaz, input_aliazes, partitioner
    set_alias(op, aliaz)
    set_partitioner(op, partitioner)
    op.set_location(org.apache.pig.parser.SourceLocation.new('',0,0)) # increment a counter or some such
    
    plan.add(op)
    
    input_aliazes.each do |a|
      pred = operators.get(a)
      plan.connect(pred, op)      
    end

    @operators.put(op.get_alias, op)
    pig_context.set_last_alias(op.get_alias)
    return op.get_alias
  end
          
end
