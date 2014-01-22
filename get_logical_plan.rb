#!/usr/bin/env jruby

require 'java'

PIG_JAR = Dir[File.join(ENV['PIG_HOME'], 'pig*.jar')].reject{|j| j =~ /withouthadoop/}.first

require PIG_JAR

import 'java.util.Properties'
import 'java.io.FileReader'
import 'java.io.BufferedReader'

import 'org.apache.pig.ExecType'
import 'org.apache.pig.PigServer'
import 'org.apache.pig.impl.PigContext'
import 'org.apache.pig.tools.grunt.GruntParser'

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
import 'org.apache.pig.impl.plan.CompilationMessageCollector'

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

  def compile plan
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
  
  def get_logical_plan script
    plan = pig_server.get_logical_plan(script)
    compile(plan)
    plan
  end
  
end

parser = PigParser.new
puts parser.get_logical_plan(ARGV[0])
