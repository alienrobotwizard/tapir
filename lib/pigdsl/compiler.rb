import 'org.apache.pig.impl.PigContext'
import 'org.apache.pig.impl.plan.CompilationMessageCollector'
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

class LogicalPlanCompiler

  attr_accessor :pig_context
  
  def initialize pig_context
    @pig_context = pig_context
  end

  def compile_from_json json
    lpj = JSON.parse(json, {:symbolize_names => true})
    compile(lpj)
  end
  
  def compile obj
    lp = LogicalOperator::Plan.new(pig_context)
    lp.build(obj)
    
    plan = lp.to_pig
    optimize(plan)
    plan
  end

  def optimize plan
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
end
