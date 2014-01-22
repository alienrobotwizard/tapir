require 'logical_expressions'

import 'org.apache.pig.newplan.logical.relational.LOForEach'
import 'org.apache.pig.newplan.logical.relational.LOGenerate'
import 'org.apache.pig.newplan.logical.relational.LOInnerLoad'
import 'org.apache.pig.newplan.logical.relational.LogicalPlan'
import 'org.apache.pig.newplan.logical.relational.LogicalSchema'
import 'org.apache.pig.newplan.logical.expression.ProjectExpression'
import 'org.apache.pig.newplan.logical.expression.LogicalExpressionPlan'

class ForeachPlanBuilder

  attr_accessor :pig_context, :current_op, :operators, :inner_plan, :foreach, :expr_plans

  # for state
  attr_accessor :in_nested_command, :nested_plan, :nested_foreach
  
  def initialize plan, pig_context
    @pig_context = pig_context
    @inner_plan  = LogicalPlan.new
    @operators   = {} # alias => Operator
    @expr_plans  = {} # alias => LogicalExpressionPlan
    @foreach     = LOForEach.new(plan)
  end
  
  def build_op op
    case op['type']
    when 'generate' then # simple
      build_generate(op)
    when 'proj' then
      operators[op['alias']] = build_proj['op']
    end
    foreach.set_inner_plan(inner_plan)
    return foreach
  end

  def build_generate op
    gen = LOGenerate.new( (in_nested_command ? nested_plan : inner_plan) )

    plans    = []
    flattens = []
    schemas  = [] # FIXME! No user schema for now?
    
    op['results'].each do |to_generate|
      leb = LogicalExpressionBuilder.new(pig_context, gen)
      leb.expression(to_generate)
      
      flattens << to_generate['flatten'] # flatten result?
      plans    << leb.plan
    end

    fe = (in_nested_command ? nested_foreach : foreach) # which foreach are we referring to?

    inner_plan = gen.get_plan
    inputs     = []
    plans.each_with_index do |plan, idx|

      # Try to set the schema; FIXME - user schema?
      # exp = plan.get_sources.first      
      # ls  = LogicalSchema.new
      # ls.add_field(exp.get_field_schema)
      # schemas[idx] = ls
      
      process_expression_plan(fe, inner_plan, plan, inputs)
    end

    gen.set_output_plans(plans)
    gen.set_flatten_flags(flattens.to_java(:boolean))
    gen.set_user_defined_schema(schemas)
    inner_plan.add(gen)

    inputs.each do |inp|
      inner_plan.connect(inp, gen)
    end    
  end

  def process_expression_plan fe, lp, lep, inputs
    lep.get_operators.each do |op|
      
      if op.is_a? ProjectExpression
        col_alias = op.get_col_alias        
        if col_alias          
          projected = op.get_projected_operator
          if projected
            idx = inputs.index(projected)
            if !idx
              idx = inputs.size
              inputs.add(projected)
            end
            op.set_input_num(idx)
            op.set_col_num(-1)            
          else
            lol = LOInnerLoad.new(lp, fe, col_alias)
            setup_innerload_and_proj(lol, op, lp, inputs)
          end         
        end        
      end
      
    end
    
  end

  def setup_innerload_and_proj inner_load, project_expr, lp, inputs
    project_expr.set_input_num(inputs.size)
    project_expr.set_col_num(-1)
    lp.add(inner_load)
    inputs << inner_load
  end
  
  def build_proj op
    leb   = LogicalExpressionBuilder.new(pig_context, current_op)
    plans = []
  end

  def build_filter
  end

  def build_sort
  end

  def build_distinct
  end

  def build_limit
  end

  def build_cross
  end

  def build_distinct
  end
  
end
