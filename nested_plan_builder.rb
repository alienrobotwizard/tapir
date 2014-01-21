require 'logical_expressions'

import 'org.apache.pig.newplan.logical.LogicalPlan'
import 'org.apache.pig.newplan.logical.expression.LogicalExpressionPlan'

class NestedPlanBuilder

  attr_accessor :pig_context, :current_op, :operators, :inner_plan, :expr_plans
  
  def initialize pig_context, op
    @pig_context = pig_context
    @current_op  = op
    @inner_plan  = LogicalPlan.new
    @operators   = {} # alias => Operator
    @expr_plans  = {} # alias => LogicalExpressionPlan
  end
  
  def build_op op
    case op['type']
    when 'proj' then
      operators[op['alias']] = build_proj['op']
    end
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
