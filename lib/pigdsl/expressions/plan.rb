import 'org.apache.pig.newplan.logical.expression.LogicalExpressionPlan'

module LogicalExpression
  class Plan
    attr_accessor :graph # Points to the root LogicalExpression
    
    attr_accessor :current_plan # Pig representation; LogicalExpressionPlan
    attr_accessor :pig_context, :current_op
    
    def initialize pig_context, current_op
      @current_plan = LogicalExpressionPlan.new
      @pig_context  = pig_context
      @current_op   = current_op
    end
    
    def build hsh
      @graph = EXPRESSIONS[hsh[:type]].from_hash(hsh)
    end
    
    def to_hash
      {
        :graph => graph.to_hash
      }
    end

    def to_json
      to_hash.to_json
    end

    #
    # An expression that has already been 'from-hashed'
    # can still be attached to this plan
    #
    def to_pig expression, in_foreach_plan = false, nest_context = {}
      expression.in_foreach_plan = in_foreach_plan
      expression.nest_context    = nest_context
      expression.to_pig(pig_context, current_plan, current_op)
      current_plan
    end
  end  
end
