import 'org.apache.pig.newplan.logical.relational.LOForEach'
import 'org.apache.pig.newplan.logical.relational.LOInnerLoad'
import 'org.apache.pig.newplan.logical.relational.LogicalPlan'

module LogicalOperator

  class ForEach < Operator
    attr_accessor :alias # Name of the output relation
    attr_accessor :input # Array of input relation (or inner bag) names
    attr_accessor :graph # Array of operators

    # Internal
    attr_accessor :operators, :expression_plans
    
    def initialize aliaz, input, graph
      @alias = aliaz
      @input = input
      @graph = graph      
    end

    def self.from_hash hsh
      aliaz = hsh[:alias]
      input = hsh[:input]
      graph = hsh[:graph].map{|op| LogicalOperator.from_hash(op) }
      ForEach.new(aliaz, input, graph)
    end

    def to_hash
      {
        :operator => 'foreach',
        :alias    => @alias,
        :input    => input,
        :graph    => graph.map{|op| op.to_hash}
      }      
    end

    def to_json
      to_hash.to_json
    end

    def to_pig pig_context, current_plan, current_op, nest_context = {}
      foreach    = LOForEach.new(current_plan)
      inner_plan = LogicalPlan.new

      expression_plans = {}
      operators        = {}

      graph.each do |op|        
        op.in_nest_plan    = true
        op.in_foreach_plan = true
        
        if !op.is_a? Generate
          op.input_ops = nested_op_inputs(op.input, foreach, inner_plan, operators)
        end        
        
        pig_op = op.to_pig(pig_context, inner_plan, foreach, {:operators => operators, :expression_plans => expression_plans})
        
        if op.is_a? LogicalExpression::AssignmentExpression
          expression_plans[op.alias] = pig_op
        elsif !op.is_a? Generate
          operators[op.alias]        = pig_op
        end
                
      end
      foreach.set_inner_plan(inner_plan)
      return foreach
    end

    # inputs - list of input aliases    
    def nested_op_inputs inputs, foreach, inner_plan, operators
      ret = []
      inputs.each do |input|
        op = operators[input]
        if !op
          op = LOInnerLoad.new(inner_plan, foreach, input)
          inner_plan.add(op)
        end
        ret << op
      end
      ret
    end
    
  end
  
end
