import 'org.apache.pig.newplan.logical.relational.LOFilter'

module LogicalOperator

  class Filter < Operator
    attr_accessor :alias     # Name of the output relation
    attr_accessor :input     # Array of input relation (or inner bag) names
    attr_accessor :condition # A LogicalExpression::Plan

    def initialize aliaz, input, condition
      @alias     = aliaz
      @input     = input
      @condition = condition      
    end

    def self.from_hash hsh
      aliaz     = hsh[:alias]
      input     = hsh[:input]
      condition = LogicalExpression.from_hash(hsh[:condition])
      Filter.new(aliaz, input, condition)
    end

    def to_hash
      {
        :operator  => 'filter',
        :alias     => @alias,
        :input     => input,
        :condition => condition.to_hash 
      }
    end

    def to_json
      to_hash.to_json
    end

    def to_pig pig_context, current_plan, current_op, nest_context = {}
      filter                = LOFilter.new(current_plan)
      condition.input_index = 0
      filter_plan           = LogicalExpression::Plan.new(pig_context, filter).to_pig(condition, in_foreach_plan, nest_context)
      filter.set_filter_plan(filter_plan)

      if in_nest_plan
        LogicalOperator.build_nested(filter, @alias, current_plan, input_ops)
      end
      
      return filter      
    end    
    
  end
  
end
