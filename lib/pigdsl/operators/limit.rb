import 'org.apache.pig.newplan.logical.relational.LOLimit'

module LogicalOperator
  
  class Limit < Operator
    attr_accessor :alias # Name of the output relation    
    attr_accessor :input # Array of input relation (or inner bag) names

    # FIXME: Limit is optionally, instead of a number, allowed a LogicalExpressionPlan
    # This plan, afaik, is intended to allow the use of scalars to limit, which
    # are not yet implemented.
    attr_accessor :limit # Number specifying number of tuples

    def initialize aliaz, input, limit
      @alias = aliaz
      @input = input
      @limit = limit
    end

    def self.from_hash hsh
      aliaz = hsh[:alias]
      input = hsh[:input]
      limit = hsh[:limit]
      Limit.new(aliaz, input, limit)
    end

    def to_hash
      {
        :operator => 'limit',
        :alias    => @alias,
        :input    => input,
        :limit    => limit
      }
    end

    def to_json
      to_hash.to_json
    end

    def to_pig pig_context, current_plan, current_op, nest_context = {}
      
      l = LOLimit.new(current_plan, limit)
      
      if in_nest_plan
        LogicalOperator.build_nested(l, @alias, current_plan, input_ops)
      end

      return l
    end    
    
  end
  
end
