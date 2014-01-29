import 'org.apache.pig.newplan.logical.relational.LODistinct'

module LogicalOperator

  class Distinct < Operator
    attr_accessor :alias # Name of the output relation
    attr_accessor :input # Array of input relation (or inner bag) names

    def initialize aliaz, input
      @alias = aliaz
      @input = input
    end

    def self.from_hash hsh
      aliaz = hsh[:alias]
      input = hsh[:input]
      Distinct.new(aliaz, input)
    end

    def to_hash
      {
        :operator => 'distinct',
        :alias    => @alias,
        :input    => input
      }
    end

    def to_json
      to_hash.to_json
    end

    def to_pig pig_context, current_plan, current_op, nest_context = {}
      distinct = LODistinct.new(current_plan)

      if in_nest_plan
        LogicalOperator.build_nested(distinct, @alias, current_plan, input_ops)
      end
      
      return distinct
    end
    
  end
  
end
