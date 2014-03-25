import 'org.apache.pig.newplan.logical.relational.LOUnion'

module LogicalOperator

  class Union < Operator
    attr_accessor :alias # Name of the output relation
    attr_accessor :input # Array of input relation names
    attr_accessor :on_schema # Boolean indicating whether to union on schema or not

    def initialize aliaz, input, on_schema
      @alias     = aliaz
      @input     = input
      @on_schema = on_schema
    end

    def self.from_hash hsh
      aliaz     = hsh[:alias]
      input     = hsh[:input]
      on_schema = hsh[:on_schema]
      Union.new(aliaz, input, on_schema)
    end

    def to_hash
      {
        :operator  => 'union',
        :alias     => @alias,
        :input     => input,
        :on_schema => on_schema
      }
    end

    def to_json
      to_hash.to_json
    end

    def to_pig pig_context, current_plan, current_op, nest_context = {}
      raise "Nested union not allowed" if in_nest_plan      
      union = LOUnion.new(current_plan, on_schema)      
      return union
    end
    
  end
  
end
