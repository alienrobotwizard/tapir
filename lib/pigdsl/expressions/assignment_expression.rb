module LogicalExpression

  class AssignmentExpression < Expression
    attr_accessor :alias
    attr_accessor :rhs

    # internal, for nest plans
    attr_accessor :input, :input_ops
    
    def initialize aliaz, rhs
      @alias = aliaz
      @rhs   = rhs
      
      @input     = [] # shh.
      @input_ops = []
    end
    
    def self.from_hash hsh
      aliaz = hsh[:alias]
      rhs   = LogicalExpression.from_hash(hsh[:rhs])
      AssignmentExpression.new(aliaz, rhs)
    end

    def to_hash
      {
        :type  => 'assign',
        :alias => @alias,
        :rhs   => rhs.to_hsh
      }
    end

    def to_json
      to_hash.to_json
    end
    
    # Special case, return the right hand side's to_pig
    def to_pig pig_context, current_plan, current_op
      rhs.in_foreach_plan = in_foreach_plan
      rhs.nest_context    = nest_context
      rhs.to_pig(pig_context, current_plan, current_op)
    end
    
  end
  
end
