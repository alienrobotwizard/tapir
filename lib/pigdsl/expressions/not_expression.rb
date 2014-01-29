import 'org.apache.pig.newplan.logical.expression.NotExpression'

module LogicalExpression

  class NotExpression < Expression
    attr_accessor :rhs

    def initialize rhs
      @rhs = rhs
    end
    
    def self.from_hash hsh
      NotExpression.new(LogicalExpression.from_hash(hsh[:rhs]))
    end
    
    def to_hash
      {
        :type => 'not',
        :rhs  => rhs.to_hash
      }
    end

    def to_json
      to_hash.to_json
    end

    def to_pig pig_context, current_plan, current_op
      rhs.in_foreach_plan = in_foreach_plan
      rhs.nest_context    = nest_context
      org.apache.pig.newplan.logical.expression.NotExpression.new(current_plan, rhs.to_pig(pig_context, current_plan, current_op))
    end
    
  end
  
end
