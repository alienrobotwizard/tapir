import 'org.apache.pig.newplan.logical.expression.ConstantExpression'

module LogicalExpression

    class ConstantExpression < Expression
    attr_accessor :val

    def initialize val
      @val = val
    end
    
    def self.from_hash hsh
      ConstantExpression.new(hsh[:val])
    end
    
    def to_hash
      {
        :type => 'const',
        :val  => val # FIXME: May need adapter for data types
      }
    end

    def to_json
      to_hash.to_json
    end

    def to_pig pig_context, current_plan, current_op
      org.apache.pig.newplan.logical.expression.ConstantExpression.new(current_plan, val)
    end    
    
  end
  
end
