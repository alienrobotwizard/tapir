import 'org.apache.pig.newplan.logical.expression.OrExpression'
import 'org.apache.pig.newplan.logical.expression.AndExpression'
import 'org.apache.pig.newplan.logical.expression.ModExpression'
import 'org.apache.pig.newplan.logical.expression.AddExpression'
import 'org.apache.pig.newplan.logical.expression.RegexExpression'
import 'org.apache.pig.newplan.logical.expression.SubtractExpression'
import 'org.apache.pig.newplan.logical.expression.MultiplyExpression'
import 'org.apache.pig.newplan.logical.expression.DivideExpression'
import 'org.apache.pig.newplan.logical.expression.NegativeExpression'
import 'org.apache.pig.newplan.logical.expression.NotEqualExpression'
import 'org.apache.pig.newplan.logical.expression.EqualExpression'
import 'org.apache.pig.newplan.logical.expression.LessThanEqualExpression'
import 'org.apache.pig.newplan.logical.expression.GreaterThanEqualExpression'
import 'org.apache.pig.newplan.logical.expression.GreaterThanExpression'
import 'org.apache.pig.newplan.logical.expression.LessThanExpression'

module LogicalExpression

  class BinaryExpression < Expression
    attr_accessor :rhs, :lhs

    def initialize lhs, rhs
      @lhs = lhs
      @rhs = rhs
    end
    
    def to_hash
      {
        :rhs => rhs.to_hash,
        :lhs => lhs.to_hash
      }
    end

    def to_json
      to_hash.to_json
    end

    def to_pig pig_context, current_plan, current_op
      rhs.in_foreach_plan = in_foreach_plan
      rhs.nest_context    = nest_context
      lhs.in_foreach_plan = in_foreach_plan
      lhs.nest_context    = nest_context
    end
    
  end

  class OrExpression < BinaryExpression
    def self.from_hash hsh
      lhs = LogicalExpression.from_hash(hsh[:lhs])
      rhs = LogicalExpression.from_hash(hsh[:rhs])
      OrExpression.new(lhs, rhs)
    end

    def to_hash
      super.merge({:type => 'or'})
    end

    def to_pig pig_context, current_plan, current_op
      super
      org.apache.pig.newplan.logical.expression.OrExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end

  class AndExpression < BinaryExpression
    def self.from_hash hsh
      lhs = LogicalExpression.from_hash(hsh[:lhs])
      rhs = LogicalExpression.from_hash(hsh[:rhs])
      AndExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.merge({:type => 'and'})
    end

    def to_pig pig_context, current_plan, current_op
      super
      org.apache.pig.newplan.logical.expression.AndExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end  

  class EqualExpression < BinaryExpression
    def self.from_hash hsh
      lhs = LogicalExpression.from_hash(hsh[:lhs])
      rhs = LogicalExpression.from_hash(hsh[:rhs])
      EqualExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.merge({:type => 'equal'})
    end

    def to_pig pig_context, current_plan, current_op
      super
      org.apache.pig.newplan.logical.expression.EqualExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end

  class GreaterThanEqualExpression < BinaryExpression
    def self.from_hash hsh
      lhs = LogicalExpression.from_hash(hsh[:lhs])
      rhs = LogicalExpression.from_hash(hsh[:rhs])
      GreaterThanEqualExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.merge({:type => 'greater_than_or_eq'})
    end

    def to_pig pig_context, current_plan, current_op
      super
      org.apache.pig.newplan.logical.expression.GreaterThanEqualExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end

  class LessThanEqualExpression < BinaryExpression
    def self.from_hash hsh
      lhs = LogicalExpression.from_hash(hsh[:lhs])
      rhs = LogicalExpression.from_hash(hsh[:rhs])
      LessThanEqualExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.merge({:type => 'less_than_or_eq'})
    end

    def to_pig pig_context, current_plan, current_op
      org.apache.pig.newplan.logical.expression.LessThanEqualExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end

  class LessThanExpression < BinaryExpression
    def self.from_hash hsh
      lhs = LogicalExpression.from_hash(hsh[:lhs])
      rhs = LogicalExpression.from_hash(hsh[:rhs])
      LessThanExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.merge({:type => 'less_than'})
    end

    def to_pig pig_context, current_plan, current_op
      super
      org.apache.pig.newplan.logical.expression.LessThanExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end

  class GreaterThanExpression < BinaryExpression
    def self.from_hash hsh
      lhs = LogicalExpression.from_hash(hsh[:lhs])
      rhs = LogicalExpression.from_hash(hsh[:rhs])
      GreaterThanExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.merge({:type => 'greater_than'})
    end

    def to_pig pig_context, current_plan, current_op
      super
      org.apache.pig.newplan.logical.expression.GreaterThanExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end

  class NotEqualExpression < BinaryExpression
    def self.from_hash hsh
      lhs = LogicalExpression.from_hash(hsh[:lhs])
      rhs = LogicalExpression.from_hash(hsh[:rhs])
      NotEqualExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.merge({:type => 'not_equal'})
    end

    def to_pig pig_context, current_plan, current_op
      super
      org.apache.pig.newplan.logical.expression.NotEqualExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end

  class AddExpression < BinaryExpression
    def self.from_hash hsh
      lhs = LogicalExpression.from_hash(hsh[:lhs])
      rhs = LogicalExpression.from_hash(hsh[:rhs])
      AddExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.merge({:type => 'plus'})
    end

    def to_pig pig_context, current_plan, current_op
      super
      org.apache.pig.newplan.logical.expression.AddExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end

  class SubtractExpression < BinaryExpression
    def self.from_hash hsh
      lhs = LogicalExpression.from_hash(hsh[:lhs])
      rhs = LogicalExpression.from_hash(hsh[:rhs])
      SubtractExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.merge({:type => 'minus'})
    end

    def to_pig pig_context, current_plan, current_op
      super
      org.apache.pig.newplan.logical.expression.SubtractExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end

  class MultiplyExpression < BinaryExpression
    def self.from_hash hsh
      lhs = LogicalExpression.from_hash(hsh[:lhs])
      rhs = LogicalExpression.from_hash(hsh[:rhs])
      MultiplyExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.merge({:type => 'star'}) # confusing?
    end

    def to_pig pig_context, current_plan, current_op
      super
      org.apache.pig.newplan.logical.expression.MultiplyExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end

  class DivideExpression < BinaryExpression
    def self.from_hash hsh
      lhs = LogicalExpression.from_hash(hsh[:lhs])
      rhs = LogicalExpression.from_hash(hsh[:rhs])
      DivideExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.merge({:type => 'div'})
    end

    def to_pig pig_context, current_plan, current_op
      super
      org.apache.pig.newplan.logical.expression.DivideExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end

  class ModExpression < BinaryExpression
    def self.from_hash hsh
      lhs = LogicalExpression.from_hash(hsh[:lhs])
      rhs = LogicalExpression.from_hash(hsh[:rhs])
      ModExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.merge({:type => 'percent'}) # confusing?
    end

    def to_pig pig_context, current_plan, current_op
      super
      org.apache.pig.newplan.logical.expression.ModExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end

  class NegativeExpression < BinaryExpression
    def self.from_hash hsh
      lhs = LogicalExpression.from_hash(hsh[:lhs])
      rhs = LogicalExpression.from_hash(hsh[:rhs])
      NegativeExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.merge({:type => 'neg'})
    end

    def to_pig pig_context, current_plan, current_op
      super
      org.apache.pig.newplan.logical.expression.NegativeExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end

  class RegexExpression < BinaryExpression
    def self.from_hash hsh
      lhs = LogicalExpression.from_hash(hsh[:lhs])
      rhs = LogicalExpression.from_hash(hsh[:rhs])
      RegexExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.merge({:type => 'regex'})
    end

    def to_pig pig_context, current_plan, current_op
      super
      org.apache.pig.newplan.logical.expression.RegexExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end
  
end
