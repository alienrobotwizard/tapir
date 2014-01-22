import 'org.apache.pig.FuncSpec'
import 'org.apache.pig.parser.FunctionType'
import 'org.apache.pig.newplan.logical.expression.OrExpression'
import 'org.apache.pig.newplan.logical.expression.AndExpression'
import 'org.apache.pig.newplan.logical.expression.NotExpression'
import 'org.apache.pig.newplan.logical.expression.ModExpression'
import 'org.apache.pig.newplan.logical.expression.AddExpression'
import 'org.apache.pig.newplan.logical.expression.IsNullExpression'
import 'org.apache.pig.newplan.logical.expression.RegexExpression'
import 'org.apache.pig.newplan.logical.expression.SubtractExpression'
import 'org.apache.pig.newplan.logical.expression.MultiplyExpression'
import 'org.apache.pig.newplan.logical.expression.DivideExpression'
import 'org.apache.pig.newplan.logical.expression.NegativeExpression'
import 'org.apache.pig.newplan.logical.expression.ConstantExpression'
import 'org.apache.pig.newplan.logical.expression.NotEqualExpression'
import 'org.apache.pig.newplan.logical.expression.EqualExpression'
import 'org.apache.pig.newplan.logical.expression.LessThanEqualExpression'
import 'org.apache.pig.newplan.logical.expression.GreaterThanEqualExpression'
import 'org.apache.pig.newplan.logical.expression.GreaterThanExpression'
import 'org.apache.pig.newplan.logical.expression.LessThanExpression'
import 'org.apache.pig.newplan.logical.expression.ProjectExpression'
import 'org.apache.pig.newplan.logical.expression.UserFuncExpression'
import 'org.apache.pig.newplan.logical.expression.LogicalExpressionPlan'

#
# Adapters
#
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

    def to_pig
      graph.to_pig(pig_context, current_plan, current_op)
      current_plan # return the current logical expression plan
    end    
    
  end
  
  class UserFuncExpression 
    attr_accessor :args # An array of LogicalExpressions
    attr_accessor :func # Fully qualified string class name

    def initialize func, args
      @func = func
      @args = args
    end
    
    def self.from_hash hsh
      args = hsh[:args].map{|arg| EXPRESSIONS[arg[:type]].from_hash(arg) }
      func = hsh[:func]
      UserFuncExpression.new(func, args)
    end
    
    def to_hash
      {
        :type => 'func_eval',
        :args => args.map{|arg| arg.to_hash },
        :func => func
      }
    end
    
    def to_json
      to_hash.to_json
    end

    def to_pig pig_context, current_plan, current_op
      func_args  = args.map{|arg| arg.to_pig(pig_context, current_plan, current_op) }
      func_clazz = pig_context.get_class_for_alias(func)
      func_spec  = pig_context.get_func_spec_from_alias(func)
      
      if (!func_spec)
        func_name = func_clazz.get_name
        func_spec = FuncSpec.new(func_name)

        return org.apache.pig.newplan.logical.expression.UserFuncExpression.new(current_plan, func_spec, func_args, false)
      else
        return org.apache.pig.newplan.logical.expression.UserFuncExpression.new(current_plan, func_spec, func_args, true)
      end    
    end
    
  end

  class ProjectExpression
    attr_accessor :alias

    def initialize aliaz
      @alias = aliaz
    end
    
    def self.from_hash hsh
      ProjectExpression.new(hsh[:alias])      
    end
    
    def to_hash
      {
        :type  => 'col_ref',
        :alias => @alias
      }
    end

    def to_json
      to_hash.to_json
    end

    def to_pig pig_context, current_plan, current_op
      org.apache.pig.newplan.logical.expression.ProjectExpression.new(current_plan, 0, @alias, nil, current_op)
    end
    
  end

  class NotExpression
    attr_accessor :rhs

    def initialize rhs
      @rhs = rhs
    end
    
    def self.from_hash hsh
      NotExpression.new(EXPRESSIONS[hsh[:type]].from_hash(hsh[:rhs]))
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
      org.apache.pig.newplan.logical.expression.NotExpression.new(current_plan, rhs.to_pig(pig_context, current_plan, current_op))
    end
    
  end

  class IsNullExpression
    attr_accessor :rhs

    def initialize rhs
      @rhs = rhs
    end
    
    def self.from_hash hsh
      NullExpression.new(EXPRESSIONS[hsh[:type]].from_hash(hsh[:rhs]))
    end
        
    def to_hash
      {
        :type => 'null',
        :rhs  => rhs.to_hash
      }
    end

    def to_json
      to_hash.to_json
    end

    def to_pig pig_context, current_plan, current_op      
      org.apache.pig.newplan.logical.expression.IsNullExpression.new(current_plan, rhs.to_pig(pig_context, current_plan, current_op))
    end
  end

  class ConstantExpression
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
  
  class BinaryExpression
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
  end  
    
  class OrExpression < BinaryExpression
    def self.from_hash hsh
      lhs = EXPRESSIONS[hsh[:lhs][:type]].from_hash(hsh[:lhs])
      rhs = EXPRESSIONS[hsh[:rhs][:type]].from_hash(hsh[:rhs])
      OrExpression.new(lhs, rhs)
    end

    def to_hash
      super.to_hash.merge({:type => 'or'})
    end

    def to_pig pig_context, current_plan, current_op
      org.apache.pig.newplan.logical.expression.OrExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end

  class AndExpression < BinaryExpression
    def self.from_hash hsh
      lhs = EXPRESSIONS[hsh[:lhs][:type]].from_hash(hsh[:lhs])
      rhs = EXPRESSIONS[hsh[:rhs][:type]].from_hash(hsh[:rhs])
      AndExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.to_hash.merge({:type => 'and'})
    end

    def to_pig pig_context, current_plan, current_op
      org.apache.pig.newplan.logical.expression.AndExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end  

  class EqualExpression < BinaryExpression
    def self.from_hash hsh
      lhs = EXPRESSIONS[hsh[:lhs][:type]].from_hash(hsh[:lhs])
      rhs = EXPRESSIONS[hsh[:rhs][:type]].from_hash(hsh[:rhs])
      EqualExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.to_hash.merge({:type => 'equal'})
    end

    def to_pig pig_context, current_plan, current_op
      org.apache.pig.newplan.logical.expression.EqualExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end

  class GreaterThanEqualExpression < BinaryExpression
    def self.from_hash hsh
      lhs = EXPRESSIONS[hsh[:lhs][:type]].from_hash(hsh[:lhs])
      rhs = EXPRESSIONS[hsh[:rhs][:type]].from_hash(hsh[:rhs])
      GreaterThanEqualExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.to_hash.merge({:type => 'greater_than_or_eq'})
    end

    def to_pig pig_context, current_plan, current_op
      org.apache.pig.newplan.logical.expression.GreaterThanEqualExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end

  class LessThanEqualExpression < BinaryExpression
    def self.from_hash hsh
      lhs = EXPRESSIONS[hsh[:lhs][:type]].from_hash(hsh[:lhs])
      rhs = EXPRESSIONS[hsh[:rhs][:type]].from_hash(hsh[:rhs])
      LessThanEqualExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.to_hash.merge({:type => 'less_than_or_eq'})
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
      lhs = EXPRESSIONS[hsh[:lhs][:type]].from_hash(hsh[:lhs])
      rhs = EXPRESSIONS[hsh[:rhs][:type]].from_hash(hsh[:rhs])
      LessThanExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.to_hash.merge({:type => 'less_than'})
    end

    def to_pig pig_context, current_plan, current_op
      org.apache.pig.newplan.logical.expression.LessThanExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end

  class GreaterThanExpression < BinaryExpression
    def self.from_hash hsh
      lhs = EXPRESSIONS[hsh[:lhs][:type]].from_hash(hsh[:lhs])
      rhs = EXPRESSIONS[hsh[:rhs][:type]].from_hash(hsh[:rhs])
      GreaterThanExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.to_hash.merge({:type => 'greater_than'})
    end

    def to_pig pig_context, current_plan, current_op
      org.apache.pig.newplan.logical.expression.GreaterThanExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end

  class NotEqualExpression < BinaryExpression
    def self.from_hash hsh
      lhs = EXPRESSIONS[hsh[:lhs][:type]].from_hash(hsh[:lhs])
      rhs = EXPRESSIONS[hsh[:rhs][:type]].from_hash(hsh[:rhs])
      NotEqualExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.to_hash.merge({:type => 'not_equal'})
    end

    def to_pig pig_context, current_plan, current_op
      org.apache.pig.newplan.logical.expression.NotEqualExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end

  class AddExpression < BinaryExpression
    def self.from_hash hsh
      lhs = EXPRESSIONS[hsh[:lhs][:type]].from_hash(hsh[:lhs])
      rhs = EXPRESSIONS[hsh[:rhs][:type]].from_hash(hsh[:rhs])
      AddExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.to_hash.merge({:type => 'plus'})
    end

    def to_pig pig_context, current_plan, current_op
      org.apache.pig.newplan.logical.expression.AddExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end

  class SubtractExpression < BinaryExpression
    def self.from_hash hsh
      lhs = EXPRESSIONS[hsh[:lhs][:type]].from_hash(hsh[:lhs])
      rhs = EXPRESSIONS[hsh[:rhs][:type]].from_hash(hsh[:rhs])
      SubtractExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.to_hash.merge({:type => 'minus'})
    end

    def to_pig pig_context, current_plan, current_op
      org.apache.pig.newplan.logical.expression.SubtractExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end

  class MultiplyExpression < BinaryExpression
    def self.from_hash hsh
      lhs = EXPRESSIONS[hsh[:lhs][:type]].from_hash(hsh[:lhs])
      rhs = EXPRESSIONS[hsh[:rhs][:type]].from_hash(hsh[:rhs])
      MultiplyExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.to_hash.merge({:type => 'star'}) # confusing?
    end

    def to_pig pig_context, current_plan, current_op
      org.apache.pig.newplan.logical.expression.MultiplyExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end

  class DivideExpression < BinaryExpression
    def self.from_hash hsh
      lhs = EXPRESSIONS[hsh[:lhs][:type]].from_hash(hsh[:lhs])
      rhs = EXPRESSIONS[hsh[:rhs][:type]].from_hash(hsh[:rhs])
      DivideExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.to_hash.merge({:type => 'div'})
    end

    def to_pig pig_context, current_plan, current_op
      org.apache.pig.newplan.logical.expression.DivideExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end

  class ModExpression < BinaryExpression
    def self.from_hash hsh
      lhs = EXPRESSIONS[hsh[:lhs][:type]].from_hash(hsh[:lhs])
      rhs = EXPRESSIONS[hsh[:rhs][:type]].from_hash(hsh[:rhs])
      ModExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.to_hash.merge({:type => 'percent'}) # confusing?
    end

    def to_pig pig_context, current_plan, current_op
      org.apache.pig.newplan.logical.expression.ModExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end

  class NegativeExpression < BinaryExpression
    def self.from_hash hsh
      lhs = EXPRESSIONS[hsh[:lhs][:type]].from_hash(hsh[:lhs])
      rhs = EXPRESSIONS[hsh[:rhs][:type]].from_hash(hsh[:rhs])
      NegativeExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.to_hash.merge({:type => 'neg'})
    end

    def to_pig pig_context, current_plan, current_op
      org.apache.pig.newplan.logical.expression.NegativeExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end

  class RegexExpression < BinaryExpression
    def self.from_hash hsh
      lhs = EXPRESSIONS[hsh[:lhs][:type]].from_hash(hsh[:lhs])
      rhs = EXPRESSIONS[hsh[:rhs][:type]].from_hash(hsh[:rhs])
      RegexExpression.new(lhs, rhs)
    end
    
    def to_hash
      super.to_hash.merge({:type => 'regex'})
    end

    def to_pig pig_context, current_plan, current_op
      org.apache.pig.newplan.logical.expression.RegexExpression.new(
        current_plan,
        lhs.to_pig(pig_context, current_plan, current_op),
        rhs.to_pig(pig_context, current_plan, current_op)
        )
    end
  end

  EXPRESSIONS = {
    'or'                 => LogicalExpression::OrExpression,
    'and'                => LogicalExpression::AndExpression,
    'not'                => LogicalExpression::NotExpression,
    'null'               => LogicalExpression::IsNullExpression,
    'equal'              => LogicalExpression::EqualExpression,
    'less_than'          => LogicalExpression::LessThanExpression,
    'less_than_or_eq'    => LogicalExpression::LessThanEqualExpression,
    'greater_than_or_eq' => LogicalExpression::GreaterThanEqualExpression,
    'greater_than'       => LogicalExpression::GreaterThanExpression,
    'not_equal'          => LogicalExpression::EqualExpression,
    'matches'            => LogicalExpression::RegexExpression,
    'plus'               => LogicalExpression::AddExpression,
    'minus'              => LogicalExpression::SubtractExpression,
    'star'               => LogicalExpression::MultiplyExpression,
    'div'                => LogicalExpression::DivideExpression,
    'percent'            => LogicalExpression::ModExpression,
    'neg'                => LogicalExpression::NegativeExpression,
    'const'              => LogicalExpression::ConstantExpression,
    'func_eval'          => LogicalExpression::UserFuncExpression,
    'col_ref'            => LogicalExpression::ProjectExpression
  }
  
end
