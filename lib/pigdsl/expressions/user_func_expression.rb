import 'org.apache.pig.newplan.logical.expression.UserFuncExpression'

module LogicalExpression
  
  class UserFuncExpression < Expression
    attr_accessor :args # An array of LogicalExpressions
    attr_accessor :func # Fully qualified string class name

    def initialize func, args
      @func = func
      @args = args
    end
    
    def self.from_hash hsh
      args = hsh[:args].map{|arg| LogicalExpression.from_hash(arg) }
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
      func_args  = args.map do |arg|
        arg.in_foreach_plan = in_foreach_plan
        arg.nest_context    = nest_context
        arg.to_pig(pig_context, current_plan, current_op)
      end
      
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
end
