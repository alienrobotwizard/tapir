import 'org.apache.pig.FuncSpec'
import 'org.apache.pig.parser.FunctionType'
import 'org.apache.pig.newplan.logical.expression.ModExpression'
import 'org.apache.pig.newplan.logical.expression.AddExpression'
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

class LogicalExpressionBuilder
  
  attr_accessor :plan, :pig_context, :current_op
  
  def initialize pig_context, current_op
    @plan        = LogicalExpressionPlan.new
    @pig_context = pig_context
    @current_op  = current_op
  end
  
  #
  # Action starts here. Builds a condition
  # as a LogicalExpressionPlan
  #
  def condition op
    case op['type']
    when 'equal' then
      build_equal(op)
    when 'less_than' then
      build_lessthan(op)
    when 'less_than_or_eq' then
      build_lessthanequal(op)
    when 'greater_than_or_eq' then
      build_greaterthanequal(op)
    when 'greater_than' then
      build_greaterthan(op)
    when 'not_equal' then
      build_notequal(op)
    end    
  end

  #
  # The names of these will likely have to change
  # as the protocol is iterated on
  #
  def expression op
    case op['type']
    when 'plus' then
      rhs = expression(op['rhs'])
      lhs = expression(op['lhs'])
      return AddExpression.new(plan, lhs, rhs)
    when 'minus' then
      rhs = expression(op['rhs'])
      lhs = expression(op['lhs'])
      return SubtractExpression.new(plan, lhs, rhs)
    when 'star' then
      rhs = expression(op['rhs'])
      lhs = expression(op['lhs'])
      return MultiplyExpression.new(plan, lhs, rhs)
    when 'div' then
      rhs = expression(op['rhs'])
      lhs = expression(op['lhs'])
      return DivideExpression.new(plan, lhs, rhs)
    when 'percent' then
      rhs = expression(op['rhs'])
      lhs = expression(op['lhs'])
      return ModExpression.new(plan, lhs, rhs)
    when 'neg' then
      rhs = expression(op['rhs'])
      return NegativeExpression.new(plan, rhs)
    when 'cast' then # avoid this one for now since it may not actually be necessary
    when 'const' then
      return build_constant(op)
    else
      return var_expression(op)
    end
  end

  def var_expression op
    return projectable_expression(op)    
  end  

  #
  # func_eval, col_ref, bin_expr, case_expr, case_cond 
  #
  def projectable_expression op
    case op['type']
    when 'func_eval' then
      return build_func_eval(op)
    when 'bin_expr' then # leave out for now; bluesky
    when 'case_expr' then
    when 'case_cond' then
      
    when 'col_ref' then
      return build_project(op)
    end
  end  

  #
  # Need to handle arg to function mapping somewhere
  # 
  def build_func_eval op
    args = []
    op['args'].each do |arg|
      args << expression(arg) # go straight to expression and skip range and star projections
    end
    func = pig_context.get_class_for_alias(op['func'])

    # FIXME:
    # Validate function; skip for now because FunctionType::EVALFUNC doesn't resolve
    #FunctionType.tryCasting(func, FunctionType::EVALFUNC)
    
    func_spec = pig_context.get_func_spec_from_alias(op['func'])
    if (!func_spec)
      func_name = func.get_name
      func_spec = FuncSpec.new(func_name)

      le = UserFuncExpression.new(plan, func_spec, args, false)
    else
      le = UserFuncExpression.new(plan, func_spec, args, true)
    end    
    return le
  end  



  
  def build_add
  end

  def build_and
  end

  def build_bincond
  end

  def build_binary
  end
  
  def build_cast
  end
  
  def build_column
  end
  
  def build_constant op
    ConstantExpression.new(plan, op['val'])
  end
  
  def build_dereference
  end
  
  def build_divide
  end
  
  def build_equal op
    lhs = expression(op['lhs'])
    rhs = expression(op['rhs'])
    EqualExpression.new(plan, lhs, rhs)
  end
  
  def build_greaterthanequal op
    lhs = expression(op['lhs'])
    rhs = expression(op['rhs'])
    GreaterThanEqualExpression.new(plan, lhs, rhs)
  end
  
  def build_greaterthan op
    lhs = expression(op['lhs'])
    rhs = expression(op['rhs'])
    GreaterThanExpression.new(plan, lhs, rhs)
  end
  
  def build_isnull
  end
  
  def build_lessthanequal op
    lhs = expression(op['lhs'])
    rhs = expression(op['rhs'])
    LessThanEqualExpression.new(plan, lhs, rhs)
  end
  
  def build_lessthan op
    lhs = expression(op['lhs'])
    rhs = expression(op['rhs'])
    LessThanExpression.new(plan, lhs, rhs)
  end
  
  def build_maplookup
  end
  
  def build_mod
  end
  
  def build_multiply
  end
  
  def build_negative
  end
  
  def build_notequal op
    lhs = expression(op['lhs'])
    rhs = expression(op['rhs'])
    NotEqualExpression.new(plan, lhs, rhs)
  end
  
  def build_not
  end
  
  def build_or
  end
  
  def build_project op
    ProjectExpression.new(plan, 0, op['alias'], nil, current_op)
  end
  
  def build_regex
  end
  
  def build_scalar
  end
  
  def build_subtract
  end
  
  def build_unary
  end
  
  def build_userfunc
  end
  
end
