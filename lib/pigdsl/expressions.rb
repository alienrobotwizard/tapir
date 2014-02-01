module LogicalExpression

  autoload :Plan,                 'pigdsl/expressions/plan'
  autoload :UserFuncExpression,   'pigdsl/expressions/user_func_expression'
  autoload :ProjectExpression,    'pigdsl/expressions/project_expression'
  autoload :NotExpression,        'pigdsl/expressions/not_expression'
  autoload :IsNullExpression,     'pigdsl/expressions/is_null_expression'
  autoload :AssignmentExpression, 'pigdsl/expressions/assignment_expression' # No pig equivalent
  autoload :ConstantExpression,   'pigdsl/expressions/constant_expression'
  
  autoload :OrExpression,               'pigdsl/expressions/binary_expression'
  autoload :AndExpression,              'pigdsl/expressions/binary_expression'
  autoload :ModExpression,              'pigdsl/expressions/binary_expression'
  autoload :AddExpression,              'pigdsl/expressions/binary_expression'
  autoload :RegexExpression,            'pigdsl/expressions/binary_expression'
  autoload :SubtractExpression,         'pigdsl/expressions/binary_expression'
  autoload :MultiplyExpression,         'pigdsl/expressions/binary_expression'
  autoload :DivideExpression,           'pigdsl/expressions/binary_expression'
  autoload :NegativeExpression,         'pigdsl/expressions/binary_expression'
  autoload :NotEqualExpression,         'pigdsl/expressions/binary_expression'
  autoload :EqualExpression,            'pigdsl/expressions/binary_expression'
  autoload :LessThanEqualExpression,    'pigdsl/expressions/binary_expression'
  autoload :GreaterThanEqualExpression, 'pigdsl/expressions/binary_expression'
  autoload :GreaterThanExpression,      'pigdsl/expressions/binary_expression'
  autoload :LessThanExpression,         'pigdsl/expressions/binary_expression'
  
  
  def self.from_hash hsh
    EXPRESSIONS[hsh[:type]].from_hash(hsh)
  end
 
  class Expression
    attr_accessor :in_nest_plan, :in_foreach_plan, :nest_context, :input_index
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
    'col_ref'            => LogicalExpression::ProjectExpression,
    # special case, only works in nested foreach plan
    'assign'             => LogicalExpression::AssignmentExpression
  }
  
end
