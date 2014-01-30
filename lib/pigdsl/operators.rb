import 'org.apache.pig.FuncSpec'
import 'org.apache.pig.parser.LogicalPlanBuilder'

module LogicalOperator

  autoload :Plan,     'pigdsl/operators/plan'
  autoload :Load,     'pigdsl/operators/load'
  autoload :Store,    'pigdsl/operators/store'
  autoload :Sort,     'pigdsl/operators/sort'
  autoload :Limit,    'pigdsl/operators/limit'
  autoload :Distinct, 'pigdsl/operators/distinct'
  autoload :Filter,   'pigdsl/operators/filter'
  autoload :ForEach,  'pigdsl/operators/foreach'
  autoload :Generate, 'pigdsl/operators/generate'
  autoload :Join,     'pigdsl/operators/join'

  class Operator
    attr_accessor :in_nest_plan, :in_foreach_plan, :input_ops
    # Optional
    attr_accessor :parallel_hint, :partitioner
  end

  OPERATORS = {
    'foreach'  => LogicalOperator::ForEach,
    'generate' => LogicalOperator::Generate,
    'load'     => LogicalOperator::Load,
    'store'    => LogicalOperator::Store,
    'filter'   => LogicalOperator::Filter,
    'limit'    => LogicalOperator::Limit,
    'distinct' => LogicalOperator::Distinct,
    'sort'     => LogicalOperator::Sort,
    'join'     => LogicalOperator::Join
  }


  
  def self.from_hash hsh
    if hsh.has_key? :operator
      OPERATORS[hsh[:operator]].from_hash(hsh)
    else
      LogicalExpression.from_hash(hsh)
    end    
  end

  def self.spec_for_name name, args = []
    if args.size > 0
      FuncSpec.new(name, args.to_java(:string))
    else      
      FuncSpec.new(name)
    end      
  end
  
  def self.func_for_name name, args = []
    PigContext.instantiate_func_from_spec(spec_for_name(name, args))
  end

  def self.set_alias(op, aliaz)
    if (!aliaz)
      aliaz = LogicalPlanBuilder.new_operator_key('')
    end
    op.set_alias(aliaz)
  end

  def self.set_partitioner(op, partitioner)
    if (partitioner)
      op.set_custom_partitioner(partitioner)
    end
  end

  def self.set_parallelism_hint(op, parallel)
    if (parallel)
      op.set_requested_parallelism(parallel)
    end
  end

  def self.build_nested op, aliaz, current_plan, inputs
    set_alias(op, aliaz)
    current_plan.add(op)
    inputs.each do |input|
      current_plan.connect(input, op)
    end        
  end  
  
end
