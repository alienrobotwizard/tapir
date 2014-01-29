import 'org.apache.pig.newplan.logical.relational.LOGenerate'
import 'org.apache.pig.newplan.logical.relational.LOInnerLoad'

module LogicalOperator

  class Generate < Operator
    attr_accessor :results  # Array of LogicalExpressions to generate
    attr_accessor :flattens # Array of booleans corresponding to which results to flatten 

    def initialize results, flattens
      @results  = results
      @flattens = flattens
    end

    def self.from_hash hsh
      results  = hsh[:results].map{|result| LogicalExpression.from_hash(result) }
      flattens = hsh[:flattens]
      Generate.new(results, flattens)
    end

    def to_hash
      {
        :operator => 'generate',
        :results  => results.map{|result| result.to_hash},
        :flattens => flattens
      }
    end

    def to_json
      to_hash.to_json
    end
    
    def to_pig pig_context, current_plan, current_op, nest_context = {}

      gen = LOGenerate.new(current_plan)

      plans = results.map do |result|
        LogicalExpression::Plan.new(pig_context, gen).to_pig(result, in_foreach_plan, nest_context)
      end
      
      inner_plan = gen.get_plan
      inputs     = []

      # Build inner load statements
      plans.each_with_index do |plan, idx|
        LogicalOperator.process_expression_plan(current_op, inner_plan, plan, inputs)
      end
      
      gen.set_output_plans(plans)
      gen.set_flatten_flags(flattens.to_java(:boolean))
      gen.set_user_defined_schema([]) # FIXME: Need to actually handle schema
      inner_plan.add(gen) # what witchcraft is this!?
      
      # Connect generate's inputs to it
      inputs.each{|input| inner_plan.connect(input, gen) }
    end
    
  end

  #
  # These are some unpleasant pig functions required for setting up generate
  # operators correctly with inputs
  #
  def self.process_expression_plan foreach, logical_plan, logical_exp_plan, inputs
    logical_exp_plan.get_operators.each do |op|
      
      if op.is_a? ProjectExpression
        col_alias = op.get_col_alias        
        if col_alias
          projected = op.get_projected_operator
          if projected
            idx = inputs.index(projected)
            if !idx
              idx = inputs.size
              inputs << projected
            end
            op.set_input_num(idx)
            op.set_col_num(-1)            
          else
            lol = LOInnerLoad.new(logical_plan, foreach, col_alias)
            setup_innerload_and_proj(lol, op, logical_plan, inputs)
          end         
        end        
      end
      
    end
    
  end

  def self.setup_innerload_and_proj inner_load, project_expr, logical_plan, inputs
    project_expr.set_input_num(inputs.size)
    project_expr.set_col_num(-1)
    logical_plan.add(inner_load)
    inputs << inner_load
  end
  
end
