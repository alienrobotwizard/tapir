import 'org.apache.pig.newplan.logical.relational.LOSplit'
import 'org.apache.pig.newplan.logical.relational.LOSplitOutput'
import 'org.apache.pig.newplan.logical.expression.OrExpression'
import 'org.apache.pig.newplan.logical.expression.NotExpression'
import 'org.apache.pig.newplan.logical.rules.OptimizerUtils'
import 'org.apache.pig.newplan.logical.expression.LogicalExpressionPlan'

module LogicalOperator

  class Split < Operator
    attr_accessor :input  # Array of input relation (or inner bag) names    
    attr_accessor :splits # Map of :relation_name => condition

    # Optional name of relation to direct output to that
    # doesn't match one of the split conditions
    attr_accessor :otherwise 

    def initialize aliaz, input, splits, otherwise
      @alias     = aliaz
      @input     = input
      @splits    = splits
      @otherwise = otherwise      
    end

    def self.from_hash hsh
      aliaz  = hsh[:alias]
      input  = hsh[:input]
      splits = hsh[:splits].inject({}) do |hsh, kv|
        hsh[kv.first] = LogicalExpression.from_hash(kv.last)
        hsh
      end
      otherwise = hsh[:otherwise]
      Split.new(aliaz, input, splits, otherwise)
    end

    def to_hash
      {
        :operator  => 'split',
        :alias     => @alias,
        :input     => input,
        :splits    => splits.inject({}){|hsh,kv| hsh[kv.first] = kv.last.to_hash; hsh},
        :otherwise => otherwise
      }
    end

    def to_json
      to_hash.to_json
    end

    #
    # Hacky way to get around not having access
    # to the plan this split is a part of. Each
    # LOSplitOutput needs to be attached to its
    # input.
    #
    def get_splits
      @logical_splits
    end

    def split_otherwise current_plan
      result     = LOSplitOutput.new(current_plan)
      split_plan = LogicalExpressionPlan.new
      current    = nil
      @logical_splits.each do |split|
        fragment = split[:pig_op].get_filter_plan
        if (OptimizerUtils.plan_has_non_deterministic_udf(fragment))
          raise "Can not use Otherwise in Split with an expression containing a @Nondeterministic UDF"
        end
        root    = fragment.get_sources.get(0).deep_copy(split_plan)
        current = (current ? OrExpression.new(split_plan, current, root) : root) 
      end

      current = NotExpression.new(split_plan, current)
      result.set_filter_plan(split_plan)
      @logical_splits << {:pig_op => result, :alias => otherwise}
    end
    
    def to_pig pig_context, current_plan, current_op, nest_context = {}
      raise "Nested split not allowed" if in_nest_plan
      
      split = LOSplit.new(current_plan)

      @logical_splits = splits.map do |rel, cond|
        splitoutput = LOSplitOutput.new(current_plan)
        filter_plan = LogicalExpression::Plan.new(pig_context, splitoutput).to_pig(cond, false, nest_context)
        splitoutput.set_filter_plan(filter_plan)
        {:pig_op => splitoutput, :alias => rel}
      end

      split_otherwise(current_plan) if otherwise
      
      return split     
    end
    
  end
  
end
