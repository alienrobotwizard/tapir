import 'org.apache.pig.newplan.logical.expression.ProjectExpression'

module LogicalExpression
  
  class ProjectExpression < Expression
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

    def build_nested current_plan, current_op
      operators   = nest_context[:operators]
      expressions = nest_context[:expression_plans]

      exp_plan = expressions[@alias]
      if exp_plan
        cp = exp_plan.deep_copy
        current_plan.merge(cp)

        current_plan.get_operators.each do |op|
          if op.is_a? org.apache.pig.newplan.logical.expression.ProjectExpression
            op.set_attached_relational_op(current_op)
          end          
        end

        root   = cp.get_sources.first
        schema = root.get_field_schema
        
        schema.alias = @alias if !schema.alias
        
        root
      elsif !@alias
        # Project star
        org.apache.pig.newplan.logical.expression.ProjectExpression.new(current_plan, input_index.to_java(:int), -1, current_op)
      else
        org.apache.pig.newplan.logical.expression.ProjectExpression.new(current_plan, input_index.to_java(:int), @alias, operators[@alias], current_op)
      end      
    end
    
    def to_pig pig_context, current_plan, current_op
      if in_foreach_plan && nest_context[:operators].has_key?(@alias)
        # inside nested foreach and column is defined in nested block
        build_nested(current_plan, current_op)
      else

        # FIXME: Add scalar here
        if in_foreach_plan
          build_nested(current_plan, current_op)
        elsif !@alias
          # Project star
          org.apache.pig.newplan.logical.expression.ProjectExpression.new(current_plan, input_index.to_java(:int), -1, current_op)
        else
          org.apache.pig.newplan.logical.expression.ProjectExpression.new(current_plan, input_index.to_java(:int), @alias, nil, current_op)
        end        
      end      
    end
    
  end
end
