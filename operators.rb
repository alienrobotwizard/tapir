require 'expressions'

import 'org.codehaus.jackson.map.ObjectMapper'

import 'org.apache.pig.FuncSpec'
import 'org.apache.pig.ResourceSchema'
import 'org.apache.pig.impl.io.FileSpec'
import 'org.apache.pig.newplan.logical.Util'
import 'org.apache.pig.parser.QueryParserUtils'
import 'org.apache.pig.parser.LogicalPlanBuilder'
import 'org.apache.pig.impl.logicalLayer.schema.Schema'
import 'org.apache.pig.newplan.logical.relational.LOLoad'
import 'org.apache.pig.newplan.logical.relational.LOStore'
import 'org.apache.pig.newplan.logical.relational.LOFilter'
import 'org.apache.pig.newplan.logical.relational.LOForEach'
import 'org.apache.pig.newplan.logical.relational.LOGenerate'
import 'org.apache.pig.newplan.logical.relational.LOInnerLoad'
import 'org.apache.pig.newplan.logical.relational.LogicalPlan'
import 'org.apache.pig.newplan.logical.relational.LogicalSchema'
import 'org.apache.pig.newplan.logical.expression.ProjectExpression'
import 'org.apache.pig.newplan.logical.expression.LogicalExpressionPlan'
import 'org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil'

module LogicalOperator

  def self.from_hash hsh
    # FIXME: We *might* get both operators and expressions here; delegate accordingly
    OPERATORS[hsh[:operator]].from_hash(hsh)
  end

  def self.spec_for_name name
    FuncSpec.new(name)
  end
  
  def self.func_for_name name
    PigContext.instantiate_func_from_spec(spec_for_name(name))
  end
  
  class Plan
    attr_accessor :properties # Hashmap of configuration properties
    attr_accessor :graph      # Array of logical operators

    attr_accessor :current_plan
    attr_accessor :pig_context

    def initialize pig_context
      @pig_context  = pig_context
      @current_plan = LogicalPlan.new      
    end

    def build hsh
      @properties = hsh[:properties]
      @graph      = hsh[:graph].map{|op| LogicalOperator.from_hash(op) }
    end
    
    def to_hash
      {
        :properties => properties,
        :graph      => graph.map{|op| op.to_hash}
      }
    end

    def to_json
      to_hash.to_json
    end

    def to_pig
      load_index    = 0
      file_name_map = {}
      
      graph.each do |op|

        if op.is_a? Load
          key                = op.set_absolute_path(pig_context, load_index, file_name_map)
          load_index        += 1
          file_name_map[key] = op.uri
        end
        
        op.to_pig(pig_context, current_plan, nil)        
      end
      
      current_plan
    end    
    
  end

  class Load
    attr_accessor :alias     # String alias to assign to
    attr_accessor :uri       # URI to read from
    attr_accessor :schema    # Schema with the same schema as pig's .pig_schema file
    attr_accessor :load_func # Optional string load func class name
    attr_accessor :load_func_args # Array of load func args

    def initialize aliaz, uri, schema, load_func, load_func_args
      @alias          = aliaz
      @uri            = uri
      @schema         = schema
      @load_func      = load_func
      @load_func_args = load_func_args
    end

    def self.from_hash hsh
      aliaz     = hsh[:alias]
      uri       = hsh[:uri]
      schema    = hsh[:schema]
      
      load_func      = (hsh[:load_func] || "PigStorage")
      load_func_args = (hsh[:load_func_args] || [])
      Load.new(aliaz, uri, schema, load_func, load_func_args)
    end

    def to_hash
      {
        :operator       => 'load',
        :alias          => @alias,
        :uri            => uri,
        :schema         => schema,
        :load_func      => load_func,
        :load_func_args => load_func_args
      }
    end

    def to_json
      to_hash.to_json
    end

    def to_pig pig_context, current_plan, current_op
      logical_schema = schema_from_hash(schema)
      func           = LogicalOperator.func_for_name(load_func)
      file_spec      = FileSpec.new(uri, LogicalOperator.spec_for_name(load_func))
      conf           = ConfigurationUtil.to_configuration(pig_context.get_properties())
      
      load = LOLoad.new(file_spec, logical_schema, current_plan, conf, func, @alias + "_" + LogicalPlanBuilder.new_operator_key(''))
      load.get_schema
      load.set_tmp_load(false)
      return load
    end    

    def set_absolute_path pig_context, load_index, file_name_map
      spec = LogicalOperator.spec_for_name(load_func)
      func = LogicalOperator.func_for_name(load_func)
      key  = QueryParserUtils.construct_file_name_signature(uri, spec) + "_" + load_index.to_s

      path = file_name_map[key]
      
      if !path
        path = func.relative_to_absolute_path(uri, QueryParserUtils.get_current_dir(pig_context))
        if path
          QueryParserUtils.set_hdfs_servers(path, pig_context) # wtf?
        end
        @uri = path
      end
      
      key
    end
      
    # FIXME: Create ResourceSchema and translate manually, or just build
    # LogicalSchema directly
    def schema_from_hash schema
      json = schema.to_json.to_java(:string) # !
      
      rs = (ObjectMapper.new()).readValue(json, ResourceSchema.java_class);
      Util.translateSchema(Schema.getPigSchema(rs))
    end
    
  end
  
  class Filter
    attr_accessor :alias     # Name of the output relation
    attr_accessor :input     # Array of input relation (or inner bag) names
    attr_accessor :condition # A LogicalExpression::Plan

    def initialize aliaz, input, condition
      @alias     = aliaz
      @input     = input
      @condition = condition      
    end

    def self.from_hash hsh
      aliaz     = hsh[:alias]
      input     = hsh[:input]
      condition = LogicalExpression.from_hash(hsh[:condition])      
    end

    def to_hash
      {
        :operator  => 'filter',
        :alias     => @alias,
        :input     => input,
        :condition => condition.to_hash 
      }
    end

    def to_json
      to_hash.to_json
    end

    def to_pig pig_context, current_plan, current_op
      filter = LOFilter.new(current_plan)
      filter.set_filter_plan(LogicalExpression::Plan.new(pig_context, op).to_pig(condition))      
      return filter      
    end    
    
  end
  
  class ForEach
    attr_accessor :alias # Name of the output relation
    attr_accessor :input # Array of input relation (or inner bag) names
    attr_accessor :graph # Array of operators

    def initialize aliaz, input, graph
      @alias = aliaz
      @input = input
      @graph = graph      
    end

    def self.from_hash hsh
      aliaz = hsh[:alias]
      input = hsh[:input]
      graph = hsh[:graph].map{|op| LogicalOperator.from_hash(op) }
      ForEach.new(aliaz, input, graph)
    end

    def to_hash
      {
        :operator => 'foreach',
        :alias    => @alias,
        :input    => input,
        :graph    => graph.map{|op| op.to_hash}
      }      
    end

    def to_json
      to_hash.to_json
    end

    def to_pig pig_context, current_plan, current_op
      foreach    = LOForEach.new(current_plan)
      inner_plan = LogicalPlan.new
      graph.each do |op|
        op.to_pig(pig_context, inner_plan, foreach)
      end
      foreach.set_inner_plan(inner_plan)
      foreach
    end
    
  end
  
  class Generate
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
    
    def to_pig pig_context, current_plan, current_op
      gen = LOGenerate.new(current_plan)

      plans = results.map do |result|
        LogicalExpression::Plan.new(pig_context, gen).to_pig(result)
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
              inputs.add(projected)
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

  OPERATORS = {
    'foreach'  => LogicalOperator::ForEach,
    'generate' => LogicalOperator::Generate
  }
  
end
