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

  def self.build_nested op, aliaz, current_plan, inputs
    set_alias(op, aliaz)
    current_plan.add(op)
    inputs.each do |input|
      current_plan.connect(input, op)
    end        
  end
  
  class Plan
    attr_accessor :properties # Hashmap of configuration properties
    attr_accessor :graph      # Array of logical operators

    attr_accessor :current_plan
    attr_accessor :pig_context

    # internal
    attr_accessor :operators
    
    class OperatorMap
      
      def initialize
        @data = {}
      end
      
      def get k
        @data[k]
      end
      
      def put k, v
        @last_rel = k
        @data[k] = v
      end    
    end
  
    def initialize pig_context
      @pig_context  = pig_context
      @current_plan = LogicalPlan.new
      @operators    = OperatorMap.new
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
      store_index   = 0
      file_name_map = {}
      
      graph.each do |op|
        case op
        when Load then
          key                = op.set_absolute_path(pig_context, load_index, file_name_map)
          load_index        += 1
          file_name_map[key] = op.uri
          
          pig_op = op.to_pig(pig_context, current_plan, nil)
          build_op(pig_op, op.alias, [], nil)
        when Store then
          key                = op.set_absolute_path(pig_context, store_index, file_name_map)
          store_index       += 1
          file_name_map[key] = op.uri

          pig_op = op.to_pig(pig_context, current_plan, nil)
          build_op(pig_op, nil, op.input, nil)
        else
          pig_op = op.to_pig(pig_context, current_plan, nil)
          build_op(pig_op, op.alias, op.input, nil)
        end
      end
      
      current_plan
    end    
    
    def build_op op, aliaz, input_aliazes, partitioner
      LogicalOperator.set_alias(op, aliaz)
      LogicalOperator.set_partitioner(op, partitioner)
      op.set_location(org.apache.pig.parser.SourceLocation.new('',0,0)) # increment a counter or some such
      
      current_plan.add(op)
      
      input_aliazes.each do |a|
        pred = operators.get(a)
        current_plan.connect(pred, op)      
      end

      @operators.put(op.get_alias, op)
      pig_context.set_last_alias(op.get_alias)
      return op.get_alias
    end
    
  end


  class Operator
    attr_accessor :in_nest_plan, :in_foreach_plan, :input_ops
  end
  
  class Load < Operator
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
      func           = LogicalOperator.func_for_name(load_func, load_func_args)
      file_spec      = FileSpec.new(uri, LogicalOperator.spec_for_name(load_func, load_func_args))
      conf           = ConfigurationUtil.to_configuration(pig_context.get_properties())

      load = LOLoad.new(file_spec, logical_schema, current_plan, conf, func, @alias + "_" + LogicalPlanBuilder.new_operator_key(''))
      load.get_schema
      load.set_tmp_load(false)
      return load
    end    

    def set_absolute_path pig_context, load_index, file_name_map
      spec = LogicalOperator.spec_for_name(load_func, load_func_args)
      func = LogicalOperator.func_for_name(load_func, load_func_args)
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

  class Store < Operator
    attr_accessor :input # Array of input relation names (limit 1)
    attr_accessor :uri   # Location to store to
    attr_accessor :store_func      # Optional StoreFunc class name
    attr_accessor :store_func_args # Array of StoreFunc arguments
    
    def initialize input, uri, store_func, store_func_args
      @input = input
      @uri   = uri
      @store_func      = store_func
      @store_func_args = store_func_args
    end

    def self.from_hash hsh
      input = hsh[:input]
      uri   = hsh[:uri]
      store_func      = (hsh[:store_func] || "PigStorage")
      store_func_args = (hsh[:store_func_args] || [])
      Store.new(input, uri, store_func, store_func_args)
    end

    def to_hash
      {
        :operator        => 'store',
        :input           => input,
        :uri             => uri,
        :store_func      => store_func,
        :store_func_args => store_func_args
      }
    end

    def to_json
      to_hash.to_json
    end    

    def to_pig pig_context, current_plan, current_op
      func       = LogicalOperator.func_for_name(store_func, store_func_args)
      signature  = input.first + LogicalPlanBuilder.new_operator_key('')
      func.set_store_func_udf_context_signature(signature)

      file_spec = FileSpec.new(uri, LogicalOperator.spec_for_name(store_func, store_func_args))
      store     = LOStore.new(current_plan, file_spec, func, signature)
      return store
    end

    def set_absolute_path pig_context, store_index, file_name_map
      func = LogicalOperator.func_for_name(store_func, store_func_args)
      key  = input.first + store_index.to_s
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
    
  end
  
  class Filter < Operator
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
      Filter.new(aliaz, input, condition)
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

    def to_pig pig_context, current_plan, current_op, nest_context = {}
      filter      = LOFilter.new(current_plan)
      filter_plan = LogicalExpression::Plan.new(pig_context, filter).to_pig(condition, in_foreach_plan, nest_context)
      filter.set_filter_plan(filter_plan)

      if in_nest_plan
        LogicalOperator.build_nested(filter, @alias, current_plan, input_ops)
      end
      
      return filter      
    end    
    
  end
  
  class ForEach < Operator
    attr_accessor :alias # Name of the output relation
    attr_accessor :input # Array of input relation (or inner bag) names
    attr_accessor :graph # Array of operators

    # Internal
    attr_accessor :operators, :expression_plans
    
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

    def to_pig pig_context, current_plan, current_op, nest_context = {}
      foreach    = LOForEach.new(current_plan)
      inner_plan = LogicalPlan.new

      expression_plans = {}
      operators        = {}

      graph.each do |op|        
        op.in_nest_plan    = true
        op.in_foreach_plan = true
        
        if !op.is_a? Generate
          op.input_ops = nested_op_inputs(op.input, foreach, inner_plan, operators)
        end        
        
        pig_op = op.to_pig(pig_context, inner_plan, foreach, {:operators => operators, :expression_plans => expression_plans})
        
        if op.is_a? LogicalExpression::AssignmentExpression
          expression_plans[op.alias] = pig_op
        elsif !op.is_a? Generate
          operators[op.alias]        = pig_op
        end
                
      end
      foreach.set_inner_plan(inner_plan)
      return foreach
    end

    # inputs - list of input aliases    
    def nested_op_inputs inputs, foreach, inner_plan, operators
      ret = []
      inputs.each do |input|
        op = operators[input]
        if !op
          op = LOInnerLoad.new(inner_plan, foreach, input)
          inner_plan.add(op)
        end
        ret << op
      end
      ret
    end
    
  end
  
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
    'generate' => LogicalOperator::Generate,
    'load'     => LogicalOperator::Load,
    'store'    => LogicalOperator::Store,
    'filter'   => LogicalOperator::Filter
  }
  
end
