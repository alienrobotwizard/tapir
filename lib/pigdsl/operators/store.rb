import 'org.apache.pig.newplan.logical.relational.LOStore'

module LogicalOperator
  
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
  
end
