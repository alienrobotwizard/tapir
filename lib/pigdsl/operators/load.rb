import 'org.apache.pig.ResourceSchema'
import 'org.apache.pig.impl.io.FileSpec'
import 'org.apache.pig.parser.QueryParserUtils'
import 'org.apache.pig.parser.LogicalPlanBuilder'
import 'org.apache.pig.impl.logicalLayer.schema.Schema'
import 'org.apache.pig.newplan.logical.Util'
import 'org.apache.pig.newplan.logical.relational.LOLoad'
import 'org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil' # What the fuck is this doing here?

import 'org.codehaus.jackson.map.ObjectMapper'

module LogicalOperator
  
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
  
end
