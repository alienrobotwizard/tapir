import 'org.apache.pig.newplan.logical.relational.LogicalPlan'

module LogicalOperator

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
      @graph      = hsh[:graph].map do |op|
        lo = LogicalOperator.from_hash(op)
        attach_hints(lo, op)        
      end      
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
          build_op(pig_op, op.alias, [], op.parallel_hint, op.partitioner)
        when Store then
          key                = op.set_absolute_path(pig_context, store_index, file_name_map)
          store_index       += 1
          file_name_map[key] = op.uri

          pig_op = op.to_pig(pig_context, current_plan, nil)
          build_op(pig_op, nil, op.input, op.parallel_hint, op.partitioner)
        when Split then
          pig_op = op.to_pig(pig_context, current_plan, nil)
          aliaz  = build_op(pig_op, nil, op.input, op.parallel_hint, op.partitioner)
          
          # splits
          op.get_splits.each do |output|
            build_op(output[:pig_op], output[:alias], [aliaz], op.parallel_hint, op.partitioner)
          end
          
        else
          pig_op = op.to_pig(pig_context, current_plan, nil)
          build_op(pig_op, op.alias, op.input, op.parallel_hint, op.partitioner)
        end
      end
      
      current_plan
    end    

    #
    # Attach parallelism hints and custom partitioner, if any
    #
    def attach_hints lo, hsh
      lo.partitioner   = hsh[:partitioner]
      lo.parallel_hint = hsh[:parallel_hint]
      lo
    end    
    
    def build_op op, aliaz, input_aliazes, parallel_hint, partitioner
      LogicalOperator.set_alias(op, aliaz)
      LogicalOperator.set_partitioner(op, partitioner)
      LogicalOperator.set_parallelism_hint(op, parallel_hint)
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
  
end
