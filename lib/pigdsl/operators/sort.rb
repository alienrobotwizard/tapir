import 'org.apache.pig.newplan.logical.relational.LOSort'

module LogicalOperator

  class Sort < Operator
    attr_accessor :alias   # Name of output relation
    attr_accessor :input   # Array of input relation (or inner bag) names

    # Array of string sort order (corresponding to columns), 'asc' or 'desc'
    # A null value means asc for all columns
    attr_accessor :orders
    
    # Array of col_ref LogicalExpressions to sort by, null to sort by -all- columns
    attr_accessor :columns 
    
    attr_accessor :compare_func # Optional Pig ComparisonFunc

    def initialize aliaz, input, orders, columns, compare_func
      @alias        = aliaz
      @input        = input
      @orders       = orders
      @columns      = columns
      @compare_func = compare_func
    end

    def self.from_hash hsh
      aliaz   = hsh[:alias]
      input   = hsh[:input]
      orders  = (hsh[:orders]  || ['asc'])
      columns = (hsh[:columns] || [{:type => 'col_ref', :alias => nil}]).map{|col| LogicalExpression.from_hash(col) }
      
      compare_func = hsh[:compare_func] # ok if this is nil?
      Sort.new(aliaz, input, orders, columns, compare_func)      
    end

    def to_hash
      {
        :operator     => 'sort',
        :alias        => @alias,
        :input        => input,
        :orders       => orders,
        :columns      => columns.map{|col| col.to_hash },
        :compare_func => compare_func
      }
    end

    def to_json
      to_hash.to_json
    end

    def to_pig pig_context, current_plan, current_op, nest_context = {}
      sort  = LOSort.new(current_plan)

      flags = java.util.ArrayList.new
      orders.each do |order|
        flags.add(order.eql?('asc') ? true : false)
      end
      
      
      plans = columns.map do |col|
        LogicalExpression::Plan.new(pig_context, sort).to_pig(col, in_foreach_plan, nest_context)
      end      
      cf    = (compare_func ? LogicalOperator.spec_for_name(compare_func) : nil)
      
      sort.set_sort_col_plans(plans)
      sort.set_user_func(cf)
      sort.set_ascending_cols(flags)

      if in_nest_plan
        LogicalOperator.build_nested(sort, @alias, current_plan, input_ops)
      end

      # what about the visitors?
      return sort
    end    
    
  end
  
end
