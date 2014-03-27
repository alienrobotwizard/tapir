import 'org.apache.pig.newplan.logical.relational.LORank'

module LogicalOperator

  class Rank < Operator
    attr_accessor :alias   # Name of output relation
    attr_accessor :input   # Array of input relation names

    # Array of string sort order (corresponding to columns), 'asc' or 'desc'
    # A null value means asc for all columns
    attr_accessor :orders
    
    # Array of col_ref LogicalExpressions to sort by, null to sort by -all- columns
    attr_accessor :columns

    # Optional boolean, if dense is true then ties do not create gaps in rank
    attr_accessor :dense
    
    def initialize aliaz, input, orders, columns, dense
      @alias   = aliaz
      @input   = input
      @orders  = orders
      @columns = columns
      @dense   = dense
    end

    def self.from_hash hsh
      aliaz   = hsh[:alias]
      input   = hsh[:input]
      orders  = (hsh[:orders]  || ['asc'])
      columns = (hsh[:columns] || [{:type => 'col_ref', :alias => nil}]).map{|col| LogicalExpression.from_hash(col) }
      dense   = (hsh[:dense] || false)
      Rank.new(aliaz, input, orders, columns, dense)
    end

    def to_hash
      {
        :operator => 'rank',
        :alias    => @alias,
        :input    => input,
        :orders   => orders,
        :columns  => columns.map{|col| col.to_hash },
        :dense    => dense
      }
    end

    def to_json
      to_hash.to_json
    end

    def to_pig pig_context, current_plan, current_op, nest_context = {}
      raise "Nested rank not allowed" if in_nest_plan      
      rank = LORank.new(current_plan)

      flags = java.util.ArrayList.new
      orders.each do |order|
        flags.add(order.eql?('asc') ? true : false)
      end

      input_index = 0
      has_null_alias = false
      plans = columns.map do |col|
        col.input_index = input_index
        input_index += 1
        has_null_alias = true unless col.alias
        LogicalExpression::Plan.new(pig_context, rank).to_pig(col, in_foreach_plan, nest_context)
      end

      rank.set_is_row_number(true) if (plans.size == 1 && has_null_alias)
      rank.set_is_dense_rank(dense)

      rank.set_rank_col_plan(plans)
      rank.set_ascending_col(flags)
      
      return rank
    end
    
  end
  
end
