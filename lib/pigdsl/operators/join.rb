import 'org.apache.pig.impl.util.MultiMap'
import 'org.apache.pig.newplan.logical.relational.LOJoin'

module LogicalOperator

  class Join < Operator
    attr_accessor :alias     # Name of the output relation
    attr_accessor :input     # Array of input relation names
    attr_accessor :by        # Map of :relation_name => [col_ref]

    # One of (left, right, full, inner). default => 'inner'
    attr_accessor :join_type

    # Optional join strategy, one of
    # (replicated, skewed, hash, merge, merge-sparse) default => 'hash'
    attr_accessor :strategy

    def initialize aliaz, input, by, join_type, strategy
      @alias     = aliaz
      @input     = input
      @by        = by
      @join_type = join_type
      @strategy  = strategy
    end

    #
    # Inner flags?
    #
    def self.from_hash hsh
      aliaz     = hsh[:alias]
      input     = hsh[:input]
      by        = hsh[:by].inject({}){|hsh, kv| hsh[kv.first] = kv.last.map{|x| LogicalOperator.from_hash(x) }; hsh}
      join_type = (hsh[:join_type] || 'inner')
      strategy  = (hsh[:strategy] || 'hash')
      Join.new(aliaz, input, by, join_type, strategy)
    end

    def to_hash
      {
        :operator  => 'join',
        :alias     => @alias,
        :input     => input,
        :by        => by.inject({}){|hsh, kv| hsh[kv.first] = kv.last.map{|x| x.to_hash}; hsh},
        :join_type => join_type,
        :strategy  => strategy
      }
    end

    def to_json
      to_hash.to_json
    end

    def to_pig pig_context, current_plan, current_op, nest_context = {}
      raise "Nested join not allowed" if in_nest_plan

      join        = LOJoin.new(current_plan)
      join_plans  = MultiMap.new()

      input.each_with_index do |aliaz, idx|
        plans = by[aliaz.to_sym].map{|x| LogicalExpression::Plan.new(pig_context, join).to_pig(x, false, {}) }
        join_plans.put(idx.to_java(:int), plans)
      end

      inner_flags = []
      case join_type
      when 'inner'
        inner_flags = [true, true]
      when 'full'
        inner_flags = [false, false]
      when 'left'
        inner_flags = [true, false]
      when 'right'
        inner_flags = [false, true]
      end

      join.pin_option(LOJoin::OPTION_JOIN) if !strategy.eql? 'hash'

      if strategy.eql? 'skewed'
        raise "Custom Partitioner is not supported for skewed join" if partitioner
        raise "Skewed join can only be applied for 2-way joins"     if input.size != 2
      elsif strategy.eql? 'merge' or strategy.eql? 'merge-sparse'
        raise "Merge join can only be applied for 2-way joins" if input.size != 2
      elsif strategy.eql? 'replicated'
        raise "Replicated join does not support (right|full) outer joins" if (inner_flags.size == 2 && !inner_flags.first)
      end

      join.set_join_type(join_strategy(strategy))
      join.set_inner_flags(inner_flags.to_java(:boolean))
      join.set_join_plans(join_plans)
      return join
    end

    def join_strategy strategy
      case strategy
      when 'replicated'
        LOJoin::JOINTYPE::REPLICATED
      when 'skewed'
        LOJoin::JOINTYPE::SKEWED
      when 'hash'
        LOJoin::JOINTYPE::HASH
      when 'merge'
        LOJoin::JOINTYPE::MERGE
      when 'merge-sparse'
        LOJoin::JOINTYPE::MERGESPARSE
      end    
    end  
  end
  
end
