import 'org.apache.pig.impl.util.MultiMap'
import 'org.apache.pig.newplan.logical.relational.LOCogroup'

module LogicalOperator

  class Group < Operator
    attr_accessor :alias # Name of the output relation
    attr_accessor :input # Array of input relation names
    attr_accessor :by    # Map of :relation_name => [col_ref] or [const]

    # (Optional) map of :relation_name => true or false; How to handle cogroup
    # key misses (case where one relation has a key and the other doesn't).
    # Only sensible during a group with more than one input;
    # indicates that the relation is to be 'inner' grouped (false is default)
    attr_accessor :inner

    # (Optional) What grouping strategy to employ
    # One of (regular, collected, merge). regular is default
    attr_accessor :strategy

    def initialize aliaz, input, by, inner, strategy
      @alias    = aliaz
      @input    = input
      @by       = by
      @inner    = inner
      @strategy = strategy
    end

    def self.from_hash hsh
      aliaz    = hsh[:alias]
      input    = hsh[:input]
      by       = hsh[:by].inject({}){|hsh, kv| hsh[kv.first] = kv.last.map{|x| LogicalOperator.from_hash(x) }; hsh}
      inner    = (hsh[:inner] || input.inject({}){|hsh, aliaz| hsh[aliaz] = false; hsh})
      strategy = (hsh[:strategy] || 'regular')
      Group.new(aliaz, input, by, inner, strategy)
    end

    def to_hash
      {
        :operator => 'group',
        :aliaz    => @alias,
        :input    => input,
        :by       => by.inject({}){|hsh, kv| hsh[kv.first] = kv.last.map{|x| x.to_hash}; hsh},
        :inner    => inner,
        :strategy => strategy
      }
    end

    def to_json
      to_hash.to_json
    end

    def to_pig pig_context, current_plan, current_op, nest_context = {}
      raise "Nested group not allowed" if in_nest_plan

      group       = LOCogroup.new(current_plan)
      group_plans = MultiMap.new()

      inner_flags = []
      input.each_with_index do |aliaz, idx|
        sym_alias = aliaz.to_sym

        input_index = 0
        plans = by[sym_alias].map do |x|
          x.input_index = input_index
          input_index  += 1
          LogicalExpression::Plan.new(pig_context, group).to_pig(x, false, {})
        end

        group_plans.put(idx.to_java(:int), plans)

        inner_flags << inner[sym_alias]
      end

      group.pin_option(LOCogroup::OPTION_GROUPTYPE) if !strategy.eql? 'regular'

      if strategy.eql? 'collected'
        raise "Collected group is only supported for single input" if input.size > 1
      end

      group.set_expression_plans(group_plans)
      group.set_group_type(group_strategy(strategy))
      group.set_inner_flags(inner_flags.to_java(:boolean))
      return group
    end

    def group_strategy strategy
      case strategy
      when 'regular'
        LOCogroup::GROUPTYPE::REGULAR
      when 'collected'
        LOCogroup::GROUPTYPE::COLLECTED
      when 'merge'
        LOCogroup::GROUPTYPE::MERGE
      end
    end
  end
   
end
