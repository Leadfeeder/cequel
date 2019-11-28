# -*- encoding : utf-8 -*-
module Cequel
  module Metal
    #
    # Internal representation of a data manipulation statement
    #
    # @abstract Subclasses must implement #write_to_statement, which writes
    #   internal state to a Statement instance. Subclasses may implement
    #   #if_statement which adds final clauses to a Statement
    #   instance state.
    #
    # @since 1.0.0
    # @api private
    #
    class Writer
      extend Util::Forwardable

      attr_accessor :type_hints

      #
      # @param data_set [DataSet] data set to write to
      #
      def initialize(data_set, &block)
        @data_set, @options, @block = data_set, options, block
        @statements, @bind_vars = [], []
        SimpleDelegator.new(self).instance_eval(&block) if block
      end

      #
      # Execute the statement as a write operation
      #
      # @param options [Options] options
      # @option options [Symbol] :consistency what consistency level to use for
      #   the operation
      # @option options [Integer] :ttl time-to-live in seconds for the written
      #   data
      # @option options [Time,Integer] :timestamp the timestamp associated with
      #   the column values
      # @option options [Boolean] :if defines `IF <condition>` clause to be
      #   to the operations statement, if supported.
      # @return [void]
      #
      def execute(options = {})
        options.assert_valid_keys(:timestamp, :ttl, :consistency, :if)
        return if empty?
        statement = Statement.new
        consistency = options.fetch(:consistency, data_set.query_consistency)
        write_to_statement(statement, options)
        statement.append(*data_set.row_specifications_cql)
        if_statement(statement, options)
        data_set.write_with_options(statement,
                                    consistency: consistency)
      end

      private

      attr_reader :data_set, :options, :statements, :bind_vars
      def_delegator :data_set, :table_name
      def_delegator :statements, :empty?

      def prepare_upsert_value(value)
        yield '?', value
      end

      #
      # Generate CQL option statement for inserts and updates
      #
      def generate_upsert_options(options)
        upsert_options = options.slice(:timestamp, :ttl)
        if upsert_options.empty?
          ''
        else
          ' USING ' <<
          upsert_options.map do |key, value|
            serialized_value =
              case key
              when :timestamp then (value.to_f * 1_000_000).to_i
              else value
              end
            "#{key.to_s.upcase} #{serialized_value}"
          end.join(' AND ')
        end
      end

      def if_statement(statement, options)
        return unless options.key?(:if)
        statement.append(generate_if_options(options[:if]))
      end

      def generate_if_options(if_options)
        serialized_if_options =
          case if_options
          when :exists
            'EXISTS'
          when :not_exists
            'NOT EXISTS'
          when Hash
            if_options.map { |key, value| "#{key} = #{value}" }.join(' AND ')
          when String
            if_options
          else
            # TODO raise exception
          end

        ' IF ' + serialized_if_options
      end
    end
  end
end
