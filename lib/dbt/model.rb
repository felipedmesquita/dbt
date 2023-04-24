module Dbt
  class Model
    attr_reader :name,
                :code,
                :materialize_as,
                :sources,
                :refs,
                :built,
                :filepath,
                :skip,
                :fingerprint

    def initialize(filepath, schema = SCHEMA)
      @filepath = filepath
      @name = File.basename(filepath, ".sql")
      @original_code = File.read(filepath)
      @sources = []
      @refs = []
      @built = false
      @materialize_as = ""
      @schema = schema

      def source(table)
        @sources << table.to_s
        table.to_s
      end

      def ref(model)
        @refs << model.to_s
        "#{@schema}.#{model}"
      end

      def materialize
        @materialize_as = "MATERIALIZED"
        ""
      end

      def skip
        @skip = true
        ""
      end

      @code = ERB.new(@original_code).result(binding)
      @fingerprint = PgQuery.fingerprint(@code)
    end

    def build
      if @skip
        puts "SKIPPING #{@name}"
      else
        puts "BUILDING #{@name}"
        ActiveRecord::Base.connection.execute <<~SQL
          #{drop_relation @schema, @name}
          CREATE #{materialize_as} VIEW #{@schema}.#{@name} AS (
            #{@code}
          );
        SQL
        @built = true
      end
    end

    def drop_relation(schema, relation)
      type =
        ActiveRecord::Base
          .connection
          .execute(
            "
      SELECT
        CASE c.relkind
          WHEN 'r' THEN 'TABLE'
          WHEN 'v' THEN 'VIEW'
          WHEN 'm' THEN 'MATERIALIZED VIEW'
        END AS relation_type
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relname = '#{relation}' AND n.nspname = '#{schema}';"
          )
          .values
          .first
          &.first
      "DROP #{type} #{schema}.#{relation} CASCADE;" unless type.nil?
    end
  end
end
