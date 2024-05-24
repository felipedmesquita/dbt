module Dbt
  class Model
    include SqlTemplateHelpers

    attr_reader :name,
                :code,
                :materialize_as,
                :sources,
                :refs,
                :built,
                :filepath,
                :skip,
                :is_incremental,
                :unique_by_column

    def initialize(filepath, schema = SCHEMA)
      @filepath = filepath
      @name = File.basename(filepath, ".sql")
      @original_code = File.read(filepath)
      @sources = []
      @refs = []
      @built = false
      @materialize_as = "TABLE"
      @schema = schema

      def source(table)
        @sources << table.to_s
        "postgres.#{table}"
      end

      def ref(model)
        @refs << model.to_s
        # "#{@schema}.#{model}"
        model.to_s
      end

      def this
        if @final
          "postgres.#{@schema}.#{@name}"
        else
          @name.to_s
        end
      end

      def final
        @final = true
      end

      def build_as kind, unique_by: "unique_by"
        case kind.to_s.downcase
          when "view"
            @materialize_as = "VIEW"
          when "table"
            @materialize_as = "TABLE"
          when "incremental"
            if get_relation_type(this) != "TABLE"
              @materialize_as = "TABLE"
              @is_incremental = false
            else
              @is_incremental = true
              @unique_by_column = unique_by
            end
        else
          raise "Invalid build_as materialization: #{kind}"
        end
        ""
      end

      def materialize
        # legacy, use build_as :table
        # will add warning in the future
        build_as :table
        ""
      end

      def skip
        @skip = true
        ""
      end

      @code = ERB.new(@original_code).result(binding)
    end

    def build duckdb_connection=nil
      puts "duck"
      if @skip
        puts "SKIPPING #{@name}"
      elsif @is_incremental
        puts "INCREMENTAL #{@name}"
        assert_column_uniqueness(unique_by_column, this)
        temp_table = "#{@name}_incremental_build_temp_table"

        # drop the temp table if it exists
        duckdb_connection.query <<~SQL
        DROP TABLE IF EXISTS #{temp_table};
        SQL

        # create a temp table with the same schema as the source
        duckdb_connection.query <<~SQL
          CREATE TABLE #{temp_table} AS (
            #{code}
          );
        SQL
        assert_column_uniqueness(unique_by_column, temp_table)
        # delete rows from the table that are in the source
        duckdb_connection.query <<~SQL
          DELETE FROM #{this}
          USING #{temp_table}
          WHERE #{this}.#{unique_by_column} = #{temp_table}.#{unique_by_column};
        SQL

        # insert rows from the source into the table
        duckdb_connection.query <<~SQL
          INSERT INTO #{this}
          SELECT * FROM #{temp_table};
        SQL

        # drop the temp table
        duckdb_connection.query <<~SQL
          DROP TABLE #{temp_table};
        SQL

      else
        puts "BUILDING #{@name}"
        curent_relation_type = get_relation_type(this)
        case @materialize_as
          when "VIEW"
            duckdb_connection.query <<~SQL
              BEGIN;
              #{drop_relation(this)}
              CREATE VIEW #{this} AS (
                #{@code}
              );
              COMMIT;
            SQL
          when "TABLE"
            duckdb_connection.query <<~SQL
              DROP TABLE IF EXISTS #{this};
              DROP VIEW IF EXISTS #{this};
              CREATE TABLE #{this} AS (
                #{@code}
              );
            SQL
          else
            raise "Invalid materialize_as: #{@materialize_as}"
        end

        @built = true
      end
    end

    def drop_relation(relation)
      # type = get_relation_type(relation)
      # if type.present?
        "DROP TABLE IF EXISTS #{relation};"
      # else
        # ""
    end

    def get_relation_type(relation)
          "TABLE"
    end

    def assert_column_uniqueness(column, relation)
      result = duckdb_connection.query <<~SQL
        SELECT COUNT(*) = COUNT(DISTINCT #{column}) FROM #{relation};
      SQL
      if result.values.first.first == false
        raise "Column #{column} is not unique in #{relation}"
      end
      true
    end
  end
end
