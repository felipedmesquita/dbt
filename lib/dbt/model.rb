class Dbt::Model

  attr_reader :name, :code, :materialize_as, :sources, :refs, :built, :filepath
  def initialize filepath
    @filepath = filepath
    @name = File.basename(filepath, '.sql')
    @original_code = File.read(filepath)
    @sources = []
    @refs = []
    @built = false
    @materialize_as = ""

    def source(table)
      @sources << table
      table.to_s
    end

    def ref(model)
      @refs << model
      "felipe_dbt.#{model}"
    end

    def materialize
      @materialize_as = "MATERIALIZED"
      ""
    end

    @code = ERB.new(@original_code).result(binding)
  end

  def build
    puts "BUILDING #{@name}"
    ActiveRecord::Base.connection.execute <<~SQL
      #{drop_relation SCHEMA, @name}
      CREATE #{materialize_as} VIEW #{SCHEMA}.#{@name} AS (
        #{@code}
      );
    SQL
    @built = true
  end

  def drop_relation schema, relation
    type = ActiveRecord::Base.execute("
    SELECT
      CASE c.relkind
        WHEN 'r' THEN 'TABLE'
        WHEN 'v' THEN 'VIEW'
        WHEN 'm' THEN 'MATERIALIZED VIEW'
      END AS relation_type
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = '#{relation}' AND n.nspname = '#{schema}';"
    ).values.first.first
    unless type.nil?
      "DROP #{type} #{schema}.#{relation} CASCADE"
    end
  end

end
