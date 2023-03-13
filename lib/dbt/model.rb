class Dbt::Model

  attr_reader :name, :code, :sources, :refs, :built, :filepath
  def initialize filepath
    @filepath = filepath
    @name = File.basename(filepath, '.sql')
    @original_code = File.read(filepath)
    @sources = []
    @refs = []
    @built = false

    def source(table)
      @sources << table
      table.to_s
    end

    def ref(model)
      @refs << model
      "felipe_dbt.#{model}"
    end

    @code = ERB.new(@original_code).result(binding)
  end

  def build
    puts "BUILDING #{@name}"
    ActiveRecord::Base.connection.execute <<~SQL
      DROP VIEW IF EXISTS felipe_dbt.#{@name} CASCADE;
      CREATE VIEW felipe_dbt.#{@name} AS (
        #{@code}
      );
    SQL
    @built = true
  end

end
