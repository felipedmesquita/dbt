module Dbt
  class Runner
    class << self
      def run(custom_schema=nil)
        schema = custom_schema || Dbt::settings['schema'] || Dbt::SCHEMA
        ActiveRecord::Base.connection.execute "CREATE SCHEMA IF NOT EXISTS #{schema}"
        duckdb = DuckDB::Database.open "dbt.db"
        con = duckdb.connect
        config = ActiveRecord::Base.connection_db_config.configuration_hash

        params = {
          dbname: config[:database],
          user: config[:username],
          password: config[:password],
          host: config[:host],
          port: config[:port]
        }
        params.compact!
        connection_string = params.map { |key, value| "#{key}=#{value}" }.join(' ')

        puts con.query "ATTACH '#{connection_string}' AS postgres (type postgres)"

        file_paths = Dir.glob("app/sql/**/*.sql")
        models = file_paths.map { |fp| Model.new(fp, schema) }
        dependencies =
          models.map { |m| { m.name => m.refs } }.reduce({}, :merge!)
        check_if_all_refs_have_sql_files dependencies
        graph = Dagwood::DependencyGraph.new dependencies
        md = Mermaid.markdown_for dependencies
        Mermaid.generate_file md
        graph.order.each do |model_name|
          models.find { |m| m.name == model_name }.build con
        end
      end

      def check_if_all_refs_have_sql_files(dependencies)
        dependencies.each do |key, value|
          sem_arquivo = (value || []) - dependencies.keys
          unless sem_arquivo.empty?
            raise "Missing .sql model files for ref #{sem_arquivo} in model #{key}"
          end
        end
      end
    end
  end
end
