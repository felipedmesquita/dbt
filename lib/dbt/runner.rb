module Dbt
  class Runner
    class << self
      def run(schema = SCHEMA)
        temp_schema = "#{schema}_temp"
        ActiveRecord::Base.connection.execute "CREATE SCHEMA IF NOT EXISTS #{temp_schema}"
        file_paths = Dir.glob("app/sql/**/*.sql")
        models = file_paths.map { |fp| Model.new(fp, temp_schema) }
        dependencies =
          models.map { |m| { m.name => m.refs } }.reduce({}, :merge!)
        check_if_all_refs_have_sql_files dependencies
        graph = Dagwood::DependencyGraph.new dependencies
        md = Mermaid.markdown_for dependencies
        Mermaid.generate_file md
        graph.order.each do |model_name|
          models.find { |m| m.name == model_name }.build
        end
        drop_old_schema_and_rename_temp temp_schema, schema
      end

      def check_if_all_refs_have_sql_files(dependencies)
        dependencies.each do |key, value|
          sem_arquivo = (value || []) - dependencies.keys
          unless sem_arquivo.empty?
            raise "Missing .sql model files for ref #{sem_arquivo} in model #{key}"
          end
        end
      end

      def drop_old_schema_and_rename_temp temp, schema
        ActiveRecord::Base.connection.execute "DROP SCHEMA IF EXISTS #{schema} CASCADE"
        ActiveRecord::Base.connection.execute "ALTER SCHEMA #{temp} RENAME TO #{schema}"
      end
    end
  end
end
