module Dbt
  class Runner
      class << self
        def run
          ActiveRecord::Base.connection.execute "CREATE SCHEMA IF NOT EXISTS #{SCHEMA}"
          file_paths = Dir.glob("app/sql/**/*.sql")
          models = file_paths.map {|fp| Model.new fp }
          dependencies = models.map {|m| {m.name => m.refs}}.reduce({}, :merge!)
          check_if_all_refs_have_sql_files dependencies
          graph = Dagwood::DependencyGraph.new dependencies
          graph.order.each do |model_name|
            models.find {|m| m.name == model_name}.build
          end
        end

        def check_if_all_refs_have_sql_files dependencies
          dependencies.each do |key, value|
            sem_arquivo = (value || []) - dependencies.keys
            raise "Missing .sql model files for ref #{sem_arquivo} in model #{key}" unless sem_arquivo.empty?
          end
        end


      end



  end
end
