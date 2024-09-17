module Dbt
  class Runner
    class << self
      def run(custom_schema = nil, glob_path = "app/sql/**/*.sql")
        schema = custom_schema || Dbt.settings["schema"] || Dbt::SCHEMA
        ActiveRecord::Base.connection.execute "CREATE SCHEMA IF NOT EXISTS #{schema}"
        file_paths = Dir.glob(glob_path)
        models = file_paths.map { |fp| Model.new(fp, schema) }
        dependencies =
          models.map { |m| { m.name => m.refs } }.reduce({}, :merge!)
        check_if_all_refs_have_sql_files dependencies
        graph = Dagwood::DependencyGraph.new dependencies
        md = Mermaid.markdown_for dependencies
        Mermaid.generate_file md
        metrics_recorder = MetricsRecorder.new(schema)
        graph.order.each do |model_name|
          model = models.find { |m| m.name == model_name }
          start_time = Time.now
          model.build
          end_time = Time.now
          # Record the timing data
          metrics_recorder.record(model.name, model.filepath, start_time, end_time)
        end
        # Save all metrics after the run completes
        metrics_recorder.save
        graph.order
      end

      def test
        puts "Running tests..."
        schema = Dbt.settings["schema"] || Dbt::SCHEMA
        tables = run(schema, "app/sql_test/**/*.sql")
        tables.each do |table|
          puts "TEST #{table}"
          raise "Table #{table} is not empty" unless ActiveRecord::Base.connection.execute("SELECT COUNT(*) FROM #{schema}.#{table}").to_a[0]["count"] == 0
        end
        puts "All tests passed!"
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
