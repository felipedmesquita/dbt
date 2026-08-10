require "set"

module Rdt
  class Runner
    class << self
      def run(custom_schema = nil, glob_path = "app/sql/**/*.sql", include: nil, exclude: nil)
        schema = custom_schema || Rdt.settings["schema"] || Rdt::SCHEMA
        ActiveRecord::Base.connection.execute "CREATE SCHEMA IF NOT EXISTS #{schema}"
        all_file_paths = Dir.glob(glob_path)
        file_paths = filter_file_paths(all_file_paths, include: include, exclude: exclude)

        models = file_paths.map { |fp| Model.new(fp, schema) }
        model_names = models.map(&:name)
        dependencies = models.to_h do |m|
          [m.name, m.refs.select { |ref| model_names.include?(ref) }]
        end

        check_if_all_refs_have_sql_files(dependencies)
        graph = Dagwood::DependencyGraph.new dependencies
        md = Mermaid.markdown_for dependencies
        Mermaid.generate_file md
        graph.order.each do |model_name|
          models.find { |m| m.name == model_name }.build
        end
      end

      def run_only(custom_schema = nil, only:, glob_path: "app/sql/**/*.sql")
        run(custom_schema, glob_path, include: only)
      end

      def run_except(custom_schema = nil, except:, glob_path: "app/sql/**/*.sql")
        run(custom_schema, glob_path, exclude: except)
      end

      def test
        puts "Running tests..."
        schema = Rdt.settings["schema"] || Rdt::SCHEMA
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

      def filter_file_paths(file_paths, include:, exclude:)
        include_names = normalize_filter_names(include)
        exclude_names = normalize_filter_names(exclude)

        if include_names.any? && exclude_names.any?
          raise ArgumentError, "Use only one of include or exclude"
        end

        selected = file_paths.select do |filepath|
          name = File.basename(filepath, ".sql")
          next false if exclude_names.include?(name)
          include_names.empty? || include_names.include?(name)
        end

        if include_names.any?
          missing = include_names - selected.map { |fp| File.basename(fp, ".sql") }
          unless missing.empty?
            raise ArgumentError, "Could not find SQL files for: #{missing.to_a.sort.join(", ")}"
          end
        end

        selected
      end

      def normalize_filter_names(value)
        Array(value).compact.flat_map do |item|
          if item.is_a?(String)
            item.split(",").map(&:strip)
          else
            item
          end
        end.map { |path| File.basename(path.to_s, ".sql") }.to_set
      end
    end
  end
end
