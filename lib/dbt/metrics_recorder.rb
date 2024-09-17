module Dbt
  class MetricsRecorder
    def initialize(schema)
      @schema = schema
      @metrics = []
    end

    def record(model_name, model_path, start_time, end_time)
      @metrics << {
        model_name: model_name,
        model_path: model_path,
        start_time: start_time,
        end_time: end_time,
        duration: end_time - start_time
      }
    end

    def save
      begin
        # Create the metrics table if it doesn't exist
        ActiveRecord::Base.connection.execute <<-SQL
          CREATE TABLE IF NOT EXISTS #{@schema}.dbt_internal_metrics (
            id SERIAL PRIMARY KEY,
            run_id INTEGER,
            model_name TEXT,
            model_path TEXT,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            duration FLOAT
          )
        SQL

        # Generate a new run_id
        result = ActiveRecord::Base.connection.execute("SELECT MAX(run_id) AS last_run_id FROM #{@schema}.dbt_internal_metrics")
        last_run_id = result.first["last_run_id"] || 0
        run_id = last_run_id + 1

        # Insert all metrics into the database
        @metrics.each do |metric|
          ActiveRecord::Base.connection.execute <<-SQL
            INSERT INTO #{@schema}.dbt_internal_metrics (run_id, model_name, model_path, start_time, end_time, duration)
            VALUES (
              #{run_id},
              '#{metric[:model_name]}',
              '#{metric[:model_path]}',
              '#{metric[:start_time]}',
              '#{metric[:end_time]}',
              #{metric[:duration]}
            )
          SQL
        end
      rescue => e
        # Handle exceptions within the class
        puts "Warning: MetricsRecorder encountered an error during save: #{e.message}"
      end
    end
  end
end
