require "minitest/autorun"
require "active_record"
require "rdt"

begin
  connection_options = {
    adapter: "postgresql",
    database: ENV.fetch("RDT_TEST_DATABASE", "postgres"),
    username: ENV.fetch("RDT_TEST_USER", ENV["USER"]),
    password: ENV["RDT_TEST_PASSWORD"],
    host: ENV["RDT_TEST_HOST"],
    port: ENV["RDT_TEST_PORT"]
  }.compact

  ActiveRecord::Base.establish_connection(connection_options)
  ActiveRecord::Base.connection
rescue StandardError => e
  warn "WARNING: PostgreSQL integration tests disabled (#{e.class}: #{e.message})"
end
