require "zeitwerk"
loader = Zeitwerk::Loader.for_gem
loader.setup

require "dagwood"
require "pg_query"

module Dbt
  SCHEMA = "felipe_dbt"

  def self.run(...)
    Runner.run(...)
  end
end
