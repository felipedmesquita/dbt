require "zeitwerk"
loader = Zeitwerk::Loader.for_gem
loader.setup

require "dagwood"

module Dbt
  SCHEMA = "felipe_dbt"

  def self.run(...)
    Runner.run(...)
  end
end
