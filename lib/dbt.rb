require "zeitwerk"
loader = Zeitwerk::Loader.for_gem
loader.setup

require "dagwood"

module Dbt
  SCHEMA = "felipe_dbt"

  def self.settings
    @settings ||= begin
      path = Rails.root.join("config", "dbt.yml").to_s
      if File.exist?(path)
        YAML.safe_load(ERB.new(File.read(path)).result, aliases: true)
      else
        {}
      end
    end
  end

  def self.run(...)
    Runner.run(...)
  end
end
