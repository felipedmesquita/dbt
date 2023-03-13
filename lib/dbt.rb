
module Dbt
  def self.run
    file_paths = Dir.glob("app/sql/**/*.sql")
    models = file_paths.map {|fp| Model.new fp }
    dependencies = models.map {|m| {m.name => m.refs}}.reduce({}, :merge!)
    graph = Dagwood::DependencyGraph.new dependencies
    graph.order.each do |model_name|
      models.find {|m| m.name == model_name}.build
    end
  end

end

require 'dbt/model.rb'
