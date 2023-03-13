Gem::Specification.new do |s|
  s.name        = "dbt"
  s.version     = "0.0.6"
  s.summary     = "Dbt"
  s.description = "A simple hello world gem"
  s.authors     = ["Felipe Mesquita"]
  s.email       = "felipemesquita@hey.com"
  s.files       = ["lib/dbt.rb", "lib/dbt/model.rb"]
  s.homepage    = "https://github.com/felipedmesquita/dbt"
  s.license     = "MIT"

  s.add_dependency 'dagwood', '~> 1.0'
  s.add_dependency 'zeitwerk'
end
