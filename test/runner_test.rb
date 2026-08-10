require_relative "test_helper"
require "tmpdir"
require "fileutils"

class RunnerTest < Minitest::Test
  def setup
    @tmpdir = Dir.mktmpdir
    @file_a = create_sql_file("a.sql", "SELECT 1 AS a")
    @file_b = create_sql_file("b.sql", "SELECT 1 AS b")
  end

  def teardown
    FileUtils.remove_entry(@tmpdir)
  end

  def test_filter_file_paths_includes_only_selected_files
    file_paths = [@file_a, @file_b]
    selected = Rdt::Runner.send(:filter_file_paths, file_paths, include: ["a"], exclude: nil)

    assert_equal [@file_a], selected
  end

  def test_filter_file_paths_excludes_specified_files
    file_paths = [@file_a, @file_b]
    selected = Rdt::Runner.send(:filter_file_paths, file_paths, include: nil, exclude: ["b"])

    assert_equal [@file_a], selected
  end

  def test_filter_file_paths_allows_comma_separated_string
    file_paths = [@file_a, @file_b]
    selected = Rdt::Runner.send(:filter_file_paths, file_paths, include: "a,b", exclude: nil)

    assert_equal [@file_a, @file_b], selected
  end

  def test_validate_dependencies_fails_when_model_ref_is_missing
    missing_dependency = ["a"]
    dependencies = { "a" => ["b"] }

    error = assert_raises(RuntimeError) do
      Rdt::Runner.send(:check_if_all_refs_have_sql_files, dependencies)
    end

    assert_match(/Missing \.sql model files for ref \["b"\] in model a/, error.message)
  end

  def test_validate_dependencies_passes_when_all_refs_present
    dependencies = { "a" => ["b"], "b" => [] }

    Rdt::Runner.send(:check_if_all_refs_have_sql_files, dependencies)
    assert true
  end

  def test_validate_selected_model_dependencies_accepts_set_for_selected_names
    model_a = Struct.new(:name, :refs).new("a", ["a"])

    selected_models = [model_a]
    selected_names = Set["a"]

    Rdt::Runner.send(:validate_selected_model_dependencies!, selected_models, {}, selected_names)
    assert true
  end

  def test_filter_file_paths_raises_when_file_missing
    error = assert_raises(ArgumentError) do
      Rdt::Runner.send(:filter_file_paths, [@file_a], include: ["missing"], exclude: nil)
    end

    assert_match(/Could not find SQL files for: missing/, error.message)
  end

  private

  def create_sql_file(name, content)
    path = File.join(@tmpdir, name)
    File.write(path, content)
    path
  end
end
