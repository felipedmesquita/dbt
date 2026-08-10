require_relative "test_helper"
require "tmpdir"
require "fileutils"
require "securerandom"

class RunnerIntegrationTest < Minitest::Test
  def setup
    @tmpdir = Dir.mktmpdir
    skip "PostgreSQL not available for integration tests" unless ActiveRecord::Base.connected?

    @schema = "rdt_test_#{SecureRandom.hex(4)}"
    ActiveRecord::Base.connection.execute("DROP SCHEMA IF EXISTS #{@schema} CASCADE")
    @file_one = create_sql_file("one.sql", "SELECT 1 AS one")
    @file_two = create_sql_file("two.sql", "SELECT 2 AS two")
  end

  def teardown
    FileUtils.remove_entry(@tmpdir) if @tmpdir
    return unless ActiveRecord::Base.connected?

    ActiveRecord::Base.connection.execute("DROP SCHEMA IF EXISTS #{@schema} CASCADE")
  end

  def test_run_only_builds_selected_view
    Rdt.run_only(@schema, only: ["one"], glob_path: File.join(@tmpdir, "*.sql"))

    assert_relation_exists("one")
    refute_relation_exists("two")
  end

  def test_run_except_skips_excluded_view
    Rdt.run_except(@schema, except: ["two"], glob_path: File.join(@tmpdir, "*.sql"))

    assert_relation_exists("one")
    refute_relation_exists("two")
  end

  private

  def create_sql_file(name, content)
    path = File.join(@tmpdir, name)
    File.write(path, content)
    path
  end

  def relation_exists?(name)
    result = ActiveRecord::Base.connection.execute(<<~SQL)
      SELECT 1
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relname = '#{name}' AND n.nspname = '#{@schema}';
    SQL
    result.any?
  end

  def assert_relation_exists(name)
    assert relation_exists?(name), "Expected relation #{name} to exist in schema #{@schema}"
  end

  def refute_relation_exists(name)
    refute relation_exists?(name), "Expected relation #{name} not to exist in schema #{@schema}"
  end
end
