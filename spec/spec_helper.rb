$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require 'pathname'
require 'rspec'
require 'shellwords'
require 'pg'
require 'postgres_monitor'


RSpec.configure do |config|
  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end

  config.mock_with :rspec do |mocks|
    mocks.verify_partial_doubles = true
  end

  config.before(:suite) do
    begin
      connection = PG.connect(dbname: 'postgres')
      connection.exec('CREATE ROLE postgres_monitor_test SUPERUSER LOGIN')
      connection.exec('CREATE DATABASE postgres_monitor_test OWNER postgres_monitor_test')
      connection.close
      connection = PG.connect(dbname: 'postgres_monitor_test')
      connection.exec('CREATE TABLE test_table (test_column varchar(5))')
      connection.exec('CREATE UNIQUE INDEX test_column_idx ON test_table (test_column);')
      connection.exec("INSERT INTO test_table (test_column) VALUES ('A');")
      connection.exec('CREATE EXTENSION pg_stat_statements;') if connection.server_version >= 90200
      connection.close
    rescue => error
      puts "\n***********\n"
      puts "DB Setup Error: #{error}"
      puts "\n***********\n"
      puts "\nThese tests expect an instance of Postgres to be on the machine and a database named Postgres to be present\n"
      puts "\n(I hate hard coded things too but could not think of an easier way)\n"
    end
  end

  config.after(:suite) do
    begin
      connection = PG.connect(dbname: 'postgres')
      connection.exec('DROP DATABASE postgres_monitor_test')
      connection.exec('DROP ROLE postgres_monitor_test')
      connection.close
    rescue => error
      puts "\n***********\n"
      puts "DB Teardown Error: #{error}"
      puts "\n***********\n"
    end
  end
end
