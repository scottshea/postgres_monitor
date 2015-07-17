require 'spec_helper'

describe PostgresMonitor do
  it 'has a version number' do
    expect(PostgresMonitor::VERSION).not_to be nil
  end

  context 'Monitor Queries' do
    before do
      @database_name = 'postgres_monitor_test'
      @connection_params = {
        host: 'localhost',
        port: nil,
        user: 'postgres_monitor_test',
        password: nil,
        sslmode: 'allow',
        dbname: @database_name
      }

      @monitor = PostgresMonitor::Monitor.new(@connection_params)
    end

    after do
      @monitor.close_connection
    end

    it 'should return the DB version via SQL' do
      result = @monitor.get_database_version
      expect(result.first.keys.include? 'version').to eq true
    end

    it 'should return a list of databases' do
        results = @monitor.list_databases
        databases = []
        results.each do |row|
          databases << row['datname']
        end

        expect(databases.include?(@database_name)).to be true
    end

    it 'should return the pg_stat info for the database' do
      fields = %w(
        datid
        datname
        numbackends
        xact_commit
        xact_rollback
        blks_read
        blks_hit
        tup_returned
        tup_fetched
        tup_inserted
        tup_updated
        tup_deleted
        conflicts
        temp_files
        temp_bytes
        deadlocks
        blk_read_time
        blk_write_time
        stats_reset
      )
      results = @monitor.database_query
      results.each do |row|
        next unless row['datname'] == @database_name
        expect(row.keys).to eq fields
      end
    end

    it 'should return bgwriter stats from the db' do
      fields = %w(
        checkpoints_timed
        checkpoints_req
        checkpoint_write_time
        checkpoint_sync_time
        buffers_checkpoint
        buffers_clean
        maxwritten_clean
        buffers_backend
        buffers_backend_fsync
        buffers_alloc
        stats_reset
      )
      results = @monitor.bgwriter_query
      row = results.first
      expect(row.keys).to eq fields
    end

    it 'should return a count of the indexes in the db' do
      results = @monitor.index_count_query
      row = results.first
      expect(row.keys.include? 'indexes').to be true
      expect(row['indexes'].to_i).to be >= 0
    end

    it 'should return the size of the indexes in the db' do
      results = @monitor.index_size_query
      row = results.first
      expect(row.keys.include? 'size').to be true
      expect(row['size'].to_i).to be >= 0
    end

    it 'should return a list of the tables and count of seq scans per table' do
      results = @monitor.seq_scans
      row = results.first
      expect(row.keys.include? 'name').to be true
      expect(row['name']).to eq 'test_table'
      expect(row['count'].to_i).to be >= 0
    end

    it 'should return a list of tables and their records' do
      results = @monitor.records_rank
      row = results.first
      expect(row['name']).to eq 'test_table'
      expect(row['estimated_count'].to_i).to eq 1
    end

    it 'should return Active and Idle connection count' do
      results = @monitor.backend_query
      row = results.first
      expect(row.keys.include? 'backends_active').to be true
      expect(row.keys.include? 'backends_idle').to be true
      expect(row['backends_active'].to_i).to be >= 1
      expect(row['backends_idle'].to_i).to be >= 0
    end

    it 'should return the cache hit ratio' do
      results = @monitor.cache_hit
      fields = []
      results.each do |row|
        fields << row['name']
      end

      expect(fields.include?('index hit rate')).to be true
      expect(fields.include?('table hit rate')).to be true
    end

    it 'should return the bloat and waste of the database by table' do
      fields = %w(
        type
        schemaname
        object_name
        bloat
        waste
      )
      results = @monitor.database_bloat
      expect(results.first.keys).to eq fields
    end

    it 'should return the rows and dead rows of the tables in the database' do
      fields = %w(
        schema
        table
        last_vacuum
        last_autovacuum
        rowcount
        dead_rowcount
        autovacuum_threshold
        expect_autovacuum
      )
      results = @monitor.vacuum_stats
      expect(results.first.keys).to eq fields
    end

    it 'should return the size of the databases' do
        results = @monitor.get_database_sizes
        databases = []
        results.each do |row|
          databases << row['db_name']
          expect(row['db_size'].to_i).to be >= 0
        end

        expect(databases.include?(@database_name)).to be true
    end

    it 'should return the index usage of the database' do
      fields = %w(
        relname
        percent_of_times_index_used
        rows_in_table
      )
      results = @monitor.index_usage
      expect(results.first.keys).to eq fields
    end

    it 'should show the total size of the indexs in the database' do
      results = @monitor.total_index_size
      row = results.first
      expect(row.keys.include? 'size').to be true
      expect(row['size'].to_i).to be >= 0
    end

    it 'should show the size of the indexes in the database' do
      results = @monitor.index_size
      row = results.first
      expect(row['name']).to eq 'test_column_idx'
      expect(row['size'].include? 'bytes').to be true
      expect(row['size'].to_i).to be >= 1
    end

    # Need to figure out how to create an unused index
    xit 'should return a list of the un- or under-used indexes' do
      results = @monitor.unused_indexes
      results.each do |row|
        puts row.inspect
      end
    end

    it 'should return a list of the tables and their size' do
      results = @monitor.table_size
      row = results.first
      expect(row['name']).to eq 'test_table'
      expect(row['size'].include? 'bytes').to be true
      expect(row['size'].to_i).to be >= 1
    end

    it 'should return a list of the tables and the size of the indexes on them' do
      results = @monitor.table_indexes_size
      row = results.first
      expect(row['table']).to eq 'test_table'
      expect(row['index_size'].to_i).to be >= 1
    end

    it 'should return a list of the tables their total size' do
      results = @monitor.total_table_size
      row = results.first
      expect(row['name']).to eq 'test_table'
      expect(row['size'].to_i).to be >= 1
    end

    #TODO: Figure out way to test these
    xit 'should display queries holding locks other queries are waiting to be released' do
      results = @monitor.blocking
      row = results.first
    end

    xit 'should display queries with active locks' do
      results = @monitor.locks
      row = results.first
    end

    xit 'show all queries longer than five minutes by descending duration' do
      results = @monitor.long_running_queries
      row = results.first
    end

    it 'should reset pg_stats' do
      @monitor.reset_pg_stats_statements
      after = @monitor.send :execute_sql, 'SELECT count(*) FROM pg_stat_statements;'
      expect(after.first['count'].to_i).to eq 1
    end

    it 'should show 10 queries that have longest execution time in aggregate' do
      fields = %w(
        qry
        total_exec_time
        prop_exec_time
        ncalls
        sync_io_time
      )
      results = @monitor.outliers
      row = results.first
      expect(row.keys).to eq fields
    end

    it 'should show 10 most frequently called queries' do
      fields = %w(
        qry
        exec_time
        prop_exec_time
        ncalls
        sync_io_time
      )
      results = @monitor.calls
      row = results.first
      expect(row.keys).to eq fields
    end
  end
end
