module PostgresMonitor
  class Monitor
      def initialize(connection_params)
        @host     = connection_params[:host]
        @port     = connection_params[:port] ? connection_params[:port] : self.port
        @user     = connection_params[:user]
        @password = connection_params[:password]
        @sslmode  = connection_params[:sslmode] ? connection_params[:sslmode] : 'require'
        @dbname   = connection_params[:dbname]

        @long_query_threshold = connection_params[:long_query_threshold] ? connection_params[:long_query_threshold] : '5 seconds'

        @connection = self.connect
      end

      # returns database version in SQL form
      def get_database_version
        execute_sql 'SELECT version();'
      end

      # returns database tranasction and row activity for the DB
      def database_query
        execute_sql "SELECT * FROM pg_stat_database WHERE datname='#{@dbname}';"
      end

      # returns Scheduled and Requested Checkpoints
      def bgwriter_query
        execute_sql 'SELECT * FROM pg_stat_bgwriter;'
      end

      # count of indexes in the database
      def index_count_query
        execute_sql "SELECT count(1) as indexes FROM pg_class WHERE relkind = 'i';"
      end

      def index_size_query
        execute_sql 'SELECT sum(relpages::bigint*8192) AS size FROM pg_class WHERE reltype = 0;'
      end

      # show the count of sequential scans by table descending by order
      def seq_scans
        execute_sql 'SELECT relname AS name, seq_scan as count FROM pg_stat_user_tables ORDER BY seq_scan DESC;'
      end

      # show all tables and the number of rows in each ordered by number of rows descending
      def records_rank
        execute_sql 'SELECT relname AS name, n_live_tup AS estimated_count FROM pg_stat_user_tables ORDER BY n_live_tup DESC;'
      end

      # list all non-template DBs known
      def list_databases
        execute_sql 'SELECT datname FROM pg_database WHERE datistemplate = false;'
      end

      # list connection states and count
      def connection_counts
        execute_sql "SELECT #{state_column}, COUNT(*) FROM pg_stat_activity GROUP BY #{state_column};"
      end

      ### DEPRECATION WARNING
      # This seems to have an issue with returning multiple duplicate results;
      # Deprecating in favor of connection_counts
      # returns Active and Idle connections from DB
      def backend_query
        warn 'DEPRECATED. Please use connection_counts instead'

        sql = %Q(
          SELECT ( SELECT count(*) FROM pg_stat_activity WHERE
            #{
              if nine_two?
                "state <> 'idle'"
              else
                "current_query <> '<IDLE>'"
              end
            }
          ) AS backends_active, ( SELECT count(*) FROM pg_stat_activity WHERE
            #{
              if nine_two?
                "state = 'idle'"
              else
                "current_query = '<IDLE>'"
              end
            }
          ) AS backends_idle FROM pg_stat_activity;
        )

        execute_sql(sql)
      end

      # get database sizes
      def get_database_sizes
        sql = %q(
          SELECT
            t1.datname AS db_name,
            pg_size_pretty(pg_database_size(t1.datname)) as db_size
          FROM
            pg_database t1
          ORDER BY
            pg_database_size(t1.datname) desc;
        )

        execute_sql(sql)
      end

      # calculates your cache hit rate (effective databases are at 99% and up)
      def cache_hit
        sql = %q(
          SELECT
            'index hit rate' AS name,
            (sum(idx_blks_hit)) / nullif(sum(idx_blks_hit + idx_blks_read),0) AS ratio
          FROM
            pg_statio_user_indexes
          UNION ALL
          SELECT
            'table hit rate' AS name,
            sum(heap_blks_hit) / nullif(sum(heap_blks_hit) + sum(heap_blks_read),0) AS ratio
          FROM
            pg_statio_user_tables;
        )

        execute_sql(sql)
      end

      # show table and index bloat in your database ordered by most wasteful
      def database_bloat
         sql = %q(
            WITH constants AS (
                    SELECT current_setting('block_size')::numeric AS bs, 23 AS hdr, 4 AS ma
                  ), bloat_info AS (
                    SELECT
                      ma,bs,schemaname,tablename,
                      (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
                      (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
                    FROM (
                      SELECT
                        schemaname, tablename, hdr, ma, bs,
                        SUM((1-null_frac)*avg_width) AS datawidth,
                        MAX(null_frac) AS maxfracsum,
                        hdr+(
                          SELECT 1+count(*)/8
                          FROM pg_stats s2
                          WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename
                        ) AS nullhdr
                      FROM pg_stats s, constants
                      GROUP BY 1,2,3,4,5
                    ) AS foo
                  ), table_bloat AS (
                    SELECT
                      schemaname, tablename, cc.relpages, bs,
                      CEIL((cc.reltuples*((datahdr+ma-
                        (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::float)) AS otta
                    FROM bloat_info
                      JOIN pg_class cc ON cc.relname = bloat_info.tablename
                      JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = bloat_info.schemaname AND nn.nspname <> 'information_schema'
                  ), index_bloat AS (
                    SELECT
                      schemaname, tablename, bs,
                      COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages,
                      COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::float)),0) AS iotta -- very rough approximation, assumes all cols
                    FROM bloat_info
                      JOIN pg_class cc ON cc.relname = bloat_info.tablename
                      JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = bloat_info.schemaname AND nn.nspname <> 'information_schema'
                      JOIN pg_index i ON indrelid = cc.oid
                      JOIN pg_class c2 ON c2.oid = i.indexrelid
                  )
                  SELECT
                    type, schemaname, object_name, bloat, pg_size_pretty(raw_waste) as waste
                  FROM
                  (SELECT
                    'table' as type,
                    schemaname,
                    tablename as object_name,
                    ROUND(CASE WHEN otta=0 THEN 0.0 ELSE table_bloat.relpages/otta::numeric END,1) AS bloat,
                    CASE WHEN relpages < otta THEN '0' ELSE (bs*(table_bloat.relpages-otta)::bigint)::bigint END AS raw_waste
                  FROM
                    table_bloat
                      UNION
                  SELECT
                    'index' as type,
                    schemaname,
                    tablename || '::' || iname as object_name,
                    ROUND(CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages/iotta::numeric END,1) AS bloat,
                    CASE WHEN ipages < iotta THEN '0' ELSE (bs*(ipages-iotta))::bigint END AS raw_waste
                  FROM
                    index_bloat) bloat_summary
                  ORDER BY raw_waste DESC, bloat DESC
            )

          execute_sql(sql)
      end

      # show dead rows and whether an automatic vacuum is expected to be triggered
      def vacuum_stats
         sql = %q(
          WITH table_opts AS (
            SELECT
              pg_class.oid, relname, nspname, array_to_string(reloptions, '') AS relopts
            FROM
               pg_class INNER JOIN pg_namespace ns ON relnamespace = ns.oid
          ), vacuum_settings AS (
            SELECT
              oid, relname, nspname,
              CASE
                WHEN relopts LIKE '%autovacuum_vacuum_threshold%'
                  THEN regexp_replace(relopts, '.*autovacuum_vacuum_threshold=([0-9.]+).*', E'\\\\\\1')::integer
                  ELSE current_setting('autovacuum_vacuum_threshold')::integer
                END AS autovacuum_vacuum_threshold,
              CASE
                WHEN relopts LIKE '%autovacuum_vacuum_scale_factor%'
                  THEN regexp_replace(relopts, '.*autovacuum_vacuum_scale_factor=([0-9.]+).*', E'\\\\\\1')::real
                  ELSE current_setting('autovacuum_vacuum_scale_factor')::real
                END AS autovacuum_vacuum_scale_factor
            FROM
              table_opts
          )
          SELECT
            vacuum_settings.nspname AS schema,
            vacuum_settings.relname AS table,
            to_char(psut.last_vacuum, 'YYYY-MM-DD HH24:MI') AS last_vacuum,
            to_char(psut.last_autovacuum, 'YYYY-MM-DD HH24:MI') AS last_autovacuum,
            to_char(pg_class.reltuples, '9G999G999G999') AS rowcount,
            to_char(psut.n_dead_tup, '9G999G999G999') AS dead_rowcount,
            to_char(autovacuum_vacuum_threshold
                 + (autovacuum_vacuum_scale_factor::numeric * pg_class.reltuples), '9G999G999G999') AS autovacuum_threshold,
            CASE
              WHEN autovacuum_vacuum_threshold + (autovacuum_vacuum_scale_factor::numeric * pg_class.reltuples) < psut.n_dead_tup
                THEN
                  'yes'
              ELSE
                'no'
            END AS expect_autovacuum
          FROM
            pg_stat_user_tables psut INNER JOIN pg_class ON psut.relid = pg_class.oid
              INNER JOIN vacuum_settings ON pg_class.oid = vacuum_settings.oid
          ORDER BY 1
       )

        execute_sql(sql)
      end

     ## INDEX queries
     # calculates your index hit rate
     def index_usage
       sql = %q(
          SELECT
            relname,
            CASE
              WHEN idx_scan > 0
                THEN (100 * idx_scan / (seq_scan + idx_scan))::text
              ELSE
                'Insufficient data'
              END AS percent_of_times_index_used,
              n_live_tup rows_in_table
          FROM
            pg_stat_user_tables
          ORDER BY
            n_live_tup DESC;
        )

       execute_sql(sql)
     end

     # show the total size of all indexes in MB
     def total_index_size
       sql = %q(
          SELECT
            pg_size_pretty(sum(c.relpages::bigint*8192)::bigint) AS size
          FROM
            pg_class c
            LEFT JOIN pg_namespace n ON (n.oid = c.relnamespace)
          WHERE
            n.nspname NOT IN ('pg_catalog', 'information_schema')
            AND n.nspname !~ '^pg_toast'
            AND c.relkind='i';
       )

       execute_sql(sql)
     end

     # show the size of indexes, descending by size
     def index_size
        sql = %q(
          SELECT
            c.relname AS name,
            pg_size_pretty(sum(c.relpages::bigint*8192)::bigint) AS size
          FROM
            pg_class c
            LEFT JOIN pg_namespace n ON (n.oid = c.relnamespace)
          WHERE
            n.nspname NOT IN ('pg_catalog', 'information_schema')
            AND n.nspname !~ '^pg_toast'
            AND c.relkind='i'
          GROUP BY
            c.relname
          ORDER BY
            sum(c.relpages) DESC;
        )
        execute_sql(sql)
     end

     # show unused and almost unused indexes, ordered by their size relative to
     # the number of index scans. Exclude indexes of very small tables (less than
     # 5 pages), where the planner will almost invariably select a sequential
     # scan, but may not in the future as the table grows.
     def unused_indexes
        sql = %q(
          SELECT
            schemaname || '.' || relname AS table,
            indexrelname AS index,
            pg_size_pretty(pg_relation_size(i.indexrelid)) AS index_size,
            idx_scan as index_scans
          FROM
            pg_stat_user_indexes ui
          JOIN
            pg_index i ON ui.indexrelid = i.indexrelid
          WHERE NOT
            indisunique
            AND idx_scan < 50
            AND pg_relation_size(relid) > 5 * 8192
          ORDER BY
            pg_relation_size(i.indexrelid) / nullif(idx_scan, 0) DESC NULLS FIRST,
            pg_relation_size(i.indexrelid) DESC;
         )

        execute_sql(sql)
     end

     ## TABLE QUERIES
     # show the size of the tables (excluding indexes), descending by size
     def table_size
        sql = %q(
          SELECT
            c.relname AS name,
            pg_size_pretty(pg_table_size(c.oid)) AS size
          FROM
            pg_class c
            LEFT JOIN pg_namespace n ON (n.oid = c.relnamespace)
          WHERE
            n.nspname NOT IN ('pg_catalog', 'information_schema')
            AND n.nspname !~ '^pg_toast'
            AND c.relkind='r'
          ORDER BY
          pg_table_size(c.oid) DESC;
        )

        execute_sql(sql)
      end

      # show the total size of all the indexes on each table, descending by size
      def table_indexes_size
          sql = %q(
            SELECT
              c.relname AS table,
              pg_size_pretty(pg_indexes_size(c.oid)) AS index_size
            FROM
              pg_class c
              LEFT JOIN pg_namespace n ON (n.oid = c.relnamespace)
            WHERE
              n.nspname NOT IN ('pg_catalog', 'information_schema')
              AND n.nspname !~ '^pg_toast'
              AND c.relkind='r'
            ORDER BY
              pg_indexes_size(c.oid) DESC;
          )

          execute_sql(sql)
       end

      # show the size of the tables (including indexes), descending by size
      def total_table_size
        sql = %q(
          SELECT
            c.relname AS name,
            pg_size_pretty(pg_total_relation_size(c.oid)) AS size
          FROM
            pg_class c
            LEFT JOIN pg_namespace n ON (n.oid = c.relnamespace)
          WHERE
            n.nspname NOT IN ('pg_catalog', 'information_schema')
            AND n.nspname !~ '^pg_toast'
            AND c.relkind='r'
          ORDER BY
            pg_total_relation_size(c.oid) DESC;
        )

        execute_sql(sql)
      end

      ## QUERY QUERIES [that feels wrong...]
      # display queries holding locks other queries are waiting to be released
      def blocking
        query_column = self.query_column
        pid_column = self.pid_column
        sql = %Q(
          SELECT
              bl.pid AS blocked_pid,
              ka.#{query_column} AS blocking_statement,
              now() - ka.query_start AS blocking_duration,
              kl.pid AS blocking_pid,
              a.#{query_column} AS blocked_statement,
              now() - a.query_start AS blocked_duration
          FROM
              pg_catalog.pg_locks bl
          JOIN
              pg_catalog.pg_stat_activity a ON bl.pid = a.#{pid_column}
          JOIN
              pg_catalog.pg_locks kl
          JOIN
              pg_catalog.pg_stat_activity ka
              ON kl.pid = ka.#{pid_column}
              ON bl.transactionid = kl.transactionid AND bl.pid != kl.pid
          WHERE NOT
            bl.granted
        )

        execute_sql(sql)
      end

      # display queries with active locks
      def locks
        query_column = self.query_column
        pid_column = self.pid_column
        sql = %Q(
         SELECT
            pg_stat_activity.#{pid_column} AS pid,
            pg_class.relname,
            pg_locks.transactionid,
            pg_locks.granted,
            pg_stat_activity.#{query_column} AS query,
            age(now(),pg_stat_activity.query_start) AS age
         FROM
            pg_stat_activity,pg_locks left
            OUTER JOIN pg_class ON (pg_locks.relation = pg_class.oid)
         WHERE
            pg_stat_activity.#{query_column} <> '<insufficient privilege>'
            AND pg_locks.pid = pg_stat_activity.#{pid_column}
            AND pg_locks.mode = 'ExclusiveLock'
            AND pg_stat_activity.#{pid_column} <> pg_backend_pid() order by query_start;
        )

        execute_sql(sql)
      end

      # show all queries longer than five minutes by descending duration
      def long_running_queries
        query_column = self.query_column
        pid_column = self.pid_column
        sql = %Q(
          SELECT
            #{pid_column} AS process,
            now() - pg_stat_activity.query_start AS duration,
            #{query_column} AS query
          FROM
            pg_stat_activity
          WHERE
            pg_stat_activity.#{query_column} <> ''::text
            #{
              if nine_two?
                "AND state <> 'idle'"
              else
                "AND current_query <> '<IDLE>'"
              end
            }
            AND now() - pg_stat_activity.query_start > interval '#{@long_query_threshold}'
          ORDER BY
            now() - pg_stat_activity.query_start DESC;
        )

        execute_sql(sql)
      end

      ##PG_STATS_STATEMENTS QUERIES
      # reset pg_stats
      def reset_pg_stats_statements
        return unless extension_loaded? 'pg_stat_statements'
        execute_sql 'SELECT pg_stat_statements_reset();'
      end
      # show 10 queries that have longest execution time in aggregate.
      # needs pg_stat_statements extension
      def outliers
        return unless extension_loaded? 'pg_stat_statements'

        sql = %q(
            SELECT
              query AS qry,
              interval '1 millisecond' * total_time AS total_exec_time,
              to_char((total_time/sum(total_time) OVER()) * 100, 'FM90D0') || '%'  AS prop_exec_time,
              to_char(calls, 'FM999G999G999G990') AS ncalls,
              interval '1 millisecond' * (blk_read_time + blk_write_time) AS sync_io_time
            FROM
              pg_stat_statements WHERE userid = (SELECT usesysid FROM pg_user WHERE usename = current_user LIMIT 1)
            ORDER BY
            total_time DESC LIMIT 10
        )

        execute_sql(sql)
      end

      # show 10 most frequently called queries
      # This is dependent on the pg_stat_statements being loaded
      def calls
        return unless extension_loaded? 'pg_stat_statements'
        sql = %Q(
          SELECT
            query AS qry,
            interval '1 millisecond' * total_time AS exec_time,
            to_char((total_time/sum(total_time) OVER()) * 100, 'FM90D0') || '%'  AS prop_exec_time,
            to_char(calls, 'FM999G999G990') AS ncalls,
            interval '1 millisecond' * (blk_read_time + blk_write_time) AS sync_io_time
        FROM
          pg_stat_statements WHERE userid = (SELECT usesysid FROM pg_user WHERE usename = current_user LIMIT 1)
        ORDER BY
          calls DESC LIMIT 10
        )

        execute_sql(sql)
      end

      ## GENERAL METHODS
      def connect
        PG::Connection.new(:host => @host, :port => @port, :user => @user, :password => @password, :sslmode => @sslmode, :dbname => @dbname)
      end

      def port
        @port || 5432
      end

      # Certain queries are dependent on the Postgres version
      def nine_two?
        @connection.server_version >= 90200
      end

      def query_column
        nine_two? ? 'query' : 'current_query'
      end

      def pid_column
        nine_two? ? 'pid' : 'procpid'
      end

      def state_column
        nine_two? ? 'state' : 'current_query'
      end

      def extension_loaded?(extname)
        @connection.exec("SELECT count(*) FROM pg_extension WHERE extname = '#{extname}'") do |result|
          result[0]['count'] == '1'
        end
      end

      def close_connection
        @connection.close
      end

      private
      def execute_sql(query)
        @connection.exec(query)
      end
  end
end
