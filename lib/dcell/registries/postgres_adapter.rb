require 'pg'
require 'uri'

# CREATE DATABASE dcell_test;
# CREATE USER dcell NOSUPERUSER NOCREATEDB NOCREATEROLE;
# \c dcell_test
# CREATE TABLE keys (key text, value text, PRIMARY KEY(key));
# GRANT INSERT, SELECT, UPDATE, TRUNCATE ON TABLE keys TO dcell;
#
# CREATE OR REPLACE FUNCTION public.dcell_upsert(k text, v text)
#  RETURNS void
#  LANGUAGE plpgsql
#  AS $function$
#  BEGIN
#      LOOP
#          -- first try to update the key
#          UPDATE keys SET value = v WHERE key = k;
#          IF found THEN
#              RETURN;
#          END IF;
#          -- not there, so try to insert the key
#          -- if someone else inserts the same key concurrently,
#          -- we could get a unique-key failure
#          BEGIN
#              INSERT INTO keys(key, value) VALUES (k, v);
#              RETURN;
#          EXCEPTION WHEN unique_violation THEN
#              -- Do nothing, and loop to try the UPDATE again.
#          END;
#      END LOOP;
#  END;
#  $function$;

module DCell
  module Registry
    class PostgresAdapter
      def initialize(options)
        # Convert all options to symbols :/
        options = options.inject({}) { |h,(k,v)| h[k.to_sym] = v; h }

        url = options[:url] || ENV['DATABASE_URL']
        @uri = URI.parse(url)

        @global_registry = GlobalRegistry.new(pg_conn)
      end

      def clear_globals
        @global_registry.clear
      end

      def get_global(key);        @global_registry.get(key) end
      def set_global(key, value); @global_registry.set(key, value) end
      def global_keys;            @global_registry.global_keys end

      class GlobalRegistry
        def initialize(pg)
          @pg = pg
        end

        def get(key)
          result = @pg.exec('SELECT value FROM keys WHERE key = $1::text', [{value: key, format: 0, type: 0}])
          Marshal.load result[0]["value"] if result[0]["value"]
        end

        def set(key, value)
          result = @pg.exec('SELECT dcell_upsert($1::text, $2::text);', [{value: key, format: 0, type: 0},{value: Marshal.dump(value), format: 0, type: 0}])
        end

        def global_keys
          result = @pg.exec('SELECT key from keys')
          result.map { |i| i["key"] }
        end

        def clear
          @pg.exec('TRUNCATE keys')
        end
      end

      private 
      def pg_conn
        ::PGconn.new(
          host:     @uri.host,
          user:     @uri.user,
          dbname:   @uri.path[1..-1],
          password: @uri.password,
          port:     @uri.port,
          #sslmode:  'require',
          connect_timeout: 20
        )
      end
    end
  end
end
