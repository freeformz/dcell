require 'pg'
require 'uri'

# CREATE DATABASE dcell_test;
# CREATE USER dcell NOSUPERUSER NOCREATEDB NOCREATEROLE;
# \c dcell_test
# CREATE TABLE registry(type text, key text, value text, PRIMARY KEY(type, key));
# GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE registry TO dcell;
#
#CREATE OR REPLACE FUNCTION public.dcell_upsert(t text, k text, v text)
# RETURNS void
#LANGUAGE plpgsql
#AS $function$
# BEGIN
#     LOOP
#         -- first try to update the key
#         UPDATE registry SET value = v WHERE type = t and key = k;
#         IF found THEN
#             RETURN;
#         END IF;
#         -- not there, so try to insert the key
#         -- if someone else inserts the same key concurrently,
#         -- we could get a unique-key failure
#         BEGIN
#             INSERT INTO registry(type, key, value) VALUES (t, k, v);
#             RETURN;
#         EXCEPTION WHEN unique_violation THEN
#             -- Do nothing, and loop to try the UPDATE again.
#         END;
#     END LOOP;
# END;
# $function$;

module DCell
  module Registry
    class PostgresAdapter
      def initialize(options)
        # Convert all options to symbols :/
        options = options.inject({}) { |h,(k,v)| h[k.to_sym] = v; h }

        url = options[:url] || ENV['DATABASE_URL']
        @uri = URI.parse(url)

        @node_registry   = NodeRegistry.new(pg_conn)
        @global_registry = GlobalRegistry.new(pg_conn)
      end

      class NodeRegistry
        def initialize(pg)
          @pg = pg
        end

        def get(node_id)
          result = @pg.exec('SELECT value FROM registry WHERE type = $1::text and key = $2::text', [{value: 'nodes', format: 0, type: 0},{value: node_id, format: 0, type: 0}])
          result[0]["value"] if result[0]["value"]
        end

        def set(node_id, addr)
          @pg.exec('SELECT dcell_upsert($1::text, $2::text, $3::text)', [{value: 'nodes', format: 0, type: 0},
                                                                         {value: node_id, format: 0, type: 0},
                                                                         {value: addr, format: 0, type: 0}])
        end

        def nodes
          result = @pg.exec("SELECT key FROM registry where type = 'nodes'")
          result.map { |i| i["key"] }
        end

        def clear
          @pg.exec("DELETE FROM registry WHERE type = 'nodes'")
        end
      end

      def get_node(node_id);       @node_registry.get(node_id) end
      def set_node(node_id, addr); @node_registry.set(node_id, addr) end
      def nodes;                   @node_registry.nodes end
      def clear_nodes;             @node_registry.clear end

      class GlobalRegistry
        def initialize(pg)
          @pg = pg
        end

        def get(key)
          result = @pg.exec('SELECT value FROM registry WHERE type = $1::text and key = $2::text', [{value: 'globals', format: 0, type: 0},
                                                                                                    {value: key, format: 0, type: 0}])
          Marshal.load result[0]["value"] if result[0]["value"]
        end

        def set(key, value)
          @pg.exec('SELECT dcell_upsert($1::text, $2::text, $3::text)', [{value: 'globals', format: 0, type: 0},
                                                                         {value: key, format: 0, type: 0},
                                                                         {value: Marshal.dump(value), format: 0, type: 0}])
        end

        def global_keys
          result = @pg.exec('SELECT key FROM registry WHERE type = \'globals\'')
          result.map { |i| i["key"] }
        end

        def clear
          @pg.exec("DELETE FROM registry WHERE type = 'globals'")
        end
      end

      def get_global(key);        @global_registry.get(key) end
      def set_global(key, value); @global_registry.set(key, value) end
      def global_keys;            @global_registry.global_keys end
      def clear_globals;          @global_registry.clear end

      private 
      def pg_conn
        ::PGconn.new(
          host:     @uri.host,
          user:     @uri.user,
          dbname:   @uri.path[1..-1],
          password: @uri.password,
          port:     @uri.port,
          sslmode:  'prefer',
          connect_timeout: 20
        )
      end
    end
  end
end
