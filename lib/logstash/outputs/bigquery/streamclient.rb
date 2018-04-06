require 'java'
require 'openssl'
require 'logstash-output-google_bigquery_jars.rb'

module LogStash
  module Outputs
    module BigQuery
      # NOTE: This file uses _a lot_ of Java. Please keep the Java looking
      # java-y so it's easy to tell the languages apart.

      include_package 'com.google.cloud.bigquery'

      # StreamingClient supports shipping data to BigQuery using streams.
      class StreamingClient
        def initialize(json_key_file, project_id, logger)
          @logger = logger

          @bigquery = initialize_google_client json_key_file, project_id
        end

        def table_exists?(dataset, table)
          api_debug('Checking if table exists', dataset, table)
          tbl = @bigquery.getTable dataset, table

          ! tbl.nil?
        end

        # Creates a table with the given name in the given dataset
        def create_table(dataset, table, schema)
          api_debug('Creating table', dataset, table)
          table_id = TableId.of dataset, table

          table_defn = StandardTableDefinition.of schema
          table_info = TableInfo.newBuilder(table_id, table_defn).build()

          @bigquery.create table_info
        end

        def append(dataset, table, rows, ignore_unknown)
          api_debug("Appending #{rows.length} rows", dataset, table)

          request = build_append_request dataset, table, rows, ignore_unknown

          response = @bigQuery.insertAll request
          return true unless response.hasErrors

          response.getInsertErrors().entrySet().each{ |entry|
            key = entry.getKey
            errors = entry.getValue

            errors.each{|bqError|
              @logger.warn('Error while inserting',
                           key: key,
                           location: bqError.getLocation,
                           message: bqError.getMessage,
                           reason: bqError.getReason)
              }
            }

            false
        end

        def build_append_request(dataset, table, rows, ignore_unknown)
          request = InsertAllRequest.newBuiler dataset, table
          request.setIgnoreUnknownValues ignore_unknown

          rows.each { |serialized_row|
            # deserialize rows into Java maps
            deserialized = JrJackson::Json.load(serialized_row, :use_bigdecimal)

            request.addRow deserialized
          }

          request.build
        end

        # raises an exception if the key file is invalid
        def get_key_file_error(json_key_file)
          return nil if json_key_file == ''

          unless ::File.exist? json_key_file
            return "json_key_file does not exist: #{json_key_file}"
          end

          abs = ::File.absolute_path json_key_file
          unless abs == json_key_file
            return "json_key_file must be an absolute path: #{json_key_file}"
          end

          nil
        end

        def initialize_google_client(json_key_path, project_id)
          @logger.debug('Initializing Google API client')
          err = get_key_file_error json_key_file
          raise err unless err.nil?

          if json_key_path.empty?
            return BigQueryOptions.getDefaultInstance().getService()
          end

          # TODO: set User-Agent

          key_file = java.io.FileInputStream.new json_key_path
          credentials = com.google.auth.oauth2.ServiceAccountCredentials.fromStream key_file

          return BigQueryOptions.newBuilder()
                     .setCredentials(credentials)
                     .setProjectId(project_id)
                     .build()
                     .getService()
        end

        private

        def api_debug(message, dataset, table)
          @logger.debug(message, dataset: dataset, table: table)
        end
      end
    end
  end
end
