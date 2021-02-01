require 'logstash/outputs/base'
require 'logstash/namespace'
require 'logstash/json'
require 'logstash/outputs/bigquery/streamclient'
require 'logstash/outputs/bigquery/batcher'
require 'logstash/outputs/bigquery/schema'

require 'time'
require 'fileutils'
require 'concurrent'

#
# === Summary
#
# This plugin uploads events to Google BigQuery using the streaming API
# so data can become available nearly immediately.
#
# You can configure it to flush periodically, after N events or after
# a certain amount of data is ingested.
#
# === Environment Configuration
#
# You must enable BigQuery on your GCS account and create a dataset to
# hold the tables this plugin generates.
#
# You must also grant the service account this plugin uses access to
# the dataset.
#
# You can use https://www.elastic.co/guide/en/logstash/current/event-dependent-configuration.html[Logstash conditionals]
# and multiple configuration blocks to upload events with different structures.
#
# === Usage
# This is an example of logstash config:
#
# [source,ruby]
# --------------------------
# output {
#    google_bigquery {
#      project_id => "folkloric-guru-278"                        (required)
#      dataset => "logs"                                         (required)
#      csv_schema => "path:STRING,status:INTEGER,score:FLOAT"    (required) <1>
#      json_key_file => "/path/to/key.json"                      (optional) <2>
#      error_directory => "/tmp/bigquery-errors"                 (required)
#      date_pattern => "%Y-%m-%dT%H:00"                          (optional)
#      flush_interval_secs => 30                                 (optional)
#    }
# }
# --------------------------
#
# <1> Specify either a csv_schema or a json_schema.
#
# <2> If the key is not used, then the plugin tries to find
# https://cloud.google.com/docs/authentication/production[Application Default Credentials]
#
# === Considerations
#
# * There is a small fee to insert data into BigQuery using the streaming API
# * This plugin buffers events in-memory, so make sure the flush configurations are appropriate
#   for your use-case and consider using
#   https://www.elastic.co/guide/en/logstash/current/persistent-queues.html[Logstash Persistent Queues]
#
# === Additional Resources
#
# * https://cloud.google.com/bigquery/[BigQuery Introduction]
# * https://cloud.google.com/bigquery/docs/schemas[BigQuery Schema Formats and Types]
# * https://cloud.google.com/bigquery/pricing[Pricing Information]
#
class LogStash::Outputs::GoogleBigQuery < LogStash::Outputs::Base
  config_name 'google_bigquery'

  concurrency :shared

  # Google Cloud Project ID (number, not Project Name!).
  config :project_id, validate: :string, required: true

  # The BigQuery dataset the tables for the events will be added to.
  config :dataset, validate: :string, required: true

  # BigQuery table ID prefix to be used when creating new tables for log data.
  # Table name will be `<table_prefix><table_separator><date>`
  config :table_prefix, validate: :string, default: 'logstash'

  # BigQuery table separator to be added between the table_prefix and the
  # date suffix.
  config :table_separator, validate: :string, default: '_'

  # BigQuery table name to be used when inserting data into an existing table
  # (Useful when using partitioned table)
  config :table_name, validate: :string, required: false, default: nil

  # Schema for log data. It must follow the format `name1:type1(,name2:type2)*`.
  # For example, `path:STRING,status:INTEGER,score:FLOAT`.
  config :csv_schema, validate: :string, required: false, default: nil

  # Schema for log data as a hash.
  # These can include nested records, descriptions, and modes.
  #
  # Example:
  # # [source,ruby]
  # --------------------------
  # json_schema => {
  #   fields => [{
  #     name => "endpoint"
  #     type => "STRING"
  #     description => "Request route"
  #   }, {
  #     name => "status"
  #     type => "INTEGER"
  #     mode => "NULLABLE"
  #   }, {
  #     name => "params"
  #     type => "RECORD"
  #     mode => "REPEATED"
  #     fields => [{
  #       name => "key"
  #       type => "STRING"
  #      }, {
  #       name => "value"
  #       type => "STRING"
  #     }]
  #   }]
  # }
  # --------------------------
  config :json_schema, validate: :hash, required: false, default: nil

  # Indicates if BigQuery should ignore values that are not represented in the table schema.
  # If true, the extra values are discarded.
  # If false, BigQuery will reject the records with extra fields and the job will fail.
  # The default value is false.
  #
  # NOTE: You may want to add a Logstash filter like the following to remove common fields it adds:
  # [source,ruby]
  # ----------------------------------
  # mutate {
  #     remove_field => ["@version","@timestamp","path","host","type", "message"]
  # }
  # ----------------------------------
  config :ignore_unknown_values, validate: :boolean, default: false

  # Time pattern for BigQuery table, defaults to hourly tables.
  # Must Time.strftime patterns: www.ruby-doc.org/core-2.0/Time.html#method-i-strftime
  config :date_pattern, validate: :string, default: '%Y-%m-%dT%H:00'

  # If logstash is running within Google Compute Engine, the plugin will use
  # GCE's Application Default Credentials. Outside of GCE, you will need to
  # specify a Service Account JSON key file.
  config :json_key_file, validate: :string, required: false

  # The number of messages to upload at a single time. (< 1000, default: 128)
  config :batch_size, validate: :number, required: true, default: 128

  # An approximate number of bytes to upload as part of a batch. Default: 1MB
  config :batch_size_bytes, validate: :number, required: true, default: 1_000_000

  # Uploads all data this often even if other upload criteria aren't met. Default: 5s
  config :flush_interval_secs, validate: :number, required: true, default: 5

  # The location to store events that could not be uploaded due to errors.
  # Consider using an additional Logstash input to pipe the contents of
  # these to an alert platform so you can manually fix the events.
  #
  # Or use https://cloud.google.com/storage/docs/gcs-fuse[GCS FUSE] to
  # transparently upload to a GCS bucket.
  #
  # Files names follow the pattern `[table name]-[UNIX timestamp].log`
  config :error_directory, validate: :string, required: true, default: '/tmp/bigquery_errors'

  # Insert all valid rows of a request, even if invalid rows exist. The default value is false,
  # which causes the entire request to fail if any invalid rows exist.
  config :skip_invalid_rows, validate: :boolean, default: false

  # The following configuration options still exist to alert users that are using them
  config :uploader_interval_secs, validate: :number, deprecated: 'No longer used.'
  config :deleter_interval_secs, validate: :number, deprecated: 'No longer used.'
  config :key_path, validate: :string, obsolete: 'Use json_key_file or ADC instead.'
  config :key_password, validate: :string, deprecated: 'No longer needed with json_key_file or ADC.'
  config :service_account, validate: :string, deprecated: 'No longer needed with json_key_file or ADC.'
  config :temp_file_prefix, validate: :string, deprecated: 'No longer used.'
  config :temp_directory, validate: :string, deprecated: 'No longer used.'

  public

  def register
    @logger.debug('Registering plugin')

    @schema = LogStash::Outputs::BigQuery::Schema.parse_csv_or_json @csv_schema, @json_schema if @table_name.nil? || @table_name.empty?
    @bq_client = LogStash::Outputs::BigQuery::StreamingClient.new @json_key_file, @project_id, @logger
    @batcher = LogStash::Outputs::BigQuery::Batcher.new @batch_size, @batch_size_bytes
    @stopping = Concurrent::AtomicBoolean.new(false)

    init_batcher_flush_thread
  end

  # Method called for each log event. It writes the event to the current output
  # file, flushing depending on flush interval configuration.
  def receive(event)
    @logger.debug('BQ: receive method called', event: event)

    # Property names MUST NOT have @ in them
    message = replace_at_keys event.to_hash

    # Message must be written as json
    encoded_message = LogStash::Json.dump message

    @batcher.enqueue(encoded_message) { |batch| publish(batch) }
  end

  def get_table_name(time=nil)
    return @table_name unless @table_name.nil? || @table_name.empty?
    time ||= Time.now

    str_time = time.strftime(@date_pattern)
    table_id = @table_prefix + @table_separator + str_time

    # BQ does not accept anything other than alphanumeric and _
    # Ref: https://developers.google.com/bigquery/browser-tool-quickstart?hl=en
    table_id.tr!(':-', '_')

    table_id
  end

  # Remove @ symbols in hash keys
  def replace_at_keys(event)
    return event unless event.is_a? Hash

    out = {}

    event.each do |key, value|
      new_key = key.to_s.delete '@'
      out[new_key] = replace_at_keys value
    end

    out
  end

  # publish sends messages to a BigQuery table immediately
  def publish(messages)
    begin
      return if messages.nil? || messages.empty?

      table = get_table_name
      @logger.info("Publishing #{messages.length} messages to #{table}")

      create_table_if_not_exists table if @table_name.nil? || @table_name.empty?

      failed_rows = @bq_client.append(@dataset, table, messages, @ignore_unknown_values, @skip_invalid_rows)
      write_to_errors_file(failed_rows, table) unless failed_rows.empty?
    rescue StandardError => e
      @logger.error 'Error uploading data.', :exception => e

      write_to_errors_file(messages, table)
    end
  end

  def create_table_if_not_exists table
    begin
      return nil if @bq_client.table_exists? @dataset, table
      @bq_client.create_table(@dataset, table, @schema)

    rescue StandardError => e
      @logger.error 'Error creating table.', :exception => e
    end
  end

  def write_to_errors_file(messages, table)
    begin
      FileUtils.mkdir_p @error_directory

      t = Time.new
      error_file_name = "#{table}-#{t.to_i}.log"
      error_file_path = ::File.join(@error_directory, error_file_name)
      @logger.info "Problem data is being stored in: #{error_file_path}"

      File.open(error_file_path, 'w') do |f|
        messages.each { |message| f.puts message }
      end
    rescue StandardError => e
      @logger.error 'Error creating error file.', :exception => e, :messages => messages, :table => table
    end
  end

  def init_batcher_flush_thread
    @flush_thread = Thread.new do
      until stopping?
        Stud.stoppable_sleep(@flush_interval_secs) { stopping? }

        @batcher.enqueue(nil) { |batch| publish(batch) }
      end
    end
  end

  def stopping?
    @stopping.value
  end

  def close
    @stopping.make_true
    @flush_thread.wakeup
    @flush_thread.join
    # Final flush to publish any events published if a pipeline receives a shutdown signal after flush thread
    # has begun flushing.
    @batcher.enqueue(nil) { |batch| publish(batch) }
  end
end
