## 4.1.6
 - Updated Google Cloud Storage client library [#67](https://github.com/logstash-plugins/logstash-output-google_bigquery/pull/67)

## 4.1.5
 - [DOC] Updated links to use shared attributes [#61](https://github.com/logstash-plugins/logstash-output-google_bigquery/pull/61)

## 4.1.4
 - Changed concurrency to :shared and publish outside of synchronized code [#60](https://github.com/logstash-plugins/logstash-output-google_bigquery/pull/60)

## 4.1.3
 - Fixed documentation issue where malformed asciidoc caused text to be lost [#53](https://github.com/logstash-plugins/logstash-output-google_bigquery/pull/53)

## 4.1.2
 - Fixed issue where Logstash shutdown could cause data loss due to not flushing buffers on close [#52](https://github.com/logstash-plugins/logstash-output-google_bigquery/pull/52)

## 4.1.1
 - Fixed inaccuracies in documentation [#46](https://github.com/logstash-plugins/logstash-output-google_bigquery/pull/46) 

## 4.1.0
 - Added `skip_invalid_rows` configuration which will insert all valid rows of a BigQuery insert
   and skip any invalid ones.
    - Fixes [#5](https://github.com/logstash-plugins/logstash-output-google_bigquery/issues/5)

## 4.0.1
 - Documentation cleanup

## 4.0.0

**Breaking**: the update to 4.0.0 requires that you use an IAM JSON credentials file
rather than the deprecated P12 files.
Applications using Application Default Credentials (ADC) _will_ continue to work.

This plugin now uses the BigQuery Streaming API which incurs an expense on upload.

 - The advantages of the streaming API are:
    - It allows real-time incoming data analysis and queries.
    - It allows Logstash instances to be started/stopped without worrying about failed batch jobs.
    - The client library has better support and performance.
 - New configuration options:
    - `batch_size` - The number of messages to upload at once.
    - `json_key_file` - The JSON IAM service account credentials to use with the plugin.
    - `batch_size_bytes` - The maximum number of bytes to upload as part of a batch (approximate).
 - Deprecated configurations:
    - `uploader_interval_secs` - No longer used
    - `deleter_interval_secs` - No longer used
    - `temp_file_prefix` - No longer used
    - `temp_directory` - No longer used
    - `key_password` - Use `json_key_file` or Application Default Credentials (ADC) instead.
    - `service_account` - Use `json_key_file` or Application Default Credentials (ADC) instead.
 - Obsolete configurations:
    - `key_path` - Use `json_key_file` or Application Default Credentials (ADC) instead.
      See [the documentation](https://www.elastic.co/guide/en/logstash/current/plugins-outputs-google_bigquery.html#plugins-outputs-google_bigquery-key_path)
      for help about moving to JSON key files.

## 3.2.4
  - Docs: Set the default_codec doc attribute.

## 3.2.3
  - Update gemspec summary

## 3.2.2
  - Fix some documentation issues

# 3.2.0
  - Add file recovery when plugin crashes

# 3.1.0
  - Fix error checking in the plugin to properly handle failed inserts

# 3.0.2
  - Docs: Fix doc formatting

# 3.0.1
  - Pin version of gems whose latest releases only work with ruby 2.x

## 3.0.0
  - Breaking: Updated plugin to use new Java Event APIs
  - relax contrains on logstash-core-plugin-api
  - mark this plugin as concurrency :single
  - update .travis.yml

## 2.0.5
  - Depend on logstash-core-plugin-api instead of logstash-core, removing the need to mass update plugins on major releases of logstash

## 2.0.4
  - New dependency requirements for logstash-core for the 5.0 release

## 2.0.3
 - Add support for specifying schema as a hash
 - Bubble up error message that BQ returns on an error
 - Add the table_separator option on bigquery output

## 2.0.0
 - Plugins were updated to follow the new shutdown semantic, this mainly allows Logstash to instruct input plugins to terminate gracefully, 
   instead of using Thread.raise on the plugins' threads. Ref: https://github.com/elastic/logstash/pull/3895
 - Dependency on logstash-core update to 2.0

