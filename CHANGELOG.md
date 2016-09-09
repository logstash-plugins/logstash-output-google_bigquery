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

