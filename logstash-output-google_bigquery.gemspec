Gem::Specification.new do |s|
  s.name            = 'logstash-output-google_bigquery'
  s.version         = '3.0.1'
  s.licenses        = ['Apache License (2.0)']
  s.summary         = "Plugin to upload log events to Google BigQuery (BQ)"
  s.description     = "This gem is a Logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/logstash-plugin install gemname. This gem is not a stand-alone program"
  s.authors         = ["Elastic"]
  s.email           = 'info@elastic.co'
  s.homepage        = "http://www.elastic.co/guide/en/logstash/current/index.html"
  s.require_paths = ["lib"]

  # Files
  s.files = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']

  # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "output" }

  # Gem dependencies
  s.add_runtime_dependency 'google-api-client', '~> 0.8.7' # version 0.9.x works only with ruby 2.x
  s.add_runtime_dependency 'logstash-codec-plain'
  s.add_runtime_dependency 'mime-types', '~> 2' # last version compatible with ruby 2.x
  s.add_runtime_dependency "logstash-core-plugin-api", ">= 1.60", "<= 2.99"

  s.add_development_dependency 'logstash-devutils'
end

