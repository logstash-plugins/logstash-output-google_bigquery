# encoding: utf-8

require 'logstash/outputs/bigquery/streamclient'

describe LogStash::Outputs::BigQuery::StreamingClient do

  # This test is mostly to make sure the Java types, signatures and classes
  # haven't changed being that JRuby is very relaxed.
  describe '#initialize' do
    let(:logger) { spy('logger') }

    it 'does not throw an error when initializing' do
      key_file = ::File.join('spec', 'fixtures', 'credentials.json')
      key_file = ::File.absolute_path(key_file)
      LogStash::Outputs::BigQuery::StreamingClient.new(key_file, 'my-project', logger)
    end
  end
end
