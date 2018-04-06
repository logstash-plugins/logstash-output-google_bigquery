# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require 'logstash/outputs/google_bigquery'

describe LogStash::Outputs::GoogleBigQuery do

  let(:config) { { 'project_id' => 'project', 'topic' => 'topic' } }
  let(:sample_event) { LogStash::Event.new }
  let(:output) { LogStash::Outputs::GoogleBigQuery.new(config) }
  let(:streamingclient) { double('streaming-client')}

  before do
    output.register
  end

  before(:each) do
    allow(LogStash::Outputs::BigQuery::StreamingClient).to receive(:new).and_return(:streamingclient)
  end

  # describe '#recieve' do
  #   it 'removes @ in keys' do
  #     allow(:output).to receive()
  #   end
  #
  #   it 'converts batches to JSON' do
  #
  #   end
  #
  #   it 'publishes if batcher returns a list' do
  #
  #   end
  # end

  describe '#get_table_name' do
    it 'does not crash if no time is given' do
      output.get_table_name
    end

    it 'formats the table name correctly' do
      table_id = output.get_table_name Time.new(2012,9,8,7,6)
      expect(table_id).to eq('logstash_2012_09_08T_07:00')
    end
  end

  describe '#replace_at_keys' do
    it 'does not change the structure of an object' do

    end

    it 'removes @ in keys' do
      nested = {'@foo': 'bar'}
      expected = {'foo': 'bar'}

      out = output.replace_at_keys nested

      expect(out).to eq(expected)
    end

    it 'does not remove @ in values' do
      nested = {'foo': '@bar'}

      out = output.replace_at_keys nested

      expect(out).to eq(nested)

    end

    it 'removes @ in nested keys' do
      nested = {'foo': {'@bar': 'bazz'}}
      expected = {'foo': {'bar': 'bazz'}}

      out = output.replace_at_keys nested

      expect(out).to eq(expected)
    end
  end

  describe '#publish' do
    it 'does nothing if there are no messages' do

    end

    it 'creates a table if it does not exist' do

    end

    it 'writes rows to a file on failed insert' do

    end

    it 'writes rows to a file if insert threw an exception' do

    end
  end

  describe '#create_table_if_not_exists' do
    it 'checks if a table exists' do

    end

    it 'creates a table if it does not exist' do

    end

    it 'catches all exceptions' do

    end
  end

  describe '#write_to_errors_file' do
    it 'uses the correct file name' do

    end

    it 'does not fail on exception' do

    end
  end
end
