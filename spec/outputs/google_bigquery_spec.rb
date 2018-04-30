require "logstash/devutils/rspec/spec_helper"
require 'logstash/outputs/google_bigquery'
require 'logstash/outputs/bigquery/streamclient'

describe LogStash::Outputs::GoogleBigQuery do

  let(:config) { { 'project_id' => 'project', 'dataset' => 'dataset', 'csv_schema' => 'path:STRING,status:INTEGER,score:FLOAT' } }
  let(:sample_event) { LogStash::Event.new }
  let(:bq_client) { double('streaming-client') }
  let(:errors_file) { double('errors file') }

  subject { LogStash::Outputs::GoogleBigQuery.new(config) }

  before(:each) do
    allow(LogStash::Outputs::BigQuery::StreamingClient).to receive(:new).and_return(bq_client)
    expect(LogStash::Outputs::BigQuery::StreamingClient).to receive(:new)

    allow(subject).to receive(:init_batcher_flush_thread).and_return(nil)
    expect(subject).to receive(:init_batcher_flush_thread)

    subject.register

  end

  describe '#get_table_name' do
    it 'does not crash if no time is given' do
      subject.get_table_name
    end

    it 'formats the table name correctly' do
      table_id = subject.get_table_name Time.new(2012,9,8,7,6)
      expect(table_id).to eq('logstash_2012_09_08T07_00')
    end
  end

  describe '#replace_at_keys' do
    it 'removes @ in keys' do
      nested = {'@foo' => 'bar'}
      expected = {foo: 'bar'}

      out = subject.replace_at_keys nested

      expect(out).to eq(keys_to_strs(expected))
    end

    it 'does not remove @ in values' do
      nested = {foo: '@bar'}

      out = subject.replace_at_keys nested

      expect(out).to eq(keys_to_strs(nested))

    end

    it 'removes @ in nested keys' do
      nested = {foo: {'@bar' => 'bazz'}}
      expected = {foo: {bar: 'bazz'}}

      out = subject.replace_at_keys nested

      expect(out).to eq(keys_to_strs(expected))
    end
  end

  describe '#publish' do
    it 'does nothing if there are no messages' do
      allow(subject).to receive(:create_table_if_not_exists).and_return(nil)

      subject.publish nil
      subject.publish []

      expect(subject).not_to receive(:create_table_if_not_exists)
    end

    it 'creates a table if it does not exist' do
      allow(subject).to receive(:create_table_if_not_exists).and_return(nil)
      allow(bq_client).to receive(:append).and_return(true)
      allow(subject).to receive(:write_to_errors_file).and_return(nil)
      expect(subject).to receive(:create_table_if_not_exists)

      subject.publish ['{"foo":"bar"}']
    end

    it 'writes rows to a file on failed insert' do
      allow(subject).to receive(:create_table_if_not_exists).and_return(nil)
      allow(bq_client).to receive(:append).and_return(false)
      allow(subject).to receive(:write_to_errors_file).and_return(nil)
      expect(subject).to receive(:write_to_errors_file)

      subject.publish ['{"foo":"bar"}']
    end

    it 'writes rows to a file if insert threw an exception' do
      allow(subject).to receive(:create_table_if_not_exists).and_return(nil)
      allow(bq_client).to receive(:append).and_raise('expected insert error')
      allow(subject).to receive(:write_to_errors_file).and_return(nil)
      expect(subject).to receive(:write_to_errors_file)

      subject.publish ['{"foo":"bar"}']
    end
  end

  describe '#create_table_if_not_exists' do
    it 'checks if a table exists' do
      allow(bq_client).to receive(:table_exists?).and_return(true)
      expect(bq_client).to receive(:table_exists?)

      subject.create_table_if_not_exists 'foo'
    end

    it 'creates a table if it does not exist' do
      allow(bq_client).to receive(:table_exists?).and_return(false)
      allow(bq_client).to receive(:create_table).and_return(nil)
      expect(bq_client).to receive(:table_exists?)
      expect(bq_client).to receive(:create_table)

      subject.create_table_if_not_exists 'foo'
    end
  end

  describe '#write_to_errors_file' do
      it 'creates missing directories' do
        allow(File).to receive(:open).and_return(errors_file)
        allow(FileUtils).to receive(:mkdir_p)
        expect(FileUtils).to receive(:mkdir_p)

        subject.write_to_errors_file(['a','b'], 'table_name')
      end

      it 'does not fail on exception' do
        allow(FileUtils).to receive(:mkdir_p).and_raise("exception creating directories")
        expect{subject.write_to_errors_file([], 'table_name')}.to_not raise_error
      end
  end


  # converts tokens into strings recursively for a map.
  def keys_to_strs(event)
    return event unless event.is_a? Hash

    out = {}

    event.each do |key, value|
      out[key.to_s] = keys_to_strs value
    end

    out
  end

  RSpec::Matchers.define :starts_with do |x|
    match { |actual| actual.start_with? x}
  end

end
