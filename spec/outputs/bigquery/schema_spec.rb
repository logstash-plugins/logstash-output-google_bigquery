require 'logstash/devutils/rspec/spec_helper'
require 'logstash/outputs/bigquery/schema'
require 'thread'
require 'java'

describe LogStash::Outputs::BigQuery::Schema do

  let(:simple_field) {keys_to_strs({'name':'foo', 'type':'STRING'})}
  let(:complex_field) {keys_to_strs({name: 'params', type: 'RECORD', mode: 'REPEATED', description: 'desc', fields: [simple_field]})}
  let(:example_field_list) {[simple_field, complex_field]}
  let(:example_schema) {keys_to_strs({fields:example_field_list})}

  describe '#parse_csv_or_json' do
    it 'ensures CSV and JSON are not both nil' do
      expect{LogStash::Outputs::BigQuery::Schema.parse_csv_or_json nil, nil}.to raise_error ArgumentError
    end

    it 'ensures CSV and JSON are not both defined' do
      expect{LogStash::Outputs::BigQuery::Schema.parse_csv_or_json "", {}}.to raise_error ArgumentError
    end

    it 'parses a CSV schema if it exists' do
      result = LogStash::Outputs::BigQuery::Schema.parse_csv_or_json "name:STRING", nil
      expect(result).to_not be_nil
    end

    it 'converts the resulting schema into a Java one' do
      result = LogStash::Outputs::BigQuery::Schema.parse_csv_or_json "name:STRING", nil
      expect(result.getClass().getName()).to eq('com.google.cloud.bigquery.Schema')
    end
  end

  describe '#parse_csv_schema' do
    it 'splits a CSV into name->type structures' do
      expected = {'fields' => [keys_to_strs({'name':'foo', 'type':'STRING'}), keys_to_strs({'name':'bar', 'type':'FLOAT'})]}
      result = LogStash::Outputs::BigQuery::Schema.parse_csv_schema "foo:STRING,bar:FLOAT"

      expect(result).to eq(keys_to_strs(expected))
    end

    it 'fails on a malformed CSV' do
      expect{LogStash::Outputs::BigQuery::Schema.parse_csv_schema "foo:bar:bazz"}.to raise_error ArgumentError
      expect{LogStash::Outputs::BigQuery::Schema.parse_csv_schema "foo:bar,,bar:bazz"}.to raise_error ArgumentError
      expect{LogStash::Outputs::BigQuery::Schema.parse_csv_schema "foo:bar,"}.to raise_error ArgumentError
    end
  end

  describe '#hash_to_java_schema' do
    subject{LogStash::Outputs::BigQuery::Schema.hash_to_java_schema(example_schema)}

    it 'parses the field list from the fields key' do
      expect(subject.getFields().size()).to eq(2)
    end

    it 'returns a BigQuery Schema object' do
      expect(subject.getClass().getName()).to eq('com.google.cloud.bigquery.Schema')
    end
  end

  describe '#parse_field_list' do
    subject{LogStash::Outputs::BigQuery::Schema.parse_field_list(example_field_list)}

    it 'returns a Java FieldList object' do
      expect(subject.getClass().getName()).to eq('com.google.cloud.bigquery.FieldList')
    end
  end

  describe '#parse_field' do
    subject{LogStash::Outputs::BigQuery::Schema.parse_field(complex_field)}

    it 'sets the correct name and type' do
      expect(subject.getName()).to eq('params')
      expect(subject.getType().toString()).to eq('RECORD')
    end

    it 'sets a description and mode if present' do
      expect(subject.getDescription()).to eq('desc')
    end
    
    it 'sets sub-fields if present' do
      expect(subject.getSubFields().size()).to eq(1)
    end

    it 'returns a Java Field object' do
      expect(subject.getClass().getName()).to eq('com.google.cloud.bigquery.Field')
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
end