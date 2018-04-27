require 'java'
require 'logstash-output-google_bigquery_jars.rb'
require 'logstash/json'

module LogStash
  module Outputs
    module BigQuery
      class Schema
        include_package 'com.google.cloud.bigquery'

        # Converts a CSV schema or JSON schema into a BigQuery Java Schema.
        def self.parse_csv_or_json(csv_schema, json_schema)
          csv_blank  = csv_schema.nil?  || csv_schema.empty?
          json_blank = json_schema.nil? || json_schema.empty?

          unless csv_blank ^ json_blank
            raise ArgumentError.new("You must provide either json_schema OR csv_schema. csv: #{csv_schema}, json: #{json_schema}")
          end

          if csv_blank
            schema = json_schema
          else
            schema = parse_csv_schema csv_schema
          end

          self.hash_to_java_schema schema
        end

        # Converts a CSV of field:type pairs into the JSON style schema.
        def self.parse_csv_schema(csv_schema)
          require 'csv'

          fields = []

          CSV.parse(csv_schema.gsub('\"', '""')).flatten.each do |field|
            raise ArgumentError.new('csv_schema must follow the format <field-name>:<field-type>') if field.nil?

            temp = field.strip.split(':')

            if temp.length != 2
              raise ArgumentError.new('csv_schema must follow the format <field-name>:<field-type>')
            end

            fields << { 'name' => temp[0], 'type' => temp[1] }
          end

          # Check that we have at least one field in the schema
          raise ArgumentError.new('csv_schema must contain at least one field') if fields.empty?

          { 'fields' => fields }
        end

        # Converts the Ruby hash style schema into a BigQuery Java schema
        def self.hash_to_java_schema(schema_hash)
          field_list = self.parse_field_list schema_hash['fields']
          com.google.cloud.bigquery.Schema.of field_list
        end

        # Converts a list of fields into a BigQuery Java FieldList
        def self.parse_field_list(fields)
          fieldslist = fields.map {|field| self.parse_field field}

          FieldList.of fieldslist
        end

        # Converts a single field definition into a BigQuery Java Field object.
        # This includes any nested fields as well.
        def self.parse_field(field)
          type = LegacySQLTypeName.valueOfStrict(field['type'])
          name = field['name']

          if field.has_key? 'fields'
            sub_fields = self.parse_field_list field['fields']
            builder = Field.newBuilder(name, type, sub_fields)
          else
            builder = Field.newBuilder(name, type)
          end

          if field.has_key? 'description'
            builder = builder.setDescription(field['description'])
          end

          if field.has_key? 'mode'
            mode = Field::Mode.valueOf field['mode']
            builder = builder.setMode(mode)
          end

          builder.build
        end
      end
    end
  end
end