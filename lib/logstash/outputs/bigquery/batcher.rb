require 'thread'
require 'java'
require 'logstash-output-google_bigquery_jars.rb'

module LogStash
  module Outputs
    module BigQuery
      # Batcher is a queue that bundles messages in batches based on their
      # size in bytes or count. It's used to provide guarantees around
      # maximum data loss due to a fault while maintaining good upload
      # throughput.
      class Batcher
        include_package 'java.util.concurrent.locks'

        def initialize(max_length, max_bytes)
          @lock = ReentrantReadWriteLock.new
          @max_length = max_length
          @max_bytes = max_bytes

          clear
        end

        # enqueue_push calls enqueue and if a batch is ready to go pushes it to
        # the provided queue.
        def enqueue_push(message, queue)
          batch = enqueue message

          queue << batch unless batch.nil?
        end

        # enqueue adds a message to the batch. If the batch is ready to be sent
        # out the internal state is reset and the array of messages is both
        # yielded and returned.
        # Otherwise nil is returned.
        def enqueue(message)
          @lock.write_lock.lock

          begin
            is_flush_request = message.nil?

            unless is_flush_request
              @batch_size_bytes += message.length
              @batch << message
            end

            length_met = @batch.length >= @max_length
            size_met = @batch_size_bytes >= @max_bytes

            if is_flush_request || length_met || size_met
              orig = @batch
              clear

              yield(orig) if block_given?
              return orig
            end

            nil
          ensure
            @lock.write_lock.unlock
          end
        end

        # removes all elements from the batch
        def clear
          @lock.write_lock.lock
          @batch = []
          @batch_size_bytes = 0
          @lock.write_lock.unlock
        end

        def empty?
          @lock.read_lock.lock
          begin
            @batch.empty? && @batch_size_bytes.zero?
          ensure
            @lock.read_lock.unlock
          end
        end
      end
    end
  end
end
