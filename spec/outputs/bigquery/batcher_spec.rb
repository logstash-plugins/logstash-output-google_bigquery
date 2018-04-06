require 'logstash/outputs/pubsub/batcher'
require 'thread'

describe LogStash::Outputs::BigQuery::Batcher do
  let(:logger) { spy(:logger) }
  let(:batcher) { LogStash::Outputs::BigQuery::Batcher.new(2, 1_000) }
  let(:one_b_message) { 'a' }
  let(:one_k_message) { 'a' * 1000 }
  let(:batch_queue) { Queue.new }

  describe '#enqueue' do
    it 'returns nil if no reason to flush' do
      batch = batcher.enqueue one_b_message

      expect(batch).to be_nil
    end

    it 'returns a batch if passed nil' do
      batch = batcher.enqueue nil

      expect(batch).to_not be_nil
    end

    it 'returns a batch if the message count overflows' do
      batch = batcher.enqueue one_b_message
      expect(batch).to be_nil

      batch = batcher.enqueue one_b_message
      expect(batch).to_not be_nil
    end

    it 'returns a batch if the message size overflows' do
      batch = batcher.enqueue one_b_message
      expect(batch).to be_nil

      batch = batcher.enqueue one_k_message
      expect(batch).to_not be_nil
    end

    it 'clears internal state after returning a batch' do
      batch = batcher.enqueue one_k_message

      expect(batch).to_not be_nil
      expect(batcher.empty?).to be_truthy
    end

    it 'does not yield a batch if there is no reason to flush' do
      batch = nil
      batcher.enqueue(one_b_message) { |b| batch = b }

      expect(batch).to be_nil
    end

    it 'yields a batch on flush' do
      batch = nil
      batcher.enqueue(nil) { |b| batch = b }

      expect(batch).to_not be_nil
      expect(batch.length).to eq 0
    end

    it 'yields a batch on overflow' do
      batch = nil
      batcher.enqueue(one_k_message) { |b| batch = b }

      expect(batch).to_not be_nil
      expect(batch.length).to eq 1
    end
  end

  describe '#enqueue_push' do
    it 'enqueues nothing nil if no reason to flush' do
      batcher.enqueue_push one_b_message, batch_queue

      expect(batch_queue.length).to eq 0
    end

    it 'enqueues a batch if passed nil' do
      batcher.enqueue_push nil, batch_queue

      expect(batch_queue.length).to eq 1
    end

    it 'enqueues a batch if the message count overflows' do
      batcher.enqueue_push one_b_message, batch_queue
      expect(batch_queue.length).to eq 0

      batcher.enqueue_push one_b_message, batch_queue
      expect(batch_queue.length).to eq 1
    end

    it 'enqueues a batch if the message size overflows' do
      batcher.enqueue_push one_b_message, batch_queue
      expect(batch_queue.length).to eq 0

      batcher.enqueue_push one_k_message, batch_queue
      expect(batch_queue.length).to eq 1
    end
  end

  describe '#clear' do
    it 'removes any existing messages' do
      batcher.enqueue one_b_message
      expect(batcher.empty?).to be_falsey

      batcher.clear
      expect(batcher.empty?).to be_truthy
    end
  end
end
