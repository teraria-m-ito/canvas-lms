# frozen_string_literal: true

# Live Events を Kinesis の代わりに Kafka へ送るクライアント。
# LiveEvents::AsyncWorker から Aws::Kinesis::Client と同じ put_records で呼ばれるため、
# 戻り値も PutRecordsOutput 形式で返す（失敗したレコードは error_code を付けるとワーカーが再送する）。
#
# live_events.yml の設定例:
#   kafka_brokers: kafka:9092
#   kafka_topic: canvas-live-events
#   kafka_config:            # 任意。librdkafka の設定をそのまま渡す
#     security.protocol: plaintext
module Canvas
  class LiveEventsKafkaClient
    DEFAULT_TOPIC = "canvas-live-events"
    DELIVERY_TIMEOUT_SECONDS = 10

    attr_reader :stream_name

    def initialize(settings)
      @stream_name = settings["kafka_topic"].presence || DEFAULT_TOPIC
      @config = {
        "bootstrap.servers" => settings["kafka_brokers"],
        "client.id" => "canvas-live-events",
        "acks" => "all",
        "enable.idempotence" => true
      }.merge(settings["kafka_config"] || {}).transform_keys(&:to_sym)
    end

    def put_records(records:, stream_name:)
      handles = records.map do |record|
        producer.produce(topic: stream_name, payload: record[:data], key: record[:partition_key])
      rescue Rdkafka::BaseError => e
        e
      end

      entries = handles.map do |handle|
        raise handle if handle.is_a?(Exception)

        handle.wait(max_wait_timeout: DELIVERY_TIMEOUT_SECONDS)
        Aws::Kinesis::Types::PutRecordsResultEntry.new
      rescue Rdkafka::BaseError, Rdkafka::AbstractHandle::WaitTimeoutError => e
        Aws::Kinesis::Types::PutRecordsResultEntry.new(error_code: e.class.name, error_message: e.message)
      end
      Aws::Kinesis::Types::PutRecordsOutput.new(records: entries)
    end

    private

    # Passenger / delayed_job は fork するため、プロセスごとにプロデューサを作り直す
    def producer
      return @producer if @producer && @producer_pid == Process.pid

      require "rdkafka"
      @producer_pid = Process.pid
      @producer = Rdkafka::Config.new(@config).producer
    end
  end
end
