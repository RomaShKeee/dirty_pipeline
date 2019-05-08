module DirtyPipeline
  # Storage structure
  # {
  #   dp_status: :errored,
  #   dp_state: {
  #     field: "value",
  #   },
  #   errors: {
  #     "<event_id>": {
  #       error: "RuPost::API::Error",
  #       error_message: "Timeout error",
  #       created_at: 2018-01-01T13:22Z
  #     },
  #   },
  #   dp_events: {
  #     <event_id>: {
  #       transition: "Create",
  #       args: ...,
  #       changes: ...,
  #       created_at: ...,
  #       updated_at: ...,
  #       attempts_count: 2,
  #     },
  #     <event_id>: {...},
  #   }
  # }
  module Redis
    class Storage
      class InvalidPipelineStorage < StandardError; end

      attr_reader :subject, :field, :store
      alias :to_h :store
      def initialize(subject, field)
        @subject = subject
        @field = field
        @store = subject.send(@field).to_h
        reset if @store.empty?
        raise InvalidPipelineStorage, store unless valid_store?
      end

      def reset!
        reset
        save!
      end

      def status
        store["dp_status"]
      end

      def commit!(event)
        store["dp_status"] = event.destination     if event.success?
        store["dp_state"].merge!(event.changes)    unless event.changes.to_h.empty?

        error = {}
        error = event.error.to_h unless event.error.to_h.empty?
        store["dp_errors"][event.id] = error

        data = {}
        data = event.data.to_h unless event.data.to_h.empty?
        store["dp_events"][event.id] = data
        save!
      end

      def find_event(event_id)
        return unless (found_event = store.dig("dp_events", event_id))
        Event.new(data: found_event, error: store.dig("dp_errors", event_id))
      end

      private

      def valid_store?
        (store.keys & %w(dp_status dp_events dp_errors dp_state)).size.eql?(4)
      end

      # FIXME: save! - configurable method
      def save!
        subject.send("#{field}=", store)
        subject.save!
      end

      def reset
        @store = subject.send(
          "#{field}=",
          {
            "dp_status" => nil,
            "dp_state" => {},
            "dp_events" => {},
            "dp_errors" => {}
          }
        )
      end
    end
  end
end
