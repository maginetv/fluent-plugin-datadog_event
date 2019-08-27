require 'fluent/plugin/output'

module Fluent::Plugin
  class OutDatadogEvent < Output
    Fluent::Plugin.register_output('datadog_event', self)

    unless method_defined?(:log)
        define_method("log") { $log }
    end

    helpers :compat_parameters

    config_param :host, :string, :default => nil
    config_param :api_key, :string
    config_param :app_key, :string, :default => nil
    config_param :msg_title, :string, :default => 'fluentd Datadog Event'
    config_param :priority, :string, :default => nil
    config_param :tags, :string, :default => nil
    config_param :alert_type, :string, :default => nil
    config_param :aggregation_key, :string, :default => nil
    config_param :source_type_name, :string, :default => 'fluentd'

    config_section :buffer do
        config_set_default :@type, "memory"
        config_set_default :flush_mode, :immediate
        config_set_default :chunk_keys, ["tag"]
    end

    def configure(conf)
        super

        compat_parameters_convert(conf, :buffer)
        raise Fluent::ConfigError, "'tag' in chunk_keys is required." if not @chunk_key_tag
    end

    def initialize
        super

        require "dogapi"
        require "date"
    end

    def start
        super

        @dog = Dogapi::Client.new(@api_key, @app_key)
    end

    def write(chunk)
        msg_title = extract_placeholders(@msg_title, chunk)
        tags = extract_placeholders(@tags, chunk)
        chunk.each do |time, record|
            post_event(time, "record", record, msg_title, tags)
        end
    end

    def post_event(time, event_key, record, msg_title, tags)
        host = @host
        if !host
            host = record["host"]
        end

        # If log_level field exists, map that to the alert_type
        # otherwise default it to info. If alert_type was set to success
        # ignore log_level logic.
        alert_type = @alert_type
        if alert_type != "success"
            log_level = record["log_level"]
            unless log_level.nil?
                log_level = log_level.downcase
                case log_level
                when "error"
                    alert_type = "error"
                when "info"
                    alert_type = "info"
                when "warn"
                    alert_type = "warning"
                when "warning"
                    alert_type = "warning"
                when "severe"
                    alert_type = "error"
                else
                    alert_type = "info"
                end
            end
        end

        res = @dog.emit_event(Dogapi::Event.new(
            "#{record}",
            :msg_title => msg_title,
            :date_happend => time,
            :priority => @priority,
            :host => host,
            :tags => tags,
            :alert_type => alert_type,
            :aggregation_key => @aggregation_key,
            :source_type_name => @source_type_name
        ))
        end
    end
end
