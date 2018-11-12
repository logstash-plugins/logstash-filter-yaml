# encoding: utf-8
require "logstash-core"
require "logstash/filters/base"
require "logstash/namespace"
require "logstash/timestamp"

# This is a YAML parsing filter. It takes an existing field which contains YAML and
# expands it into an actual data structure within the Logstash event.
#
# By default it will place the parsed YAML in the root (top level) of the Logstash event, but this
# filter can be configured to place the YAML into any arbitrary event field, using the
# `target` configuration.
class LogStash::Filters::Yaml < LogStash::Filters::Base

  config_name "yaml"

  # The configuration for the YAML filter:
  # [source,ruby]
  #     source => source_field
  #
  # For example, if you have YAML data in the @message field:
  # [source,ruby]
  #     filter {
  #       yaml {
  #         source => "message"
  #       }
  #     }
  #
  # The above would parse the yaml from the @message field
  config :source, :validate => :string, :required => true

  # Define the target field for placing the parsed data. If this setting is
  # omitted, the YAML data will be stored at the root (top level) of the event.
  #
  # For example, if you want the data to be put in the `doc` field:
  # [source,ruby]
  #     filter {
  #       yaml {
  #         target => "doc"
  #       }
  #     }
  #
  # YAML in the value of the `source` field will be expanded into a
  # data structure in the `target` field.
  #
  # NOTE: if the `target` field already exists, it will be overwritten!
  config :target, :validate => :string

  public
  def register
    require 'yaml'
  end # def register

  public
  def filter(event)
    return unless filter?(event)

    @logger.debug("Running yaml filter", :event => event)

    return unless event.include?(@source)

    source = event.get(@source)

    begin
      parsed = YAML::load(source)
    rescue => e
      event.tag("_yamlparsefailure")
      @logger.warn("Error parsing yaml", :source => @source,
                   :raw => event.get(@source), :exception => e.message)
      return
    end

    if @target
      event.set(@target, parsed)
    else
      unless parsed.is_a?(Hash)
        event.tag("_yamlparsefailure")
        @logger.warn("Parsed YAML object/hash requires a target configuration option", :source => @source, :raw => source)
        return
      end

      # The following logic was copied from Json filter
      # a) since the parsed hash will be set in the event root, first extract any @timestamp field to properly initialized it
      parsed_timestamp = parsed.delete(LogStash::Event::TIMESTAMP)
      begin
        timestamp = parsed_timestamp ? LogStash::Timestamp.coerce(parsed_timestamp) : nil
      rescue LogStash::TimestampParserError => e
        timestamp = nil
      end

      # b) then set all parsed fields in the event
      parsed.each{|k, v| event.set(k, v)}

      # c) finally re-inject proper @timestamp
      if parsed_timestamp
        if timestamp
          event.timestamp = timestamp
        else
          event.timestamp = LogStash::Timestamp.new
          @logger.warn("Unrecognized #{LogStash::Event::TIMESTAMP} value, setting current time to #{LogStash::Event::TIMESTAMP}, original in #{LogStash::Event::TIMESTAMP_FAILURE_FIELD} field", :value => parsed_timestamp.inspect)
          event.tag(LogStash::Event::TIMESTAMP_FAILURE_TAG)
          event.set(LogStash::Event::TIMESTAMP_FAILURE_FIELD, parsed_timestamp.to_s)
        end
      end
    end

    filter_matched(event)
    @logger.debug? && @logger.debug("Event after yaml filter", :event => event.inspect)
  end # def filter
end # class LogStash::Filters::Yaml
