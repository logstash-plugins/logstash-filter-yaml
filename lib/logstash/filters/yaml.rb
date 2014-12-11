# encoding: utf-8
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
  milestone 1

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

    # TODO(colin) this field merging stuff below should be handled in Event.

    source = event[@source]
    if @target.nil?
      # Default is to write to the root of the event.
      dest = event.to_hash
    else
      if @target == @source
        # Overwrite source
        dest = event[@target] = {}
      else
        dest = event[@target] ||= {}
      end
    end

    begin
      dest.merge!(YAML::load(source))

      # If no target, we target the root of the event object. This can allow
      # you to overwrite @timestamp and this will typically happen for yaml
      # LogStash Event deserialized here.
      if !@target && event.timestamp.is_a?(String)
        event.timestamp = LogStash::Timestamp.parse_iso8601(event.timestamp)
      end

      filter_matched(event)
    rescue => e
      event.tag("_yamlparsefailure")
      @logger.warn("Trouble parsing yaml", :source => @source,
                   :raw => event[@source], :exception => e)
      return
    end

    @logger.debug("Event after yaml filter", :event => event)

  end # def filter

end # class LogStash::Filters::Yaml
