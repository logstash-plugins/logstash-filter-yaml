require "logstash/devutils/rspec/spec_helper"
require "logstash/filters/yaml"
require "logstash/timestamp"

describe LogStash::Filters::Yaml do

  describe "parse message into the event" do
    config <<-CONFIG
      filter {
        yaml {
          # Parse message as YAML
          source => "message"
        }
      }
    CONFIG

    sample '{ hello: "world", list: [ 1, 2, 3 ], hash: { k: v }, sometime: "2013-10-19T00:14:32.996Z" }' do
      insist { subject["hello"] } == "world"
      insist { subject["list" ].to_a } == [1,2,3] # to_a for JRuby + JrJacksom which creates Java ArrayList
      insist { subject["hash"] } == { "k" => "v" }
    end
  end

  describe "parse message into a target field" do
    config <<-CONFIG
      filter {
        yaml {
          # Parse message as YAML, store the results in the 'data' field'
          source => "message"
          target => "data"
        }
      }
    CONFIG

    sample '{ hello: "world", list: [ 1, 2, 3 ], "hash": { k: v } }' do
      insist { subject["data"]["hello"] } == "world"
      insist { subject["data"]["list" ].to_a } == [1,2,3] # to_a for JRuby + JrJacksom which creates Java ArrayList
      insist { subject["data"]["hash"] } == { "k" => "v" }
    end
  end

  describe "tag invalid yaml" do
    config <<-CONFIG
      filter {
        yaml {
          # Parse message as YAML, store the results in the 'data' field'
          source => "message"
          target => "data"
        }
      }
    CONFIG

    sample "invalid yaml" do
      insist { subject["tags"] }.include?("_yamlparsefailure")
    end
  end

  describe "testing @timestamp" do
    config <<-CONFIG
      filter {
        yaml {
          source => "message"
        }
      }
    CONFIG

    sample "{ \"@timestamp\": \"2013-10-19T00:14:32.996Z\" }" do
      insist { subject["@timestamp"] }.is_a?(LogStash::Timestamp)
      insist { YAML::dump(subject["@timestamp"]) } == "--- !ruby/object:LogStash::Timestamp\ntime: 2013-10-19 00:14:32.996000000 Z\n"
    end
  end

  describe "source == target" do
    config <<-CONFIG
      filter {
        yaml {
          source => "example"
          target => "example"
        }
      }
    CONFIG

    sample({ "example" => "{ hello: world }" }) do
      insist { subject["example"] }.is_a?(Hash)
      insist { subject["example"]["hello"] } == "world"
    end
  end

end
