require "logstash-core"
require "logstash/devutils/rspec/spec_helper"
require "logstash/filters/yaml"
require "logstash/timestamp"

describe LogStash::Filters::Yaml do
  subject { described_class.new(config) }
  let(:event) { LogStash::Event.new(data) }

  describe "parse message into the event" do
    let(:config) { { "source" => "message" } }
    let(:data) { { "message" => '{ hello: "world", list: [ 1, 2, 3 ], hash: { k: v }, sometime: "2013-10-19T00:14:32.996Z" }' } }

    it "parses correctly" do
      subject.filter(event)
      expect(event.get("hello")).to eq("world")
      expect(event.get("list" ).to_a).to eq([1,2,3]) # to_a for JRuby + JrJacksom which creates Java ArrayList
      expect(event.get("hash")).to eq({ "k" => "v" })
    end
  end

  describe "parse message into a target field" do
    let(:config) { { "source" => "message", "target" => "data" } }
    let(:data) { { "message" => '{ hello: "world", list: [ 1, 2, 3 ], "hash": { k: v } }' } }

    it "parses correctly" do
      subject.filter(event)
      expect(event.get("data")["hello"]).to eq("world")
      expect(event.get("data")["list" ].to_a).to eq([1,2,3]) # to_a for JRuby + JrJacksom which creates Java ArrayList
      expect(event.get("data")["hash"]).to eq({ "k" => "v" })
    end
  end

  describe "tag invalid yaml" do
    # Parse message as YAML, store the results in the 'data' field'
    let(:config) { { "source" => "message", "target" => "data" } }
    let(:data) { { "message" => "'" } }

    it "tags the failure to parse" do
      subject.filter(event)
      expect(event.get("tags")).to include("_yamlparsefailure")
    end
  end

  describe "testing @timestamp" do
    let(:config) { { "source" => "message" } }
    let(:date) { "2013-10-19T00:14:32.996Z" }
    let(:data) { { "message" => "{ \"@timestamp\": \"#{date}\" }"  } }
    it "parses correctly" do
      subject.filter(event)
      expect(event.timestamp).to be_a(LogStash::Timestamp)
      expect(event.timestamp.to_s).to eq(date)
    end
  end

  describe "source == target" do
    let(:config) { { "source" => "example", "target" => "example" } }
    let(:data) { { "example" => "{ hello: world }" } }

    it "parses correctly" do
      subject.filter(event)
      expect(event.get("example")).to be_a(Hash)
      expect(event.get("example")["hello"]).to eq("world")
    end
  end
end
