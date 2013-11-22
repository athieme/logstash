#
#
#
require "logstash/namespace"
require "test_utils"

describe LogStash::Filters::LogService do
  extend LogStash::RSpec

  util = LogStash::Util::LogServiceUtil.new("localhost", 9200, true)

  describe "testing filter" do

    before(:each) do
      util.delete_all
    end

    after(:each) do
      #
    end

    describe "replace default index field with initial index name" do

      config <<-CONFIG
      filter {
        log_service {
          index => "_index"
          application => "xmo"
          domain => "fluff"
        }
      }
      CONFIG

      sample("this is my event") do
        insist { subject["_index"] } == "_w_xmo_fluff"
      end

    end

    describe "replace alternate index field with initial index name" do

      config <<-CONFIG
      filter {
        log_service {
          index => "foo"
          application => "xmo"
          domain => "fluff"
        }
      }
      CONFIG

      sample("this is my event") do
        insist { subject["foo"] } == "_w_xmo_fluff"
      end
    end

  end

end




