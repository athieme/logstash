#
# Copyright (c) 2013 Alex Thieme
#
require "logstash/namespace"
require "test_utils"

#
#
#
describe LogStash::Filters::LogService do
  extend LogStash::RSpec

  logger = Cabin::Channel.get(LogStash)
  host = "localhost"
  port = 9200
  util = LogStash::Util::LogServiceUtil.new(host, port, false)

  describe "generate events, routing them through filter, then into elastic search" do

    before(:each) do
      util.delete_all
    end

    after(:each) do
      #
    end

    #describe "first and only event" do
    #
    #  message = "hello world"
    #  count = 1
    #  type = "ion_log"
    #  index = "xmo_fluff"
    #
    #  config <<-CONFIG
    #  input {
    #    generator {
    #      message => "#{message}"
    #      count => #{count}
    #      type => "#{type}"
    #    }
    #  }
    #  filter {
    #    log_service {
    #      index => "_index"
    #      application => "xmo"
    #      domain => "fluff"
    #    }
    #  }
    #  output {
    #    elasticsearch_http {
    #      host => "#{host}"
    #      port => #{port}
    #      flush_size => 1
    #      index => "%{_index}"
    #    }
    #  }
    #  CONFIG
    #
    #  agent do
    #    util.flush()
    #    try_count(10, count, index, logger, util)
    #    try_documents(index, message, type, util)
    #  end
    #
    #end

    describe "first and second events" do

      message = "hello world"
      count = 2
      type = "ion_log"
      index = "xmo_fluff"

      config <<-CONFIG
      input {
        generator {
          message => "#{message}"
          count => #{count}
          type => "#{type}"
        }
      }
      filter {
        log_service {
          index => "_index"
          application => "xmo"
          domain => "fluff"
        }
      }
      output {
        elasticsearch_http {
          host => "#{host}"
          port => #{port}
          flush_size => 1
          index => "%{_index}"
          replication => "async"
        }
      }
      CONFIG

      agent do
        util.flush()
        try_count(10, count, index, logger, util)
        try_documents(index, message, type, util)
      end

    end


    #describe "first, second and third events" do
    #
    #  message = "hello world"
    #  count = 3
    #  type = "ion_log"
    #  index = "xmo_fluff"
    #
    #  config <<-CONFIG
    #  input {
    #    generator {
    #      message => "#{message}"
    #      count => #{count}
    #      type => "#{type}"
    #    }
    #  }
    #  filter {
    #    log_service {
    #      index => "_index"
    #      application => "xmo"
    #      domain => "fluff"
    #    }
    #  }
    #  output {
    #    elasticsearch_http {
    #      host => "#{host}"
    #      port => #{port}
    #      flush_size => 1
    #      index => "%{_index}"
    #    }
    #  }
    #  CONFIG
    #
    #  agent do
    #    util.flush()
    #    try_count(10, count, index, logger, util)
    #    try_documents(index, message, type, util)
    #  end
    #
    #end

    #describe "first, second, third and forth events" do
    #
    #  message = "hello world"
    #  count = 6
    #  type = "ion_log"
    #  index = "xmo_fluff"
    #
    #  config <<-CONFIG
    #  input {
    #    generator {
    #      message => "#{message}"
    #      count => #{count}
    #      type => "#{type}"
    #    }
    #  }
    #  filter {
    #    log_service {
    #      index => "_index"
    #      application => "xmo"
    #      domain => "fluff"
    #    }
    #  }
    #  output {
    #    elasticsearch_http {
    #      host => "#{host}"
    #      port => #{port}
    #      flush_size => 1
    #      index => "%{_index}"
    #    }
    #  }
    #  CONFIG
    #
    #  agent do
    #    util.flush()
    #    try_count(10, count, index, logger, util)
    #    try_documents(index, message, type, util)
    #  end
    #
    #end

  end

end

def try_count(times, count, index, logger, util)
  Stud::try(times.times) do
    result = util.index_count(index)
    logger.error("try_count", :result => result)
    insist { result["count"] } == count
  end
end

def try_documents(index, message, type, util)
  result = util.index_search(index)

  result["hits"]["hits"].each do |doc|
    insist { doc["_type"] } == type
    insist { doc["_source"]["message"] } == message
  end
end







