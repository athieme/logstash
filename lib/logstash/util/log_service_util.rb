#
#
#
require "logstash/namespace"
require "logstash/util"

class LogStash::Util::LogServiceUtil

  public
  def initialize(host, port, start)
    @logger = Cabin::Channel.get(LogStash)
    @host = host
    @port = port
    @http = LogStash::Util::LogServiceHttpUtil.new()
    if start
      builder = org.elasticsearch.node.NodeBuilder.nodeBuilder
      builder.settings.put("http.port", @port) # "9200-9300"
      @elasticsearch = builder.node

      @elasticsearch.start()

    end
  end

  def delete_all()
    response = @http.delete("http://#{@host}:#{@port}/")
    @logger.error("delete_all", :response => response)
  end

  def flush()
    response = @http.get("http://#{@host}:#{@port}/_flush")
  end

  def index_flush(index)
    response = @http.get("http://#{@host}:#{@port}/#{index}/_flush")
  end

  def index_count(index)
    response = @http.get("http://#{@host}:#{@port}/#{index}/_count?q=*")
  end

  def index_search(index)
    response = @http.get("http://#{@host}:#{@port}/#{index}/_search?q=*&size=1000")
  end

end

