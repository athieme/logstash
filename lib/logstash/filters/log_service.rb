#
#
#

require "logstash/namespace"
require "logstash/filters/base"

class LogStash::Filters::LogService < LogStash::Filters::Base

  config_name "log_service"

  milestone 1

  config :host, :validate => :string, :default => "localhost"

  config :port, :validate => :number, :default => 9200

  config :index, :validate => :string, :default => "_index"

  config :application, :validate => :string

  config :domain, :validate => :string

  config :messages_per_slice, :validate => :number, :default => 3

  public
  def register

    @http = LogStash::Util::LogServiceHttpUtil.new

  end

  public
  def filter(event)

    return unless filter?(event)

    if @index and @application and @domain

      index_name = index_name(event)
      @logger.error("index name", :index_name => index_name)

      event[@index] = index_name

    else

      @logger.error("missing params", :index => @index, :application => @application, :domain => @domain)

    end

    filter_matched(event)
  end

  #
  # Determine status on the index and based on the number of documents
  # either use the current name, or create and use a new index name
  #
  def index_name(event)

    index_name, num_docs, write_alias, read_alias = index_status()
    @logger.error("status", :index_name => index_name, :num_docs => num_docs, :write_alias => write_alias, :read_alias => read_alias)

    if num_docs == 0

      @logger.error("index does not exist, create one", :index_name => index_name)

      create_index(index_name)

      create_index_aliases(index_name, write_alias, read_alias)

    elsif num_docs < @messages_per_slice

      @logger.error("less than, use the current index", :index_name => index_name)

    else

      @logger.error("greater than or equal to, create new index and aliases")

      slice = index_name.split('_')[0].to_i
      @logger.error("slice", :slice => slice)

      new_index_name = to_index_name(slice += 1, @application, @domain)
      @logger.error("index_name", :new_index_name => new_index_name)

      create_index(new_index_name)

      update_index_aliases(index_name, new_index_name, write_alias, read_alias)
    end

    write_alias

  end

  #
  # Determine the status of the current write index alias
  # Return the physical index name, number of documents
  # If the index does not exist, then return the default
  #
  def index_status()

    read_index_alias = to_read_index_alias_name()
    write_index_alias = to_write_index_alias_name()
    parsed = @http.get("http://#{@host}:#{@port}/#{write_index_alias}/_status")
    @logger.error("+++++ write index_status", :write_index_alias => write_index_alias, :parsed => parsed)

    if parsed.key?("ok")

      @logger.error("***** index_status ok")

      indices = parsed["indices"]
      index_name = indices.keys.first
      index = indices[index_name]
      docs = index["docs"]
      num_docs = docs["num_docs"]

      return index_name, num_docs, write_index_alias, read_index_alias

    else

      error = parsed["error"]
      @logger.error("***** index_status error", :error => error)

      index_name = to_index_name(0, @application, @domain)
      return index_name, 0, write_index_alias, read_index_alias

    end

  end

  #
  # Create the new index in Elastic Search
  #
  def create_index(index)

    parsed = @http.put("http://#{@host}:#{@port}/#{index}")
    ok = parsed["ok"]
    @logger.error("parsed???", :parsed => parsed)

    @logger.error("create_index ok", :ok => ok, :index => index)

    @http.get("http://#{@host}:#{@port}/_flush")

  end

  #
  # In a single request to Elastic Search:
  # Add the write index alias to the new index
  # Add the read index alias to the new index
  # Return the parsed JSON response
  #
  def create_index_aliases(new_index, write_alias, read_alias)
    body = {
        "actions" => [
            {"add" => {"index" => new_index, "alias" => write_alias}},
            {"add" => {"index" => new_index, "alias" => read_alias}}
        ]
    }

    parsed = @http.post("http://#{@host}:#{@port}/_aliases", body)
    ok = parsed["ok"]
    @logger.error("create index aliases ok", :ok => ok, :new_index => new_index, :write_alias => write_alias, :read_alias => read_alias)

    @http.get("http://#{@host}:#{@port}/_flush")

  end

  #
  # In a single request to Elastic Search:
  # Remove the write index alias to the old index
  # Add the write index alias to the new index
  # Add the read index alias to the new index
  # Return the parsed JSON response
  #
  def update_index_aliases(old_index, new_index, write_alias, read_alias)
    body = {
        "actions" => [
            {"remove" => {"index" => old_index, "alias" => write_alias}},
            {"add" => {"index" => new_index, "alias" => write_alias}},
            {"add" => {"index" => new_index, "alias" => read_alias}}
        ]
    }

    parsed = @http.post("http://#{@host}:#{@port}/_aliases", body)
    ok = parsed["ok"]
    @logger.error("update_index_aliases ok", :ok => ok)

    @http.get("http://#{@host}:#{@port}/_flush")

  end

  #
  # Construct an index name using the slice number, application and domain
  #
  def to_index_name(slice, application, domain)
    sprintf("%d_%s_%s", slice, application, domain)
  end

  #
  # Construct a write index alias name using the application and domain
  #
  def to_write_index_alias_name()
    sprintf("_w_%s_%s", @application, @domain)
  end

  #
  # Construct a read index alias name using the application and domain
  #
  def to_read_index_alias_name()
    sprintf("%s_%s", @application, @domain)
  end

end
