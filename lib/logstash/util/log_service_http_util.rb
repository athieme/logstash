#
# Copyright (c) 2013 Alex Thieme
#
require "logstash/namespace"
require "logstash/util"
require "ftw"

#
# HTTP utility class
#
class LogStash::Util::LogServiceHttpUtil

  public
  def initialize(param=nil)
    @logger = Cabin::Channel.get(LogStash)
    @http = FTW::Agent.new()
  end

  #
  # Make an HTTP GET
  # Rturn the parsed JSON response body
  #
  def get(url)
    #@logger.error("get", :url => url)

    begin
      response = @http.get!(url)
    rescue EOFError
      @logger.error("EOF while reading response header", :url => url)
      return
    end

    body = ""
    begin
      response.read_body { |chunk| body += chunk }
    rescue EOFError
      @logger.error("EOF while reading response body", :url => url)
      return
    end

    parsed = JSON.parse(body)

  end

  #
  # Make and HTTP PUT
  # Return the parsed JSON response body
  #
  def put(url)

    #@logger.error("put", :url => url)

    begin
      response = @http.put!(url)
    rescue EOFError
      @logger.error("EOF while reading response header", :url => url)
      return
    end

    body = ""
    begin
      response.read_body { |chunk| body += chunk }
    rescue EOFError
      @logger.error("EOF while reading response body", :url => url)
      return
    end

    parsed = JSON.parse(body)

  end

  #
  # Make and HTTP POST
  # Return the parsed JSON response body
  #
  def post(url, body)

    #@logger.error("post", :url => url)

    begin
      response = @http.post!(url, :body => body.to_json)
    rescue EOFError
      @logger.error("EOF while reading response header", :url => url)
      return
    end

    body = ""
    begin
      response.read_body { |chunk| body += chunk }
    rescue EOFError
      @logger.error("EOF while reading response body", :url => url)
      return
    end

    parsed = JSON.parse(body)

  end

  #
  # Make an HTTP DELETE
  # Return the parsed JSON response body
  #
  def delete(url)

    #@logger.error("delete", :url => url)

    body = ""
    response = @http.delete!(url)
    response.read_body { |chunk| body << chunk }
    result = JSON.parse(body)
  end

end

