#!/usr/bin/env ruby

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

require 'bunny'
require 'stringio'
require 'timeout'
require 'uuidtools'

module Thrift

  class ResponseTimeout < Timeout::Error; end

  class AmqpRpcClientTransport < BaseTransport

    def initialize(service_queue_name, opts={})
      @service_queue_name = service_queue_name
      @outbuf = Bytes.empty_byte_buffer

      if opts[:connection].nil?
        if opts[:host].nil?
          raise ArgumentError, ":host key not provided in opts dict to make connection"
        end

        if opts[:port].nil?
          raise ArgumentError, ":port key not provided in opts dict to make connection"
        end

        vhost = opts[:vhost] || "/"
        user = opts[:user] || "guest"
        password = opts[:password] || "guest"
        ssl = opts[:ssl] || false

        @conn = Bunny.new(:host => opts[:host], :port => opts[:port], :vhost => vhost, :user => user, :password => password, :ssl=> ssl)

        @conn.start
        @connection_started = true
      else
        @conn = opts[:connection]
        @connection_started = false
      end

      @from_name                 = opts[:from_name].nil? ? "Unknown Client" : opts[:from_name]
      @exchange                  = opts[:exchange] || nil

      @ch                        = @conn.create_channel
      @service_exchange          = @exchange.nil? ? @ch.default_exchange : @ch.direct(@exchange, :durable => true)
      @service_response_exchange = @ch.default_exchange
      @reply_queue               = @ch.queue("", :exclusive => true)
      @is_opened                 = true

    end

    def close
      if @is_opened
        @reply_queue.delete
        @ch.close

        if @connection_started
          @conn.close
          @connection_started = false
        end

        @is_opened = false
      end
    end

    def open?; @is_opened end
    def read(sz); @inbuf.read sz end
    def write(buf); @outbuf << Bytes.force_binary_encoding(buf) end

    #If blocking is set to true then wait for a response message in the reply_to queue, otherwise
    #just send and go!
    def flush(options={})

      operation = options.has_key?(:operation) ? options[:operation] : ""
      blocking = options.has_key?(:blocking) ? options[:blocking] : true
      msg_timeout = options.has_key?(:msg_timeout) ? options[:msg_timeout] : 10
      log_messages = options.has_key?(:log_messages) ? options[:log_messages] : false

      correlation_id = self.generate_uuid

      headers = {:service_name      => @service_queue_name,
                 :operation         => operation,
                 :response_required => blocking,   #Tell the receiver if a response is required
                 :from_name         => @from_name
      }

      #Publish the message
      print_log "Publishing message reply-to: #{@reply_queue.name} - headers: #{headers}", correlation_id if log_messages
      start_time = Time.now
      @service_exchange.publish(@outbuf,
                                :routing_key    => @service_queue_name,
                                :correlation_id => correlation_id,
                                :expiration     => msg_timeout,
                                :reply_to       => @reply_queue.name,
                                :headers        => headers)

      #If this is a standard RPC blocking call, then wait for there to be a response from the
      #service provider or timeout and log the timeout
      if blocking
        @response = ""
        begin
          #Adding 1sec to timeout to account for clock differences
          Timeout.timeout(msg_timeout + 1, ResponseTimeout) do
            @reply_queue.subscribe(:block => true) do |delivery_info, properties, payload|

              if log_messages
                response_time = Time.now - start_time
                print_log "---- Response Message received in #{response_time}sec for #{@reply_queue.name}", correlation_id
                print_log "HEADERS: #{properties}", correlation_id
              end

              if properties[:correlation_id] == correlation_id
                @response = payload

                #once the return message has been received, no need to continue a subscription
                delivery_info.consumer.cancel
              end
            end
          end
        rescue ResponseTimeout => ex
          #Trying to work around weirdness being seen in a multi threaded workflow environment
          if @response == ""
            msg = "A timeout has occurred (#{msg_timeout}sec) trying to call #{@service_queue_name}.#{operation}"
            print_log msg, correlation_id
            raise ex, msg
          else
            print_log "Ignoring timeout - #{@response}", correlation_id
          end
        end
        @inbuf = StringIO.new Bytes.force_binary_encoding(@response)
      end
      @outbuf = Bytes.empty_byte_buffer
    end

    protected

    def generate_uuid
      UUIDTools::UUID.timestamp_create.to_s
    end

    def print_log(message="", correlation_id="")
      puts "#{Time.now.utc} C Thread: #{Thread.current.object_id} CID:#{correlation_id} - #{message}"
    end
  end
end


