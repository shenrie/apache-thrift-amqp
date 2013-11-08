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


module Thrift
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

      #print "client:", @conn, "\n"
      @ch   = @conn.create_channel

      @from_name = opts[:from_name].nil? ? "Unknown Client" : opts[:from_name]

      @exchange = opts[:exchange] || nil
      @service_exchange = @exchange.nil? ? @ch.default_exchange : @ch.direct(@exchange, :durable => true)

      @service_response_exchange = @ch.default_exchange
      @reply_queue = @ch.queue("", :exclusive => true)

      @is_opened = false
    end

    def open
      @is_opened = true
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

    def open?; true end
    def read(sz); @inbuf.read sz end
    def write(buf); @outbuf << Bytes.force_binary_encoding(buf) end

    #If blocking is set to true then wait for a response message in the reply_to queue, otherwise
    #just send and go!
    def flush(operation="", blocking=true, msg_timeout=10)

      #Replace this with something that generate a real conversation id
      correlation_id = self.generate_uuid
      response = ""

      #Publish the initial message
      #TODO - add exchange
      @service_exchange.publish(@outbuf,
                                :routing_key    => @service_queue_name,
                                :correlation_id => correlation_id,
                                :expiration => Time.now.to_i + msg_timeout,  #Now + sec
                                :reply_to       => @reply_queue.name,
                                :headers => {:service_name => @service_queue_name,
                                             :operation => operation,
                                             :response_required => blocking,   #Tell the receiver if a response is required
                                             :from_name => @from_name
                                })

      #If this is a standard RPC blocking call, then wait for there to be a response from the
      #service provider or timeout and log the timeout
      if blocking
        begin
          complete_results = Timeout.timeout(msg_timeout) do


            @reply_queue.subscribe(:block => true) do |delivery_info, properties, payload|
              if properties[:correlation_id] == correlation_id
                response = Bytes.force_binary_encoding(payload)

                #once the return message has been received, no need to continue a subscription
                delivery_info.consumer.cancel
              end
            end

          end

          @inbuf = StringIO.new response

        rescue Timeout::Error => ex
          #TODO - Log this
          msg = "A timeout has occurred (#{msg_timeout}sec) trying to send a message to the #{@service_queue_name} service"
          print msg, "\n"
          raise ex, msg
        end

      end

      @outbuf = Bytes.empty_byte_buffer

    end

    protected

    def generate_uuid
      #TODO - generate a real UUID
      # very naive but good enough for code
      # examples
      "#{rand}#{rand}#{rand}"
    end

  end
end

