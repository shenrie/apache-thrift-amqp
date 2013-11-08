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

module Thrift
  class AmqpRpcServer < BaseServer

    def initialize(processor, opts={})

      @log_messages = true

      @processor = processor

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
      else
        @conn = opts[:connection]
      end

      #print "service:", @conn, "\n"

      if not opts.has_key?(:queue_name)
        raise ArgumentError, "A service queue name has not been specified"
      end

      @queue_name = opts[:queue_name]
      @protocol_factory = opts[:protocol_factory] || BinaryProtocolFactory.new

      #Create a channel to the service queue
      @request_channel = @conn.create_channel

      @exchange = opts[:exchange] || nil

      if @exchange.nil?
        @service_exchange = @request_channel.default_exchange
        @request_queue = @request_channel.queue(@queue_name, :auto_delete => true)
      else
        @service_exchange = @request_channel.direct(@exchange,:durable => true)
        @request_queue = @request_channel.queue(@queue_name, :auto_delete => true).bind(@service_exchange, :routing_key => @queue_name)
      end

    end

    def close

      if not @request_channel.nil? and @request_channel.respond_to?('close')
        @request_channel.close
      end

      #Always close the broker connection when closing the server
      @conn.close

    end

    def serve

      @request_cnt = 0

      @thread_pool = Set.new []

      @request_queue.subscribe(:block => true) do |delivery_info, properties, payload|

        if @log_messages
          print "Message received, number of threads currently running: #{@thread_pool.size()}\n"
          print "headers: #{properties}\n"
          print "payload: #{payload.strip}\n"
        end

        #Trying to manage the number of threads to be spawned
        if @thread_pool.size() > 10  #TODO - externalize this or make it dynamically configurable
          print "Too many threads #{@thread_pool.size()} are open to handle this message. The message will be requeued"
          @request_channel.reject(delivery_info.delivery_tag, true)      #requeue the message
        else

          @request_cnt += 1

          thread = Thread.new {

            response_channel = @conn.create_channel
            response_exchange = response_channel.default_exchange

            response_required = properties.headers.has_key?('response_required') ? properties.headers['response_required'] : true

            start_time = Time.now

            #Binary content will imply thrift based message payload
            if properties.content_type == 'application/octet-stream'

              input = StringIO.new payload
              out = StringIO.new
              transport = IOStreamTransport.new input, out
              protocol = @protocol_factory.get_protocol transport

              @processor.process protocol, protocol

              stop_time = Time.now

              #rewind the buffer for reading
              out.rewind

              if @log_messages
                print "Time to process request: #{stop_time - start_time}sec\n"
                print "response: #{out.read(out.length).strip}\n"
                #rewind the buffer for reading
                out.rewind
              end

              #only send a response if the operation is not defined as a oneway, otherwise messages will build up in client reply queue
              if response_required
                #Don't respond if expired
                if stop_time.to_i <= properties.expiration.to_i
                  response_exchange.publish(out.read(out.length), :routing_key => properties.reply_to, :correlation_id => properties.correlation_id, :content_type => 'application/octet-stream' )
                else
                  response_exchange.publish("Service #{properties.headers['service_name']} request for #{properties.headers['operation']} timed out",
                                            :routing_key => properties.reply_to, :correlation_id => properties.correlation_id, :content_type => 'text/plain' )
                end
              end

            else

              print "Unable to process message content of type #{properties.content_type}. The message will be rejected"
              @request_channel.reject(delivery_info.delivery_tag, false)      #requeue the message

            end

            response_channel.close

            @thread_pool.delete(Thread.current)

            Thread.exit

          }

          @thread_pool.add(thread)

        end

      end

    end
  end
end
