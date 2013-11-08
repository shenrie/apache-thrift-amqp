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

$:.push('../gen-rb')
$:.push('../../transport/rb')
$:.unshift '../../lib/rb/lib'

require 'thrift'
require 'thrift/server/mongrel_http_server'
require 'thrift/protocol/compact_protocol'
require 'amqp_rpc_service'

require "rubygems"
require "json"

require 'calculator'
require 'shared_types'


class CalculatorHandler
  def initialize()
    @log = {}
  end

  def ping()
    puts "ping()"
  end

  def add(n1, n2)
    print "add(", n1, ",", n2, ")\n"
    return n1 + n2
  end

#  def add(n1, n2, n3=0)
#    print "add(", n1, ",", n2, ",", n3 , ")\n"
#    return n1 + n2 + n3
#  end


  def calculate(logid, work)
    print "calculate(", logid, ", {", work.op, ",", work.num1, ",", work.num2,"})\n"
    if work.op == Operation::ADD
      val = work.num1 + work.num2
    elsif work.op == Operation::SUBTRACT
      val = work.num1 - work.num2
    elsif work.op == Operation::MULTIPLY
      val = work.num1 * work.num2
    elsif work.op == Operation::DIVIDE
      if work.num2 == 0
        x = InvalidOperation.new()
        x.what = work.op
        x.why = "Cannot divide by 0"
        raise x
      end
      val = work.num1 / work.num2
    else
      x = InvalidOperation.new()
      x.what = work.op
      x.why = "Invalid operation"
      raise x
    end

    entry = SharedStruct.new()
    entry.key = logid
    entry.value = "#{val}"
    @log[logid] = entry

    return val

  end

  def getStruct(key)
    print "getStruct(", key, ")\n"
    return @log[key]
  end

  def zip()
    print "zip\n"
  end
    
    
  def update_json_data(input, new_key, new_val)
      
      input[new_key] = new_val
      return input
  end


end

class CalculatorProcessor < Calculator::Processor


  #Overrides this method to convert string to json from transport
  def read_args(iprot, args_class)
    args = args_class.new
    args.read(iprot)
    iprot.read_message_end
 
    if args.respond_to?(:struct_fields)
      args.struct_fields.each do | idx, arg |

        if arg[:type] == Thrift::Types::STRUCT and arg[:class] == Json_data
          arg_name = arg[:name]
          json_res = JSON.parse(args.send(arg[:name]).data)
          args.instance_variable_set("@#{arg_name}", json_res)
        end

        
      end
    end
      
    args
  end

  #Overrides this method to convert json to string for transport
  def write_result(result, oprot, name, seqid)

    if result.respond_to?(:success) and result.success.class == Hash and
        result.struct_fields[0][:type] == Thrift::Types::STRUCT and result.struct_fields[0][:class] == Json_data
      res = Json_data.new()
      res.data = JSON.generate(result.success)
      result.success = res
    end

    super

  end


end

handler = CalculatorHandler.new()
processor = CalculatorProcessor.new(handler)

if ARGV[0] == "socket"
  port = ARGV[1] || 9090
  transport = Thrift::ServerSocket.new(port)
  transportFactory = Thrift::BufferedTransportFactory.new()
  server = Thrift::SimpleServer.new(processor, transport, transportFactory)

elsif ARGV[0] == "http"

  #"http://localhost:8081/calc"
  server = Thrift::MongrelHTTPServer.new(processor, { :ip => '127.0.0.1', :port => "8081"})
  #server = Thrift::MongrelHTTPServer.new(processor, { :ip => '127.0.0.1', :port => "8081", :protocol_factory => Thrift::CompactProtocolFactory.new})
else
  server = Thrift::AmqpRpcServer.new(processor, opts = {:host => '127.0.0.1', :port => 5672, :queue_name => 'rpc_queue', :exchange => 'rpc_services'})
end


begin
  puts "Starting the server..."
  server.serve()
  puts "done."
rescue Interrupt => _
  server.close()
end


