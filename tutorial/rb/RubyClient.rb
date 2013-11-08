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

require "json"
require 'thrift'
require 'amqp_rpc_client'

require 'calculator'

class CalculatorClient < Calculator::Client

  def send_message(name, args_class, args = {})
    @oprot.write_message_begin(name, Thrift::MessageTypes::CALL, @seqid)
    data = args_class.new
    fld = 1
    args.each do |k, v|
        
      if data.struct_fields[fld][:type] == Thrift::Types::STRUCT and data.struct_fields[fld][:class] == Json_data
        res = Json_data.new()
        res.data = JSON.generate(v)
        v = res
      end
        
      data.send("#{k.to_s}=", v)
      fld += 1
    end
    begin
      data.write(@oprot)
    rescue StandardError => e
      @oprot.trans.close
      raise e
    end
    @oprot.write_message_end
    @oprot.trans.flush
  end

  def receive_message(result_klass)
    fname, mtype, rseqid = @iprot.read_message_begin
    handle_exception(mtype)
    result = result_klass.new
    result.read(@iprot)
    @iprot.read_message_end

    if result.respond_to?(:success) and result.success.class == Json_data and
        result.struct_fields[0][:type] == Thrift::Types::STRUCT and result.struct_fields[0][:class] == Json_data
      json_res = JSON.parse(result.success.data)

      result.success = json_res
    end

    result

  end


end


begin

  if ARGV[0] == "socket"
    port = ARGV[1] || 9090
    transport = Thrift::BufferedTransport.new(Thrift::Socket.new('127.0.0.1', port))
  elsif ARGV[0] == "http"
    transport = Thrift::HTTPClientTransport.new("http://127.0.0.1:8081/calc")
  else
    transport = Thrift::AmqpRpcClientTransport.new("rpc_queue", opts = {:host => '127.0.0.1', :port => 5672})
  end

  protocol = Thrift::BinaryProtocol.new(transport)
  client = CalculatorClient.new(protocol)

  transport.open()

  client.ping()
  print "ping()\n"

  version = 1

  if version == 2
    sum = client.add(1,1, 3)
    print "1+1+3=", sum, "\n"

    sum = client.add(1,4, 6)
    print "1+4+6=", sum, "\n"

  else
    sum = client.add(1,1)
    print "1+1=", sum, "\n"

    sum = client.add(1,4)
    print "1+4=", sum, "\n"
  end


  work = Work.new()

  work.op = Operation::SUBTRACT
  work.num1 = 15
  work.num2 = 10
  diff = client.calculate(1, work)
  print "15-10=", diff, "\n"

  log = client.getStruct(1)
  print "Log: ", log.value, "\n"

  begin
    work.op = Operation::DIVIDE
    work.num1 = 1
    work.num2 = 0
    quot = client.calculate(1, work)
    puts "Whoa, we can divide by 0 now?"
  rescue InvalidOperation => io
    print "InvalidOperation: ", io.why, "\n"
  end

  client.zip()
  print "zip\n"

  string = '{"desc":{"someKey":"someValue","anotherKey":"value"},"main_item":{"stats":{"a":8,"b":12,"c":10}}}'
  js_val = JSON.parse(string) # returns a hash
    
  begin
    ret = client.update_json_data(js_val, "mynewkey", "another-val")
    print "ret=", ret.to_s , "\n"
  rescue NoMethodError => ex
    print "Exception: ", ex.message, "\n"
  end

  transport.close()

rescue Thrift::Exception => tx
  print 'Thrift::Exception: ', tx.message, "\n"
end
