apache-thrift-amqp
==================

Support for an AMQP transport layer for Apache Thrift


This project implements an AMQP transport layer for Apache Thrift in Ruby and
extends the Apache Thrift tutorial.

The AMQP transport layer depends on the Ruby bunny gem for connection to a RabbitMQ broker.

The tutorial was designed to demonstrate the use of Apache Thrift using a
simple socket, HTTP or AMQP transport layer.

To run the simple socket example:
From a command line, execute: ruby RubyServer.rb socket
From a second command line, execute RubyClient.rb socket

To run the http example:
From a command line, execute: ruby RubyServer.rb http
From a second command line, execute RubyClient.rb http

To run the AMQP demo:
Start RabbitMQ (if a different host or port is used then the code will need to be updated to match)
From a command line, execute: ruby RubyServer.rb
From a second command line, execute RubyClient.rb


When using AMQP, multiple RubyServer processes can be run to do load balancing.


