#!/usr/bin/env ruby

require 'rubygems'
require 'bundler/setup'

Bundler.setup(:default)

require './events_consumer'
require './events_processor'

processor = EventsProcessor.new
consumer = EventsConsumer.new processor.queue

trap("INT") { 
	puts "Shutting down."
	consumer.terminate
	exit
}

loop do
	processor.process!
end