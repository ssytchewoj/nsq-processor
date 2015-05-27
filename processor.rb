#!/usr/bin/env ruby

require 'rubygems'
require 'bundler/setup'

Bundler.setup(:default)

require './events_consumer'
require './events_processor'

processor = EventsProcessor.new
consumer = EventsConsumer.new processor.queue

loop do
	processor.process!
end