require './queue_with_timeout'
require 'json'
require 'date'


class EventsProcessor
	def initialize
		@queue = QueueWithTimeout.new
		@videos = Hash.new

		@last_publish = DateTime.now
	end

	def queue
		@queue
	end

	def process!
		begin
			message = @queue.pop_with_timeout 0.1
		rescue QueueWithTimeout::EmptyError
		else
			process_message message
		ensure
			publish_results!
		end
	end

	private

	def process_message message
		begin
			_message = JSON.parse message
		rescue JSON::JSONError
		else
			guid = _message['guid']
			if @videos.has_key? guid
				@videos[guid][:viewes] += 1
			else
				@videos[guid] = { viewes: 1, publish_required: 1 }
			end

			@videos[guid][:publish_required] = 1

			if @videos[guid][:viewes] < 100
				publish_results! true
			end
		end
	end

	def publish_results! urgent = false
		if urgent or (((DateTime.now - @last_publish) * 24 * 60 * 60).to_i > 59)
			puts "videos update"

			@videos.each do |k, v|
				puts "#{k}: #{v[:viewes]}"
			end

			@last_publish = DateTime.now
		end
	end
end