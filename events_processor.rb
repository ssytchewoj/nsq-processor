require './queue_with_timeout'
require 'json'
require 'date'


class EventsProcessor
	def initialize
		@queue = QueueWithTimeout.new
		@videos = Hash.new

		@last_publish = DateTime.now

		@events_before_publish = 0

		@urgent = false

		@events_processed = 0
	end

	def queue
		@queue
	end

	def videos
		@videos
	end

	def events_processed
		@events_processed
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
			if _message.has_key? 'video_id'
				@events_processed += 1

				guid = _message['video_id']
				if @videos.has_key? guid
					@videos[guid][:viewes] += 1
				else
					@videos[guid] = { viewes: 1 }
				end

				if @videos[guid][:viewes] < 100
					urgent! @videos[guid][:viewes]
				end
			end
		end
	end

	def publish_results! 
		if publish_required? or (((DateTime.now - @last_publish) * 24 * 60 * 60).to_i > 60)
			puts "videos update"

			@videos.each do |k, v|
				puts "#{k}: #{v[:viewes]}" 
			end

			@last_publish = DateTime.now
		end
	end

	def publish_required?
		result = false

		if @urgent == true
			@events_before_publish -= 1
			if @events_before_publish == 0
				@urgent = false
				result = true
			end

			if ((DateTime.now - @last_urgent_call) * 24 * 60 * 60).to_i >= 1
				result = true
			end
		end

		result
	end

	def urgent! views
		events_counter = 10

		if views < 10
			events_counter = 3
		elsif views < 30
			events_counte = 6
		elsif views < 70
			events_counter = 7
		end

		if @urgent == true and @events_before_publish > events_counter
			@events_before_publish = events_counter
		elsif @urgent == false
			@urgent = true
			@last_urgent_call = DateTime.now
			@events_before_publish = events_counter
		end
	end
end