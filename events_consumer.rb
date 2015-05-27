require 'thread'
require 'nsq'

class EventsConsumer
	def initialize queue
		@queue = queue

		begin
			@consumer = Nsq::Consumer.new(
	  			topic: 'play-events',
	  			channel: 'consumer',
				)
		rescue Exception => ex
			puts ex
			exit
		end

		@thread = Thread.new {
			receive_loop
		}
	end

	def receive_loop
		loop do
			event = @consumer.pop
			@queue << event.body
			event.finish
		end
	end

	def terminate
		@consumer.terminate
	end
end
