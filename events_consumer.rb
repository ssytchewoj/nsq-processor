require 'thread'
require 'nsq'

class EventsConsumer
	def initialize queue
		@queue = queue

		@consumer = Nsq::Consumer.new(
  			topic: 'play-events',
  			channel: 'consumer',
			)

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
