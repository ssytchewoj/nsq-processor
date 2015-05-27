require 'thread'
require 'nsq'

# Used to get events from NSQ
# TODO: Need to check if connection is successful
# TODO: Signal handling

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
end
