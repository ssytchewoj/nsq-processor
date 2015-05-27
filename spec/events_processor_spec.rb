require 'spec_helper'

describe EventsProcessor do
	before :each do
		@processor = EventsProcessor.new
	end

	context "Interface" do
		it "should return internal Queue" do
			expect(@processor.queue).not_to be(nil)
		end

		it "should return videos hash" do
			expect(@processor.videos).to be_a(Hash)
		end

		it "should return processed messages count" do
			expect(@processor.events_processed).to eq(0)
		end
	end

	context "Processing" do
		before :each do
			@queue = @processor.queue
		end

		it "shouldn't raise an error if json is damaged" do
			@queue << 'foo bar'

			expect {
				@processor.process! 
			}.not_to raise_error
		end

		it "should incerease processed messages count after each message processing" do
			2.times do
				@queue << { video_id: 'test' }.to_json

				expect {
					@processor.process! 
				}.to change(@processor, :events_processed).by(1)
			end
		end

		it "shouldn't increase processed messages count if json doesn't have video_id attributes" do
			@queue << { asdfasf: 'test' }.to_json

			expect {
				@processor.process!
			}.not_to change(@processor, :events_processed)
		end

		it "should add GUID to videos hash" do
			expect(@processor.videos).not_to include('test')

			@queue << { video_id: 'test' }.to_json
			@processor.process!

			expect(@processor.videos).to include('test')
		end

		context "Publishing" do
			before :each do
				@original_stdout = $stdout
				$stdout = File.open(File::NULL, "w")
			end

			after :each do
				$stdout = @original_stdout
			end

			it "should output views counter for videos with less than 100 views ASAP" do
				@queue << { video_id: 'test' }.to_json
				@processor.process!

				sleep 1

				expect {
					@processor.process!
				}.to output(/test: 1/).to_stdout
			end

			it "shouldn't output views counter for videos with more than 100 views ASAP" do
				105.times do
					@queue << { video_id: 'test' }.to_json
					@processor.process!
				end
				
				sleep 1

				expect {
					@processor.process!
				}.not_to output.to_stdout
			end
		end
	end
end