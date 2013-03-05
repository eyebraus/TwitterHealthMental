#!/usr/bin/env ruby

require 'couchrest'
require 'json'
require 'nokogiri'
require 'ruby-aws'

if __FILE__ == $0 then
    couch = CouchRest::Server.new("http://dev.fount.in:5984")
    database = couch.database("mturk_trial")
    mturk = Amazon::WebServices::MechanicalTurkRequester.new :Host => :Sandbox    

    # grab tweets and generate tasks
    couch_docs = database.view("Tweet/all", params = { :include_docs => true })["rows"].map {|r| r["doc"] }

    (0...5).step(5) do |off|
        # parse XML doc
        File.open("questions/tweet_eval.question") do |f|
            doc = Nokogiri::XML(f)
            questions = doc.css("Question")
            # add tweets to evaluation questions
            couch_docs[off...(off + 5)].each_index do |i|
                tweet = couch_docs[i]
                questions.css("QuestionIdentifier").to_a.select {|n| n.inner_text == "tweet#{i + 1}d" or n.inner_text == "tweet#{i + 1}a" }.each do |n|
                    question = n.parent
                    question.at_css("QuestionContent Text").content = tweet["text"]
                end
            end
            # submit to the turk sandbox
            #puts doc.to_xml(:indent => 4, :enconding => 'UTF-8')
            result = mturk.createHIT(
                :Title          => "Answer some quick surveys about these tweets.",
                :Description    => "Please rate this tweet as showing depression, anxiety, or unrelated.",
                :MaxAssignments => 5,
                :Reward         => { :Amount => 0.05, :CurrencyCode => 'USD' },
                :Question       => doc.to_s,
                :Keywords       => "twitter, tweet, tweets, mood, survey, health")

            puts "Created HIT, woo"
            if mturk.host =~ /sandbox/
                puts "id: #{result[:HITId]}, location: http://workersandbox.mturk.com/mturk/preview?groupId=#{result[:HITId]}"
            else
                puts "id: #{result[:HITId]}, location: http://mturk.com/mturk/preview?groupId=#{result[:HITId]}"
            end
        end
    end
    
end