
require 'json'
require 'byebug'
require 'open-uri'


states = File.read("states.json")
states = JSON.parse(states)

states.each do |state|
  begin
    if !File.exists?("./state_dumps/#{state[1]}.json")
      puts "Started dumping #{state[1]}."
      download = open("http://waterservices.usgs.gov/nwis/gwlevels/?format=json,1.2&stateCd=#{state[0]}&startDT=1900-01-01&endDT=2016-03-06&siteType=GW", :read_timeout=>3600)
      IO.copy_stream(download, "./state_dumps/#{state[1]}.json")
      puts "Finished dumping #{state[1]}."
    else
      puts "#{state[1]} already dumped."
    end
  rescue StandardError => e
   puts "error: " + e.message
  end
end
