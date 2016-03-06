
require 'json'
require 'byebug'
require 'open-uri'


states = File.read("states.json")
states = JSON.parse(states)

states.each do |state|
  download = open("http://waterservices.usgs.gov/nwis/gwlevels/?format=json,1.2&stateCd=#{state[0]}&startDT=1900-01-01&endDT=2016-03-06&siteType=GW")
  IO.copy_stream(download, "./state_dumps/#{state[0]}.json")
end
