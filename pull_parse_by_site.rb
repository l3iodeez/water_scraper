require "nokogiri"
require "byebug"
require 'pg'
require 'json'
require 'open-uri'


args = {
  host: "localhost",
  dbname: "water_data",
  user: "henry",
  password: "water"
}

conn = PGconn.connect(args)

sites = conn.exec("SELECT id, site_name FROM sites")
site_names = sites.to_a.map{|el| el["site_name"] }
site_count = sites.count
p "Processing #{site_count} sites."
sites.each_with_index do |site, s_idx|
  if s_idx < 3640
    next
  end
  site_id = site["id"]
  site_name = site["site_name"]

  unless site_name.match(/VW_GWDP_GEOSERVER.USGS.(\d+)/)
    p "Skipping non-USGU site #{s_idx+1}"
    next
  end
  site_num = site_name.match(/VW_GWDP_GEOSERVER.USGS.(\d+)/)[1]

    unless File.size?("./site_dumps/#{site_num}.xml")
      p "Start downloading data for site #{s_idx+1} of #{site_count} sites."
      open("./site_dumps/#{site_num}.xml", 'wb') do |file|
        begin
          file << open("http://waterservices.usgs.gov/nwis/gwlevels/?format=waterml,2.0&sites=#{site_num}&startDT=1800-01-01&endDT=2016-03-06").read
        rescue OpenURI::HTTPError => e
          p e.message
          next
        end

      end
    else
      p "Using existing file for site #{s_idx+1} of #{site_count} sites."
    end
    site_xml = Nokogiri::XML(File.open("./site_dumps/#{site_num}.xml"))
    measurements = site_xml.xpath("//wml2:MeasurementTimeseries").xpath("wml2:point")
    measure_count = measurements.count
    loaded_count = conn.exec("SELECT count(*) FROM measurements WHERE site_id = #{site_id}").first["count"].to_i
    if loaded_count > 0
      if loaded_count >= measure_count - 1
        p "Measurments loaded. Skipping site."
        next
      else
        p "Measurments partially loaded. Deleting and reloading site."
        conn.exec("DELETE FROM measurements WHERE site_id = #{site_id}")
      end

    end
    data_provider = "U.S. Geological Survey"
    measure_type = site_xml.xpath("//om:observedProperty").first.attributes["title"].text
    p "Processing #{measure_count} data points."
    data_string = ""
    measurements.each_with_index do |measurement, m_idx|
      data_point = measurement.text.split("\n").map{|e|e.strip}.reject(&:empty?)
      unless data_point.empty? || data_point[1] == "" || data_point[1].nil?

        measurement = {
          site_id: site_id,
          site_name: site_name,
          measure_date: Date.parse(data_point[0]),
          water_level: data_point[1],
          measure_type:  measure_type,
          data_provider: data_provider
        }
        data_string += "('" + measurement.values.join("', '") +"'),"
      end
      p "Processed measurement #{m_idx+1} of #{measure_count}."
    end
    data_string
    p "Processed site #{s_idx+1} of #{site_count}. Running SQL insert."
    insert = <<-SQL
      INSERT INTO measurements (site_id,site_name,measure_date,water_level,measure_type,data_provider)
      VALUES #{data_string.chomp(",")}
        RETURNING id;
    SQL
    site = conn.exec(insert)
    if site.error_message.length > 0
      byebug
    end
    p "Inserted #{site_count} measurements."

end
