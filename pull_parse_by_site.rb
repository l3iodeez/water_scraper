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

sites = conn.exec("SELECT s.id, s.site_name FROM sites s LEFT JOIN measurements m ON m.site_id = s.id GROUP BY s.id HAVING count(m.id) = 0")
site_names = sites.to_a.map{|el| el["site_name"] }
site_count = sites.count
p "Processing #{site_count} sites."
sites.each_with_index do |site, s_idx|

  site_id = site["id"]
  site_name = site["site_name"]

  # unless site_name.match(/VW_GWDP_GEOSERVER.USGS.(\d+)/)
  #   p "Skipping non-USGU site #{s_idx+1}"
  #   next
  # end
  # site_num = site_name.match(/VW_GWDP_GEOSERVER.USGS.(\d+)/)[1]
  next if site_name =~ /\s/
    unless File.size?("./site_dumps/#{site_name.gsub(/\s/,'')}.xml")
      p "Start downloading data for site #{s_idx+1} of #{site_count} sites."
      open("./site_dumps/#{site_name.gsub(/\s/,'')}.xml", 'wb') do |file|
        begin
          sleep(3.5)
          url = "http://cida.usgs.gov/ngwmn_cache/sos?request=GetObservation&service=SOS&version=2.0.0&responseFormat=text/xml&featureOfInterest=#{site_name}"

          file << open(url).read
        rescue OpenURI::HTTPError => e
          sleep(5)
          retry
        end

      end
    else
      p "Using existing file for site #{s_idx+1} of #{site_count} sites."
    end
    site_xml = Nokogiri::XML(File.open("./site_dumps/#{site_name.gsub(/\s/,'')}.xml"))
    site_xml.remove_namespaces!
    measurements = site_xml.xpath("//MeasurementTimeseries").xpath("//point")
    next unless measurements.count > 0
    neasurments = measurements
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
    measure_type = site_xml.xpath("//observedProperty").first.attributes["href"].value
    p "Processing #{measure_count} data points."
    data_string = ""
    measurements.each_with_index do |measurement, m_idx|
      data_point = measurement.text.split("\n").map{|e|e.strip}.reject(&:empty?)
      unless data_point.empty? || data_point[1] == "" || data_point[1].nil?
        date = data_point[0].split('-')
        date = date.map do |el|
          if el == "00"
            "01"
          else
            el
          end
        end
        begin
        date = Date.parse(date.join("-"))
        measurement = {
          site_id: site_id,
          site_name: site_name,
          measure_date: date,
          water_level: data_point[1],
          measure_type:  measure_type,
          data_provider: data_provider
        }
        data_string += "('" + measurement.values.join("', '") +"'),"
        rescue StandardError => e
          next
        end
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
    p "Inserted #{measure_count} measurements."

end
