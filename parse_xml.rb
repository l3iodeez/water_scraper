require "nokogiri"
require "byebug"
require 'pg'
require 'json'

args = {
  host: "localhost",
  dbname: "water_data",
  user: "henry",
  password: "water"
}

conn = PGconn.connect(args)

foi_list = Nokogiri::XML(File.open("foi_list.xml"))
sites = foi_list.xpath("//sos:featureMember")
sites.each do |site|
  begin

  site_data = {
    site_name: site.xpath("sams:SF_SpatialSamplingFeature").first.attributes["id"].value,
    well_reference: site.xpath("sams:SF_SpatialSamplingFeature").xpath("gml:description").text,
    latitude:  site.xpath("sams:SF_SpatialSamplingFeature").xpath("sams:shape").xpath("gml:Point").xpath("gml:pos").text.split[0],
    longitude: site.xpath("sams:SF_SpatialSamplingFeature").xpath("sams:shape").xpath("gml:Point").xpath("gml:pos").text.split[1],
    full_json: site.to_json
  }
  create_site_sql = <<-SQL
  INSERT INTO sites (site_name, well_reference, latitude, longitude,full_json)
  VALUES ('#{site_data[:site_name]}',
          '#{site_data[:well_reference]}',
          '#{site_data[:latitude]}',
          '#{site_data[:longitude]}',
          '#{site_data[:full_json]}'
          )
  RETURNING id;
  SQL

  site = conn.exec(create_site_sql)
  site_id = site.first["id"]
  p "Created site record for #{site_data[:site_name]}"
  rescue StandardError => e
    byebug
    p "Failed to create site record for site"
    p e.message
  end
end

# Site table
# id             | integer                     | not null default nextval('sites_id_seq'::regclass) | plain    |              |
#  site_name      | character varying           |                                                    | extended |              |
#  well_reference | character varying           |                                                    | extended |              |
#  latitude       | double precision            |                                                    | plain    |              |
#  longitude      | double precision            |                                                    | plain    |              |
#  start          | timestamp without time zone |                                                    | plain    |              |
#  end            | timestamp without time zone |                                                    | plain    |              |
#  measure_count  | integer                     |                                                    | plain    |              |
#  city           | character varying           |                                                    | extended |              |
#  state          | character varying           |                                                    | extended |              |
#  zip            | character varying           |                                                    | extended |              |
#  address        | character varying           |                                                    | extended |


# Measurement Table
# ---------------+-------------------+-----------------------------------------------------------+----------+--------------+-------------
# id            | integer           | not null default nextval('measurements_id_seq'::regclass) | plain    |              |
# site_id       | integer           |                                                           | plain    |              |
# site_name     | character varying |                                                           | extended |              |
# measure_date  | date              |                                                           | plain    |              |
# water_level   | numeric           |                                                           | main     |              |
# units         | character varying |                                                           | extended |              |
# measure_type  | character varying |                                                           | extended |              |
# data_provider | character varying |                                                           | extended |              |
