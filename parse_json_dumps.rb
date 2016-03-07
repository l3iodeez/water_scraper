require 'json'
require 'byebug'
require 'pg'

args = {
  host: "localhost",
  dbname: "water_data",
  user: "henry",
  password: "water"
}

conn = PGconn.connect(args)
states = File.read("states.json")
states = JSON.parse(states)
state_count = 0
states.each do |state|
  point_count = 0
  current_state = File.read("./state_dumps/#{state[1]}.json")
  current_state = JSON.parse(current_state)

  current_state["value"]["timeSeries"].each do |data_point|
    site_data = {
      site_name: data_point["sourceInfo"]["siteCode"].first["value"],
      latitude: data_point["sourceInfo"]["geoLocation"]["geogLocation"]["latitude"],
      longitude: data_point["sourceInfo"]["geoLocation"]["geogLocation"]["longitude"],
      full_json: data_point["sourceInfo"].to_json
    }

    site = conn.exec("SELECT id FROM sites WHERE site_name = '#{site_data[:site_name]}'")

    unless site.count > 0
      create_site_sql = <<-SQL
      INSERT INTO sites (site_name,latitude,longitude)
      VALUES ('#{site_data[:site_name]}','#{site_data[:latitude]}','#{site_data[:longitude]}')
      RETURNING id;
      SQL
      site = conn.exec(create_site_sql)
      site_id = site.first["id"]
      p "Created site record for #{site_data[:site_name]}"
    else
      site_id = site.first["id"].to_i
    end
    if data_point["values"].count > 1
    end
    byebug
    measurement = {
      site_id: site_id,
      site_name: site_data[:site_name],
      measure_date: Date.parse(data_point["values"].first["value"].first["dateTime"]),
      water_level: data_point["values"].first["value"].first["value"].to_i,
      measure_type:  data_point["variable"]["variableName"],
      data_provider: data_point["sourceInfo"]["siteCode"].first["agencyCode"],
      full_json: data_point.delete("sourceInfo").to_json
    }

    insert_measurement_sql = <<-SQL
    INSERT INTO measurements (site_id,site_name,measure_date,water_level,measure_type,data_provider,full_json)
    VALUES ('#{measurement[:site_id]}','#{measurement[:site_name]}','#{measurement[:measure_date]}',
            '#{measurement[:water_level]}','#{measurement[:measure_type]}','#{measurement[:data_provider]}',
            '#{measurement[:full_json]}')
    RETURNING id;
    SQL
    site = conn.exec(insert_measurement_sql)

    point_count += 1
    p "Processed data_point #{point_count} for #{state[1]}"

  end
  if state_count == 1
    byebug
  end
  state_count += 1
  p "Finished parsing #{state[1]}."
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
