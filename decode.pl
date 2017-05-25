# Originally written by Andrew Rawlins https://github.com/andraw
# and then modified by Martin Vickers
#!/usr/bin/perl
use JSON qw( decode_json );
use DBI;

$db = 'MONITORDB';
$host = 'HOSTNAME';
$user = 'MONITOR_USER';
$password = 'PASSWORD';

$dbh = DBI->connect("DBI:mysql:database=$db;host=$host",$user, $password, {RaiseError => 1});

while(<>)
{
	my $json = $_;

	eval {
		my $decoded = decode_json($json);

		# ----------------------------------------------------------------------------------
		# Power Usage
		# ----------------------------------------------------------------------------------
		# CREATE DATABASE homemonitordb;
		# CREATE TABLE power_usage (channel_id  VARCHAR(4),
        	#                           log_time    DATETIME,
        	#                           consumption DECIMAL(5,2));
		# GRANT USAGE ON homemonitordb.* TO homemonitor_user@localhost IDENTIFIED BY 'PASSWORD';
		# GRANT ALL PRIVILEGES ON homemonitordb.* TO homemonitor_user@localhost WITH GRANT OPTION;
		# FLUSH PRIVILEGES;

		if ($decoded->{'model'} eq "CurrentCost TX") 
		{
	                $timestamp = $decoded->{'time'};
	                $device_id = $decoded->{'dev_id'};
       		        $power_use = $decoded->{'power0'};

			$sql = "INSERT INTO power_usage VALUES($device_id, '$timestamp', $power_use)";

                	$sth = $dbh -> prepare($sql);
                	$sth -> execute;

                	print "$timestamp: $device_id -> $power_use" . "W \n";
		}

		# ----------------------------------------------------------------------------------
		# Weather Station
		# ----------------------------------------------------------------------------------
		#
		# CREATE TABLE weather_station ( station_id    VARCHAR(4),
		#                                log_time      DATETIME,
		#                                outdoor_temp  DECIMAL(5,2),
		#                                humidity      DECIMAL(5,2),
		#				 wind_avg_mph  DECIMAL(5,2),
		#                                wind_gust_mph DECIMAL(5,2),
		#                                total_rain     DECIMAL(5,2));
        	# GRANT USAGE ON homemonitordb.* TO homemonitor_user@localhost IDENTIFIED BY 'PASSWORD';
        	# GRANT ALL PRIVILEGES ON homemonitordb.* TO homemonitor_user@localhost WITH GRANT OPTION;
        	# FLUSH PRIVILEGES;


		if ($decoded->{'model'} eq "Fine Offset WH1050 weather station")
		{
			#{"time" : "2017-05-09 13:52:11", "model" : "Fine Offset WH1050 weather station", "id" : 215, "temperature_C" : 17.300, "humidity" : 40, "speed" : 0.000, "gust" : 0.000, "rain" : 90.900, "battery" : "LOW"}

			$timestamp = $decoded->{'time'};
			$station_id = $decoded->{'id'};
			$temperature = $decoded->{'temperature_C'};
			$humidity = $decoded->{'humidity'};
			$wind_speed = $decoded->{'speed'};
			$gust = $decoded->{'gust'};
			$rainfall = $decoded->{'rain'};
			$batt_status =$decoded->{'battery'};

                	$sql = "INSERT INTO weather_station VALUES($device_id, '$timestamp', $temperature, $humidity, $wind_speed, $gust, $rainfall)";

                	$sth = $dbh -> prepare($sql);
               		$sth -> execute;

		}

		#create table temperature_sensors(id INTEGER, log_time DATETIME, channel INTEGER, temperature DECIMAL(5,2), humidity DECIMAL(5,2));

		#{"time" : "2017-05-23 18:25:56", "model" : "Prologue sensor", "id" : 5, "rid" : 163, "channel" : 3, "battery" : "OK", "button" : 0, "temperature_C" : 22.200, "humidity" : 67}
		#{"time" : "2017-05-23 18:26:07", "model" : "Prologue sensor", "id" : 5, "rid" : 120, "channel" : 1, "battery" : "OK", "button" : 0, "temperature_C" : 22.900, "humidity" : 65}
		if ($decoded->{'model'} eq "Prologue sensor")
                {
                        $timestamp = $decoded->{'time'};
                        $station_id = $decoded->{'id'};
			$station_rid = $decoded->{'rid'};
			$station_channel = $decoded->{'channel'};
                        $temperature = $decoded->{'temperature_C'};
                        $humidity = $decoded->{'humidity'};
                        $batt_status =$decoded->{'battery'};

			$sql = "INSERT INTO temperature_sensors VALUES($station_id, '$timestamp', $station_channel, $temperature, $humidity)";
                        $sth = $dbh -> prepare($sql);
                        $sth -> execute;
		}

		1;

	} or do {
		print "Line error \n";
	}
}

$dbh->disconnect;
