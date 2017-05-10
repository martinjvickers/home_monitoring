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
	my $decoded = decode_json($json);

	# ----------------------------------------------------------------------------------
	# Power Usage
	# ----------------------------------------------------------------------------------
	# CREATE DATABASE MONITORDB;
	# CREATE TABLE power_usage (channel_id  VARCHAR(4),
        #                           log_time    DATETIME,
        #                           consumption DECIMAL(5,2));
	# GRANT USAGE ON MONITORDB.* TO MONITOR_USER@HOSTNAME IDENTIFIED BY 'PASSWORD';
	# GRANT ALL PRIVILEGES ON MONITORDB.* TO MONITOR_USER@HOSTNAME WITH GRANT OPTION;
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
        # GRANT USAGE ON MONITORDB.* TO MONITOR_USER@HOSTNAME IDENTIFIED BY 'PASSWORD';
        # GRANT ALL PRIVILEGES ON MONITORDB.* TO MONITOR_USER@HOSTNAME WITH GRANT OPTION;
        # FLUSH PRIVILEGES;


	if ($decoded->{'model'} eq "Fine Offset WH1050 weather station")
	{

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

		print "$timestamp: $station_id -> $temperature" . "C \n";

	}
}

$dbh->disconnect;
