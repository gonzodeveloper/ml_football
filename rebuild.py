from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *

config = SparkConf().setAppName("build dataset")
config = (config.setMaster('local[4]')
    .set('spark.executor.memory', '7G')
    .set('spark.driver.memory', '10G')
    .set('spark.driver.maxResultSize', '4G'))
sc = SparkContext(conf=config)
sc.setLogLevel(logLevel="OFF")
spark = SparkSession(sparkContext=sc)


schema = StructType([StructField('team_id', IntegerType(), True),
                     StructField('team_name', StringType(), True),
                     StructField('game_date', DateType(), True),
                     StructField('opp_id', IntegerType(), True),
                     StructField('opp_name', StringType(), True),
                     StructField('team_pts', IntegerType(), True),
                     StructField('opp_pts', IntegerType(), True),
                     StructField('location', StringType(), True)])

schedule = spark.read.csv('raw/schedule.csv', dateFormat='MM/dd/yy', schema=schema)
schedule.createTempView('schedule')

schema = StructType([StructField('team_id', IntegerType(), True),
                     StructField('team_name', StringType(), True),
                     StructField('game_date', DateType(), True),
                     StructField('lname', StringType(), True),
                     StructField('fname', StringType(), True),
                     StructField('rush_net', IntegerType(), True),
                     StructField('rush_td', IntegerType(), True),
                     StructField('pass_yards', IntegerType(), True),
                     StructField('pass_td', IntegerType(), True)
                     ])
offense = spark.read.csv('raw/offense.csv', dateFormat='MM/dd/yy', schema=schema)
offense.createTempView('offense')

schema = StructType([StructField('team_id', StringType(), True),
                     StructField('station_id', StringType(), True)])
stations = spark.read.csv('raw/schools.csv', schema=schema)
stations.createTempView('stations')


schema = StructType([StructField('station', StringType(), True),
                     StructField('date', DateType(), True),
                     StructField('PRCP', FloatType(), True),
                     StructField('SNOW', FloatType(), True)])
weather = spark.read.csv('raw/weather.csv', dateFormat='yyyy-MM-dd', schema=schema).fillna(0)
weather.createTempView('weather')

weather.show()

game_stats = spark.sql('SELECT '
                       ' schedule.game_date,'
                       ' schedule.team_id,'
                       ' schedule.opp_id,'
                       ' schedule.team_pts,'
                       ' schedule.opp_pts,'
                       ' SUM(rush_net) rush,'
                       ' SUM(pass_yards) pass, '
                       ' (CASE '
                       '     WHEN schedule.location == "Home" '
                       '     THEN home_station.station_id '
                       '     ELSE opp_station.station_id '
                       ' END) station '
                       'FROM schedule '
                       'JOIN offense ON 1=1'
                       ' AND schedule.team_id = offense.team_id'
                       ' AND schedule.game_date = offense.game_date '
                       'JOIN stations home_station ON 1=1 '
                       ' AND schedule.team_id = home_station.team_id '
                       'JOIN stations opp_station ON 1=1 '
                       ' AND schedule.opp_id = opp_station.team_id '
                       'GROUP BY schedule.game_date, schedule.team_id, schedule.opp_id, '
                       ' schedule.team_pts, schedule.opp_pts, schedule.location, '
                       '    home_station.station_id, opp_station.station_id '
                       )
game_stats.createTempView('game_stats')
game_stats.show()

full_game_stats = spark.sql("SELECT "
                            "   t1.game_date, "
                            "   t1.team_id AS team_id, "
                            "   t1.rush AS team_rush, "
                            "   t1.pass AS team_pass, "
                            "   t1.team_pts AS team_pts, "
                            "   t2.team_id AS opp_id, "
                            "   t2.rush AS opp_rush, "
                            "   t2.pass AS opp_pass, "
                            "   t2.team_pts AS opp_pts, "
                            "   (weather.PRCP + weather.SNOW) weather "
                            "FROM game_stats t1 "
                            "INNER JOIN game_stats t2 ON 1=1 "
                            "   AND t1.opp_id = t2.team_id "
                            "   AND t1.game_date = t2.game_date "
                            "JOIN weather ON 1=1 "
                            "   AND t1.station = weather.station "
                            "   AND t1.game_date = weather.date ")
full_game_stats.createTempView('full_game_stats')

full_game_stats.show()

game_data = spark.sql('SELECT'
                      ' game_date, '
                      ' team_id, '
                      ' opp_id, '
                      ' AVG(team_rush) OVER '
                      '     (PARTITION BY team_id ORDER BY game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) '
                      '     AS team_rush_avg, '
                      ' AVG(team_pass) OVER '
                      '     (PARTITION BY team_id ORDER BY game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) '
                      '     AS team_pass_avg, '
                      ' AVG(opp_rush) OVER '
                      '     (PARTITION BY opp_id ORDER BY game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) '
                      '     AS opp_rush_avg, '
                      ' AVG(opp_pass) OVER '
                      '     (PARTITION BY opp_id ORDER BY game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) '
                      '     AS opp_pass_avg, '
                      ' AVG(opp_rush) OVER '
                      '     (PARTITION BY team_id ORDER BY game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) '
                      '     AS team_rush_weak, '
                      ' AVG(opp_pass) OVER '
                      '     (PARTITION BY team_id ORDER BY game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) '
                      '     AS team_pass_weak, '
                      ' AVG(team_rush) OVER '
                      '     (PARTITION BY opp_id ORDER BY game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) '
                      '     AS opp_rush_weak, '
                      ' AVG(team_pass) OVER '
                      '     (PARTITION BY opp_id ORDER BY game_date ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING) '
                      '     AS opp_pass_weak, '
                      ' weather, '
                      ' (CASE'
                      '     WHEN team_pts > opp_pts '
                      '     THEN 1'
                      '     ELSE 0'
                      ' END) result '
                      'FROM full_game_stats')
game_data.createTempView('game_data')

game_data.coalesce(1).write.csv("data/game_data_9d.csv", header=True)