hadoop fs -rmdir /input/*
hadoop fs -mkdir /input
hadoop fs -copyFromLocal /home/guest/Desktop/data/*.csv /input
hadoop fs -ls /input
 
## Start accumulo shell
cd /home/guest/cdse/accumulo/accumulo-1.4.2/bin
accumulo shell -u root -p acc
 
## User auths
setauths -s east,west -u root
createuser east
## create password
setauths -s east -u east 
createuser west
## create password
setauths -s west -u west
createtable NBACount
grant Table.READ -t NBACount -u east 
grant Table.READ -t NBACount -u west 
setiter -class org.apache.accumulo.core.iterators.user.SummingCombiner -p 10 -t NBACount -majc -minc -scan
True 
<enter> 
<enter> 
STRING 


quit
 
## code to run the code
bin/tool.sh ~/Desktop/mapreduce.jar WinnerLoser acc guestvb /input NBACount -u root -p acc
