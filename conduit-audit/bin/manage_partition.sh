#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#                                                                                                             #
#                                            manage_partition.sh                                              #
#                                  Script for creating partitions automatically                               #
#                                        Written By Sushil on : 2011-03-28                                    #
# NOTE :                                                                                                      #
# 1. Arguments Used                                                                                           #
#      $1 - Table name                                                                                        #
#      $2 - Column name                                                                                       #
#      $3 - Starting Month                                                                                    #
#      $4 - No of partitions                                                                                  #
#      $5 - DBuser to connect 
#  eg. ./manage_partition.sh daily_databus_summary timeinterval 2012-01-01 30 adarsh > /tmp/part_schema.sql   #
#                                                                                                             #
# 2. Save the output into a .sql file and execute it. This will create all partitions and respective indices  #
#                                                                                                             #
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

Prg=$(basename $0)
PartTable=$1
PartColumn=$2
StartPartMonth=$3
NoOfPart=$4
dbuser=$5
DbConnect="/usr/lib/postgresql/9.2/bin/psql -p5499 -U $dbuser databus_audit"

for (( i = 0; i < $NoOfPart; i++ ))
do

DatePrefix=`$DbConnect -t -c "SELECT to_char('$3'::DATE+interval '$i day','yyyymmdd')" | sed 's/^[ \t]*//'`
CurrPartDay=`$DbConnect -t -c "SELECT to_char('$3'::DATE + interval '$i day','yyyy-mm-dd')" | sed 's/^[ \t]*//'`
NextPartDay=`$DbConnect -t -c "SELECT to_char('$CurrPartDay'::DATE + interval '1 day','yyyy-mm-dd')" | sed 's/^[ \t]*//'`

CreatePartTab=$($DbConnect -t <<EOF
SELECT 'CREATE TABLE $PartTable$DatePrefix ( CHECK($PartColumn >= extract ('||'epoch'||' from timestamp '||'''$CurrPartDay'''||')*1000::bigint AND $PartColumn < extract ('||'epoch'|| ' from timestamp '||'''$NextPartDay'''||')*1000::bigint ))  inherits ($PartTable);';
\q
EOF
)

CreatePartInd=$($DbConnect -t <<EOF
SELECT 'CREATE INDEX $PartTable$DatePrefix'||'_idx'||' ON $PartTable$DatePrefix USING btree ($PartColumn);';
\q
EOF
)

CreatePartInd1=$($DbConnect -t <<EOF
SELECT 'CREATE INDEX $PartTable$DatePrefix'||'_clusteridx'||' ON $PartTable$DatePrefix USING btree (cluster);';
\q
EOF
)

CreatePartInd2=$($DbConnect -t <<EOF
SELECT 'CREATE INDEX $PartTable$DatePrefix'||'_topicidx'||' ON $PartTable$DatePrefix USING btree (topic);';
\q
EOF
)

CreatePartInd3=$($DbConnect -t <<EOF
SELECT 'CREATE INDEX $PartTable$DatePrefix'||'_hostnameidx'||' ON $PartTable$DatePrefix USING btree (hostname);';
\q
EOF
)

CreatePartInd4=$($DbConnect -t <<EOF
SELECT 'CREATE INDEX $PartTable$DatePrefix'||'_tieridx'||' ON $PartTable$DatePrefix USING btree (tier);';
\q
EOF
)

CreatePartAlter=$($DbConnect -t <<EOF
SELECT  'ALTER TABLE $PartTable$DatePrefix owner to databus_user;';
\q
EOF
)

echo ""
echo "-- Create Partition : $PartTable$DatePrefix"
echo $CreatePartTab
echo $CreatePartInd
echo $CreatePartInd1
echo $CreatePartInd2
echo $CreatePartInd3
echo $CreatePartInd4
echo $CreatePartAlter
done
                                   
