#!/bin/sh

# migrate_databus_to_conduit.sh : migrate databus tables to conduit
#
# Usage:
#     create "conduit_user" user in postgres (update in feeder and visualization)
#     $ export PGPASSWORD=<password>
#     $ ./migrate_databus_to_conduit.sh <db> <dbuser> <host> <port> 

db=$1
dbuser=$2
host=$3
port=$4
DbConnect="/usr/bin/psql -A -t -R \n -p $port -h $host $db $dbuser"

tables=$($DbConnect <<EOF
select tablename from pg_tables where tableowner = '$dbuser'
\q
EOF
)

tables=$(echo $tables | grep databus_summary | sort -u)

for table in $tables
do
	fromTable="$table"
	toTable=$(echo $fromTable | perl -i -p -e 's:databus:conduit:g')
	echo "Renaming Table: $fromTable -> $toTable"
	cmd="ALTER TABLE $fromTable RENAME TO $toTable"
	rename=$($DbConnect <<EOF
$cmd
\q
EOF
)
	echo $rename
done

echo "Granting priviledges to conduit_user"
grant=$($DbConnect <<EOF
GRANT ALL ON TABLE daily_conduit_summary TO conduit_user
\q
EOF
)

echo $grant
