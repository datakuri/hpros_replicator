#author: Datakuri
#Date:05/2018
#Description:
#Moves data in and out of Azure General Purpose V2 storage to Hadoop Cluster
#Primary Functions: export and import
#Uses configuration files to store wasb keys (Owned by root/backup svc account)
#Needs backup svc account access to execute
#Example usage
#./datamove.sh export /tmp/testhdfs dir db_hdfs_path conf_datamove_dev_wasb.conf
#./datamove.sh import /tmp/testhdfs dir db_hdfs_path conf_datamove_dev_wasb.conf
#./datamove.sh export hive_db_name db db_hdfs_path conf_datamove_dev_wasb.conf
#./datamove.sh import hive_db_name db db_hdfs_path conf_datamove_dev_wasb.conf
#/data0/sysapps/backup/scripts/dataCopy/datamove.sh export hive_db_name db /hive_db/data/path conf_datamove_dev_wasb1.conf
##########################################################################################################
if [ $# -ne 5 ]
then
	echo -e "\nError: Exactly three arguments are allowed. Like export dbtest db OR import dbtest dir\n"
	exit 1
elif [[ ! "$1" =~ ^(export|import)$ ]]
then
	echo -e "\nError: First argument needs to be export or import. Full argument set like export dbtest db OR import dbtest dr\n"
	exit 1
elif [[ ! "$3" =~ ^(db|dir)$ ]]
then
	echo -e "\nError: Last argument needs to be db or dr. Full argument set like export dbtest db OR import dbtest dr\n"
	exit 1
fi

#bl_exp="beeline --showWarnings=false --showHeader=false --silent=true --outputformat=tsv2 -u 'jdbc:hive2://hiveserver2host:10000/default/;principal=hive/hiveserver2@realm;ssl=true;sslTrustStore=/opt/cloudera/security/jks/truststore.jks'"
baseDir=/data0/sysapps/backup/scripts/dataCopy
operation=$1
hiveDBName=$2
copyType=$3
hiveDataPath=$4
confFile=$5

function logsetup {
	ts=$(date +%Y_%m_%d_%H_%M_%S)
	LOGFILE="/data0/logs/backup/${copyType}_${operation}_${hiveDBName}_$ts.log"
	exec > >(tee -a $LOGFILE)
	exec 2>&1
}

function log {
	echo "[$(date +%Y/%m/%d:%H:%M:%S)]: $*"
}

function error_test {
        if test $1 -gt 0
        then
                log "$2 Command execution FAILED: status: $1"
                #cat $log_file | mail -s "Failed: load job FAILED for file date: $filedate" $alert_email  >> $log_file 2>&1
                stat_cnt=`expr $1 + 1`
                #exit -1
        fi
}
function send_alert {
  log "Sending an email alert with status to:$1"
  
  if test $2 -gt 0
  then
          log "$3 Job FAILED: status: $2"
          echo "$3 Job FAILED: status: $2" | mail -s "$3 Job FAILED: status: $2" $1
  else
          log "$3 Job Success: status: $2"
          echo "$3 Job Success: status: $2" | mail -s "$3 Job Success: status: $2" $1 
  fi
  exit $2 
}

function exportDB {

  if [ -e $hiveDBName/${hiveDBName}_all_tables_DDL.txt ]
	then
    mv $hiveDBName/${hiveDBName}_all_tables_DDL.txt $hiveDBName/${hiveDBName}_all_tables_DDL_$(date +%Y_%m_%d_%H_%M_%S).txt
    error_test $? "Take previous day DDL file backup with current date"
	fi
	
  
  log "Step1: Getting list of tables"

	$bl_exp -e "USE $hiveDBName; SHOW tables;" > $hiveDBName/${hiveDBName}_alltables.txt
  error_test $? "Extract All tables DDLs from beeline" 
  
	log "Step2: Fetching Hive table DDLs"

	#log "DROP DATABASE IF EXISTS $hiveDBName CASCADE;" > $hiveDBName/${hiveDBName}_all_tables_DDL.txt
  #echo "DROP DATABASE IF EXISTS $hiveDBName CASCADE;" > $hiveDBName/${hiveDBName}_all_tables_DDL.txt
	log "CREATE DATABASE $hiveDBName location '${hiveDataPath}' ;>> $hiveDBName/${hiveDBName}_all_tables_DDL.txt"
  echo "CREATE DATABASE $hiveDBName location '${hiveDataPath}';" >> $hiveDBName/${hiveDBName}_all_tables_DDL.txt 
  #echo "CREATE DATABASE $hiveDBName LOCATION;" >> $hiveDBName/${hiveDBName}_all_tables_DDL.txt
  
  log "USE $hiveDBName;"
  echo "USE $hiveDBName;" >> $hiveDBName/${hiveDBName}_all_tables_DDL.txt

	while read line
	do
		log "Processing table $line"
		$bl_exp -e "USE $hiveDBName; SHOW CREATE TABLE ${hiveDBName}.$line;" >> $hiveDBName/${hiveDBName}_all_tables_DDL.txt
    error_test $? "Extract table DDL ${hiveDBName}.$line from beeline" 
		echo ";" >> $hiveDBName/${hiveDBName}_all_tables_DDL.txt

		$bl_exp -e "USE $hiveDBName; SHOW PARTITIONS $line;" > $hiveDBName/tmp_part.txt
		while read tablepart
		do
			partname=`echo ${tablepart/=/=\"}`
			echo "ALTER TABLE ${hiveDBName}.$line ADD PARTITION ($partname\");" >> $hiveDBName/${hiveDBName}_all_tables_DDL.txt
			echo "ANALYZE TABLE ${hiveDBName}.$line PARTITION ($partname\") COMPUTE STATISTICS;" >> $hiveDBName/${hiveDBName}_all_tables_DDL.txt
		done < $hiveDBName/tmp_part.txt

		checklib=$($bl_exp -e "USE $hiveDBName; DESCRIBE EXTENDED $line;" |grep serializationLib)
		if [[ $checklib == *"OpenCSVSerde"* ]]
		then
			echo "ALTER TABLE ${hiveDBName}.$line SET SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';" >> $hiveDBName/${hiveDBName}_all_tables_DDL.txt
			escapechar="\"escapeChar\"=\"\\\\\""
			quotechar="\"quoteChar\"=\"\\\"\""
			seperatorchar="\"separatorChar\"=\",\""
			echo "ALTER TABLE ${hiveDBName}.$line SET SERDEPROPERTIES ($escapechar, $quotechar, $seperatorchar);" >> $hiveDBName/${hiveDBName}_all_tables_DDL.txt
		fi

	done <	$hiveDBName/${hiveDBName}_alltables.txt

	sed -i.bak '/LOCATION/,+1 d' $hiveDBName/${hiveDBName}_all_tables_DDL.txt

	log " Wrote DDLs to $hiveDBName/${hiveDBName}_all_tables_DDL.txt"

	if [ -e /tmp/${hiveDBName}_all_tables_DDL.txt ]
	then
		rm -f /tmp/${hiveDBName}_all_tables_DDL.txt
    error_test $? "remove all_tables_DDL file from tmp location"
		hdfs dfs -rm -skipTrash /tmp/${hiveDBName}_all_tables_DDL.txt
    error_test $? "remove all_tables_DDL file from HDFS tmp location"
	fi

	cp $hiveDBName/${hiveDBName}_all_tables_DDL.txt /tmp
  error_test $? "copying all_tables_DDL file to tmp location"

	hdfs dfs -put /tmp/${hiveDBName}_all_tables_DDL.txt /tmp/${hiveDBName}_all_tables_DDL.txt
  error_test $? "copying all_tables_DDL file to HDFS tmp location" 

  log "********************distcp command starts***********************************************"
	log "hadoop distcp -D hadoop.security.credential.provider.path=${hadoop_sec_cred_path} -p -update hdfs:///tmp/${hiveDBName}_all_tables_DDL.txt ${wasbbucket}/${hiveDBName}/${hiveDBName}_all_tables_DDL.txt"
  hadoop distcp -D hadoop.security.credential.provider.path=${hadoop_sec_cred_path} -p -update hdfs:///tmp/${hiveDBName}_all_tables_DDL.txt ${wasbbucket}/${hiveDBName}/${hiveDBName}_all_tables_DDL.txt
  error_test $? "Export DB DDL ${hiveDBName} to backup"

  log "hadoop distcp -D hadoop.security.credential.provider.path=${hadoop_sec_cred_path} -p -update hdfs://${hiveDataPath} ${wasbbucket}/${hiveDBName}/data/"
	hadoop distcp -D hadoop.security.credential.provider.path=${hadoop_sec_cred_path} -p -update hdfs://${hiveDataPath} ${wasbbucket}/${hiveDBName}/data/
  #log "Export DB ${hiveDBName} to backup Status:$? 0:Successful 1:Failed"
  error_test $? "Export DB Data ${hiveDBName} to backup"  
}

function exportDIR {

	log "Step1:Running disributed copy export DIR"
	hadoop distcp -D hadoop.security.credential.provider.path=${hadoop_sec_cred_path} -p -update hdfs:///${hiveDataPath} ${wasbbucket}/${hiveDataPath}
	error_test $? "Export Directory Data ${hiveDataPath} to backup"
}


function importDB {
	log "Step1: Running disributed copy. import DB"
  log "hadoop distcp	-D hadoop.security.credential.provider.path=${hadoop_sec_cred_path} -p -update ${wasbbucket}/${hiveDBName}/${hiveDBName}_all_tables_DDL.txt hdfs:///tmp/${hiveDBName}_all_tables_DDL.txt" 
	hadoop distcp	-D hadoop.security.credential.provider.path=${hadoop_sec_cred_path} -p -update ${wasbbucket}/${hiveDBName}/${hiveDBName}_all_tables_DDL.txt hdfs:///tmp/${hiveDBName}_all_tables_DDL.txt
  error_test $? "Importing All tables DDLs from backup to restore HDFS tmp location"
  log "Getting DDL from backup to HDFS tmp Status:$? 0:Successful 1:Failed"

	log "Step2: Running database import."

	if [ -e /tmp/${hiveDBName}_all_tables_DDL.txt ]
	then
		rm -f /tmp/${hiveDBName}_all_tables_DDL.txt
    error_test $? "remove All tables DDLs if exist from restore OS tmp location"
	fi
  log "hdfs dfs -get /tmp/${hiveDBName}_all_tables_DDL.txt /tmp/${hiveDBName}_all_tables_DDL.txt"
	hdfs dfs -get /tmp/${hiveDBName}_all_tables_DDL.txt /tmp/${hiveDBName}_all_tables_DDL.txt
  error_test $? "copy All tables DDLs from HDFS temp to restore tmp location"

	if [ ! -e $hiveDBName ]
	then
		mkdir $hiveDBName
	fi

	if [ -e $hiveDBName/${hiveDBName}_all_tables_DDL.txt ]
	then
		rm -f $hiveDBName/${hiveDBName}_all_tables_DDL.txt
	fi
	chmod 755 /tmp/${hiveDBName}_all_tables_DDL.txt

	cp -f /tmp/${hiveDBName}_all_tables_DDL.txt $hiveDBName/${hiveDBName}_all_tables_DDL.txt
  error_test $? "copy All tables DDLs from OS temp to restore db location"

	$bl_exp -f /tmp/${hiveDBName}_all_tables_DDL.txt
  error_test $? " Running DDLs on Hive"
	cat /tmp/${hiveDBName}_all_tables_DDL.txt | grep ANALYZE > /tmp/${hiveDBName}_all_tables_ANALYZE.txt

  log "Step3: Getting data from backup to database import."
  log "hadoop distcp -D hadoop.security.credential.provider.path=${hadoop_sec_cred_path} -p -update ${wasbbucket}/${hiveDBName}/data/ hdfs://${hiveDataPath}"
	hadoop distcp -D hadoop.security.credential.provider.path=${hadoop_sec_cred_path} -p -update ${wasbbucket}/${hiveDBName}/data/ hdfs://${hiveDataPath}
  error_test $? "Import DB data ${hiveDBName} from backup Status"
  
	hdfs dfs -chown -R hive:hive ${hiveDataPath}
#	$bl_exp -f /tmp/${hiveDBName}_all_tables_DDL.txt
#	cat /tmp/${hiveDBName}_all_tables_DDL.txt | grep ANALYZE > /tmp/${hiveDBName}_all_tables_ANALYZE.txt
	$bl_exp -f /tmp/${hiveDBName}_all_tables_ANALYZE.txt
}

function importDIR {
	sudo -u hdfs hadoop distcp -D fs.wasba.server-side-encryption-algorithm=${fs_wasba_server_side_encryption_algorithm} \
	-D fs.wasba.secret.key=${fs_wasba_secret_key} \
	-D fs.wasba.access.key=${fs_wasba_access_key} \
	-p -update ${bucket}/${formattedDirName} hdfs:///${formattedDirName}
	sudo -u hdfs hdfs dfs -chown -R hive:hdfs /${formattedDirName}
}


##############
#MAIN
##############

cd $baseDir

if [ $copyType = 'dir' ]
then
	formattedDirName=$(echo $hiveDBName | sed 's/^\///')
	hiveDBName=$(basename $hiveDBName)
fi
if [ ! -e $hiveDBName ]
then
	mkdir $hiveDBName
fi

logsetup
stat_cnt=0

log "$copyType $hiveDBName copy initiation..."
log "Given params: operation:${operation}, hiveDBName:${hiveDBName}, copyType:${copyType}, hiveDataPath In Backup:${hiveDataPath}, confFile:${confFile} "

log "Reading config params from given config..."
. conf/$confFile
log "wasbbucket: ${wasbbucket}"
log "beeline shell: ${bl_exp_conf}"
log "hadoop cred path: ${hadoop_sec_cred_path}"
log "send alerts to: ${alert_emails}"
bl_exp="${bl_exp_conf}"

if [ $operation = 'export' ]
then
	log "$copyType $hiveDBName export initiation..."
	if [ $copyType = 'db' ]
	then
		log "calling export DB"
    exportDB
	else
		log "calling export DIR"
    		exportDIR
	fi
	log "$hiveDBName Export Processing finished."
else
	log "$copyType $hiveDBName import initiation..."
	if [ $copyType = 'db' ]
	then
		log "calling Import DB"
    importDB
	else
    log "calling Import Dir"  
		#importDIR
	fi
  log "$hiveDBName Import Processing finished." 
fi
log "Backup $hiveDBName $copyType $operation processing finished. Please verify the objects"
send_alert ${alert_emails} ${stat_cnt} "Backup $hiveDBName $copyType $operation "  

 


