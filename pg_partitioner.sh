#!/bin/bash
#
# @author 
#
# DESCRIPTION: Create 
# CREATION: 0.1 Sep 2017


function numberOfNodes {
	NONODES=`sudo -u postgres  psql -d alfresco -t -c "select max(node_id) from alf_node_properties"`
	echo "$NONODES";
}

### VARS

NINODES=4000
COUNT_NODES=$(numberOfNodes)
let " PARTITIONS = ($COUNT_NODES / $NINODES) + 1 "
echo "$PARTITIONS"
DBNAME="alfresco"

DUMP_DIR=/tmp/$DBNAME/$DBNAME_`date +%Y%m%d%H%M%S`.dump
RESTORE_DIR=$DUMP_DIR
#RESTORE_DIR=/tmp/alfresco/20170922144817.dump

### NUMBER OF NODES 

function dumpDB {
	echo "Dumping DB..."
	mkdir -p  /tmp/$DBNAME
	sudo -u postgres pg_dump $DBNAME > $DUMP_DIR
	echo "Done!"
}

function restoreDB {
	echo "Restoring DB..."
	sudo -u postgres psql -t -c "drop database $DBNAME;"
    sudo -u postgres psql -t -c "create database $DBNAME with encoding 'utf8';"
	sudo -u postgres psql $DBNAME < $RESTORE_DIR
	echo "Done!"	
}

function createMasterTable {
	echo "Creating Master Table ..."

	sudo -u postgres psql -d alfresco -t -c "CREATE TABLE alf_node_properties_intermediate (LIKE alf_node_properties INCLUDING ALL);"
	sudo -u postgres psql -d alfresco -t -c "CREATE FUNCTION alf_node_properties_insert_trigger()
    						     RETURNS trigger AS \$\$ 
    					   	     BEGIN 
        						RAISE EXCEPTION 'Create partitions first.'; 
    						     END;
    						     \$\$ LANGUAGE plpgsql;"
	sudo -u postgres psql -d alfresco -t -c "CREATE TRIGGER alf_node_properties_insert_trigger 
    						     BEFORE INSERT ON alf_node_properties_intermediate 
    						     FOR EACH ROW EXECUTE PROCEDURE alf_node_properties_insert_trigger();"
	echo "Done!"
}

function createPartitions {
	echo "Creating Partitions ..."

	for i in `seq 1 $PARTITIONS`;
	do
	 let " PART_NAME = ($i * $NINODES)"
	 let " MIN_LEVEL = ($i-1)*$NINODES "
	 let " MAX_LEVEL = $MIN_LEVEL + $NINODES "
	 sudo -u postgres psql -d alfresco -t -c "CREATE TABLE alf_node_properties_$PART_NAME
                                                    (CHECK (node_id > $MIN_LEVEL AND node_id <= $MAX_LEVEL))
                                                    INHERITS (alf_node_properties_intermediate);"
	 sudo -u postgres psql -d alfresco -t -c "ALTER TABLE alf_node_properties_$PART_NAME ADD PRIMARY KEY (node_id, qname_id, list_index, locale_id);"
	 sudo -u postgres psql -d alfresco -t -c "CREATE INDEX fk_alf_nprop_n_$PART_NAME ON alf_node_properties_$PART_NAME (node_id);"
	 sudo -u postgres psql -d alfresco -t -c "CREATE INDEX fk_alf_nprop_qn_$PART_NAME ON alf_node_properties_$PART_NAME (qname_id);"
	 sudo -u postgres psql -d alfresco -t -c "CREATE INDEX fk_alf_nprop_loc_$PART_NAME ON alf_node_properties_$PART_NAME (locale_id);"
	 sudo -u postgres psql -d alfresco -t -c "CREATE INDEX idx_alf_nprop_s_$PART_NAME ON alf_node_properties_$PART_NAME (qname_id, string_value, node_id);"
	 sudo -u postgres psql -d alfresco -t -c "CREATE INDEX idx_alf_nprop_l_$PART_NAME ON alf_node_properties_$PART_NAME (qname_id, long_value, node_id);"
	 sudo -u postgres psql -d alfresco -t -c "CREATE INDEX idx_alf_nprop_b_$PART_NAME ON alf_node_properties_$PART_NAME (qname_id, boolean_value, node_id);"
	 sudo -u postgres psql -d alfresco -t -c "CREATE INDEX idx_alf_nprop_f_$PART_NAME ON alf_node_properties_$PART_NAME (qname_id, float_value, node_id);"
	 sudo -u postgres psql -d alfresco -t -c "CREATE INDEX idx_alf_nprop_d_$PART_NAME ON alf_node_properties_$PART_NAME (qname_id, double_value, node_id);"
	done
	
	echo "Done!"
}

function triggerInsertRows {

	echo "Creating Trigger ..."


	for i in `seq 1 $PARTITIONS`;
	do
	 let " PART_NAME = ($i * $NINODES)"
	 let " MIN_LEVEL = ($i-1)*$NINODES "
	 let " MAX_LEVEL = $MIN_LEVEL + $NINODES "
	 if [ "$i" = "$PARTITIONS" ]
	 then
	 	body="IF (NEW.node_id > $MIN_LEVEL AND NEW.node_id <= $MAX_LEVEL) THEN INSERT INTO alf_node_properties_$PART_NAME VALUES (NEW.*); $body"
	 else
	    body="ELSIF (NEW.node_id > $MIN_LEVEL AND NEW.node_id <= $MAX_LEVEL) THEN INSERT INTO alf_node_properties_$PART_NAME VALUES (NEW.*); $body "
	 fi
	done

	echo "$body"

	sudo -u postgres psql -d alfresco -t -c "CREATE OR REPLACE FUNCTION alf_node_properties_insert_trigger()
	        RETURNS trigger AS
			  \$\$
			  BEGIN 
			  $body
			  ELSE
			      RAISE EXCEPTION 'Ensure partitions are created';
			  END IF;
			  RETURN NULL;
			  END;
              \$\$ LANGUAGE plpgsql;"	

    echo "Done!"

}

function insertion {

    echo "Inserting rows ..."

	for i in `seq 1 $PARTITIONS`;
	do

	 let " MIN_LEVEL = ($i - 1)*$NINODES "
	 echo $MIN_LEVEL
	 let " MAX_LEVEL = $MIN_LEVEL + $NINODES "
	 echo $MAX_LEVEL

	 sudo -u postgres psql -d alfresco -t -c "INSERT INTO alf_node_properties_intermediate (
    							node_id,
    							actual_type_n,
    							persisted_type_n,
    							boolean_value,
    							long_value,
    							float_value,
    							double_value,
    							string_value,
    							serializable_value,
    							qname_id,
    							list_index,
    							locale_id
						)
							SELECT node_id,
							actual_type_n,
							persisted_type_n,
							boolean_value,
							long_value,
							float_value,
							double_value,
							string_value,
							serializable_value,
							qname_id,
							list_index,
							locale_id 
							FROM alf_node_properties
							WHERE node_id > $MIN_LEVEL AND node_id <= $MAX_LEVEL;"
	done

 	echo "Done!"
}

function analyze {

    echo "Analyzing new tables..."
    for i in `seq 1 $PARTITIONS`;
	do
	 let " PART_NAME = ($i * $NINODES)"
	 sudo -u postgres psql -d alfresco -t -c "ANALYZE VERBOSE alf_node_properties_$PART_NAME" 
    done
    sudo -u postgres psql -d alfresco -t -c "ANALYZE VERBOSE alf_node_properties_intermediate" 
    echo "Done!"
}

function commit {
    sudo -u postgres psql -d alfresco -t -c "ALTER TABLE alf_node_properties RENAME TO alf_node_properties_retired"
    sudo -u postgres psql -d alfresco -t -c "ALTER TABLE alf_node_properties_intermediate RENAME TO alf_node_properties"
    sudo -u postgres psql -d alfresco -t -c "DROP TABLE alf_node_properties_retired"
    sudo -u postgres psql -d alfresco -t -c "GRANT ALL PRIVILEGES ON TABLE alf_node_properties TO alfresco"
    for i in `seq 1 $PARTITIONS`;
	do
	 let " PART_NAME = ($i * $NINODES)"
	 sudo -u postgres psql -d alfresco -t -c "GRANT ALL PRIVILEGES ON TABLE alf_node_properties_$PART_NAME TO alfresco" 
    done
}

## EXECUTION 

#dumpDB
#numberOfNodes
#restoreDB
#createMasterTable
#createPartitions
#triggerInsertRows
#insertion
#analyze
#commit
