#!/bin/bash

### DEFAULT VALUES
DEFAULT_NODES_PER_PARTITION=4000

function numberOfNodes {
	NONODES=`sudo -u postgres  psql -d alfresco -t -c "select max(node_id) from alf_node_properties"`
	echo "$NONODES";
}

### VARS
if [[ -z $2 ]]
then
    NODES_PER_PARTITION=DEFAULT_NODES_PER_PARTITION
else
	NODES_PER_PARTITION=$2
fi	

COUNT_NODES=$(numberOfNodes)
let " PARTITIONS = ($COUNT_NODES / $NODES_PER_PARTITION) + 1 "
echo "$PARTITIONS partitions detected"
DBNAME="alfresco"

DUMP_DIR=/tmp/$DBNAME/$DBNAME_`date +%Y%m%d%H%M%S`.dump
RESTORE_DIR=$DUMP_DIR

### FUNCTIONS
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

	echo "... table alf_node_properties_intermediate created!"

}

function createPartitions {

	echo "Creating Partitions ..."

	for i in `seq 1 $PARTITIONS`;
	do
	 let " PART_NAME = ($i * $NODES_PER_PARTITION)"
	 let " MIN_LEVEL = ($i-1)*$NODES_PER_PARTITION "
	 let " MAX_LEVEL = $MIN_LEVEL + $NODES_PER_PARTITION "
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

    echo "Partition alf_node_properties_$PART_NAME created"
	 
	done
	
	echo "$PARTITIONS has been created!"
}

function addPartition {

	echo "Add new partition ..."

	let " PART_NAME = ($PARTITIONS + 1) * $NODES_PER_PARTITION"
	let " MIN_LEVEL = $PARTITIONS * $NODES_PER_PARTITION "
	let " MAX_LEVEL = $MIN_LEVEL + $NODES_PER_PARTITION "

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

    echo "Partition alf_node_properties_$PART_NAME created"

    let " PARTITIONS = $PARTITIONS + 1 "

    triggerInsertRows
	 
}

function triggerInsertRows {

	echo "Creating Trigger..."

	for i in `seq 1 $PARTITIONS`;
	do
	 let " PART_NAME = ($i * $NODES_PER_PARTITION)"
	 let " MIN_LEVEL = ($i-1)*$NODES_PER_PARTITION "
	 let " MAX_LEVEL = $MIN_LEVEL + $NODES_PER_PARTITION "
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

    echo "Trigger alf_node_properties_insert_trigger has been created!"

}

function fill {

    echo "Copying rows..."

	for i in `seq 1 $PARTITIONS`;
	do

	 let " MIN_LEVEL = ($i - 1)*$NODES_PER_PARTITION "
	 echo $MIN_LEVEL
	 let " MAX_LEVEL = $MIN_LEVEL + $NODES_PER_PARTITION "
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
	
    echo "Rows for partition $i has been copied"

	done

 	echo "All rows has been copied!"
}

function analyze {

    echo "Analyzing new tables..."
    for i in `seq 1 $PARTITIONS`;
	do
	 let " PART_NAME = ($i * $NODES_PER_PARTITION)"
	 sudo -u postgres psql -d alfresco -t -c "ANALYZE VERBOSE alf_node_properties_$PART_NAME" 
    done
    sudo -u postgres psql -d alfresco -t -c "ANALYZE VERBOSE alf_node_properties_intermediate" 

    echo "Tables has been analyzed"
}

function swap {

	echo "Swapping tables..."

    sudo -u postgres psql -d alfresco -t -c "ALTER TABLE alf_node_properties RENAME TO alf_node_properties_retired"
    sudo -u postgres psql -d alfresco -t -c "ALTER TABLE alf_node_properties_intermediate RENAME TO alf_node_properties"
    sudo -u postgres psql -d alfresco -t -c "DROP TABLE alf_node_properties_retired"
    sudo -u postgres psql -d alfresco -t -c "GRANT ALL PRIVILEGES ON TABLE alf_node_properties TO alfresco"
    for i in `seq 1 $PARTITIONS`;
	do
	 let " PART_NAME = ($i * $NODES_PER_PARTITION)"
	 sudo -u postgres psql -d alfresco -t -c "GRANT ALL PRIVILEGES ON TABLE alf_node_properties_$PART_NAME TO alfresco" 
    done

	echo "Partitioning is ready!"

}

function unswap {

	echo "Undo swapping..."

# To be done

	echo "... currently not implemented!"

}

## EXECUTION 
while :; do
    case $1 in
        create-master) 
            createMasterTable
            break
        ;;
        create-partitions)
            createPartitions
            break
        ;;
        create-trigger) 
            triggerInsertRows
            break
        ;;
        fill)
            fill
            break
        ;;
        analyze)
            analyze
            break
        ;;
        swap)
            swap
            break
        ;;
        unswap)
            unswap
            break
        ;;
        add-partition)
            addPartition
            break
        ;;
        count-nodes)
            echo "Number of nodes: $NONODES" 
            break
        ;;
        dump)
            dumpDB
            if [[ ! -z $2 ]]
			then
			    DUMP_DIR=$2
			fi	
            break
        ;;
        restore)
            restoreDB
            if [[ ! -z $2 ]]
			then
			    RESTORE_DIR=$2
			fi	
            break
        ;;
        *) echo "USAGE: 
           pg_partitioner.sh [create-master | create-partitions | create-trigger | fill | analyze | swap] [nodesPerPartition]
           pg_partitioner.sh [unswap | count-nodes]
           pg_partitioner.sh [dump | restore] [folder-path]"
           break
    esac
    shift
done