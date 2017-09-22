
Alfresco Node Partitioner
================================================

ALF_NODE_PROPERTIES table stores every metadata value for Alfresco objects: contents, folders, users... Commonly, this table is filled with millions of rows, as it stores about 30 properties per node. Using a table partitioning approach improves performance and reduces maintenance operations. This project provides a shell tool to perform table partitioning on living or brand new Alfresco PostgreSQL databases and also a guide for automatic partition generation. 

A simple "Node Id" partitioning pattern is currently provided.

**License**
The plugin is licensed under the [LGPL v3.0](http://www.gnu.org/licenses/lgpl-3.0.html). 

**State**
Current release is 1.0.0

**Compatibility** 
The current version has been developed using Postgresql 9.4.8

**Currently not working**

Available functions
--------------------------------------
dumpDB
numberOfNodes
createMasterTable
createPartitions
triggerInsertRows
insertion


Using the script
----------------------
TBD

