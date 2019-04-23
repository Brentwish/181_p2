
#include "rm.h"

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
}

RelationManager::~RelationManager()
{
}

//This method creates two system catalog tables, Tables and Columns. If they already exist, return
//an error. The actual files for these two tables should be created, and tuples describing themselves
//should be inserted into these tables as shown earlier in the catalog section. 
//
//We need a way to prevent "users" from modifying Tables.tbl and Columns.tbl
//only calls from within should be able to modify these tables
//createCatalog should populate these two tables with these records:
//Tables
//  (1, "Tables", "Tables.tbl")
//  (2, "Columns", "Columns.tbl")
//  Columns
//  (1, "table-id", TypeInt, 4 , 1)
//  (1, "table-name", TypeVarChar, 50, 2)
//  (1, "file-name", TypeVarChar, 50, 3)
//  (2, "table-id", TypeInt, 4, 1)
//  (2, "column-name", TypeVarChar, 50, 2)
//  (2, "column-type", TypeInt, 4, 3)
//  (2, "column-length", TypeInt, 4, 4)
//  (2, "column-position", TypeInt, 4, 5)
RC RelationManager::createCatalog() {
    FileHandle fileHandle;
    vector<Attribute> tables_attrs;

    //Create the file
    rbfm->createFile("Tables.tbl");
    //Open the file
    rbfm->openFile("Tables.tbl", fileHandle);

    //Prepare the record descriptor
    Attribute attr;
    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = SIZE_INT;
    tables_attrs.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    tables_attrs.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = 50;
    tables_attrs.push_back(attr);


    rbfm->insertTuple(fileHandle, tables_attrs, 
    rbfm->closeFile(fileHandle);

    //error if Tables or Columns already exist
    //create "Tables" and "Columns" file
    //rbfm->createFile("Tables.tbl")
    //rbfm->createFile("Columns.tbl")
    //
    //open these files
    //rbfm->openFile("Tables.tbl")
    //rbfm->openFile("Columns.tbl")
    //
    //prepareTuple(
    return -1;
}

RC RelationManager::deleteCatalog()
{
    return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    return -1;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    return -1;
}



