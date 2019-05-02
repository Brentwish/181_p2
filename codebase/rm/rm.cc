
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
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    FileHandle fileHandle;
    string filename;
    RID rid; //will we ever care about rid in this function?
    void *record;
    int tablesId, columnsId;
    vector<Attribute> tablesRecDesc = getTablesRecordDescriptor();
    vector<Attribute> columnsRecDesc = getColumnsRecordDescriptor();

    //Create the Tables.tbl file
    filename = toFilename(TABLES_NAME);
    rbfm->createFile(filename);

    //Open it to insert an entry for itself and an entry for the colums table
    if (rbfm->openFile(filename, fileHandle) != SUCCESS) {
        perror("RelationManager: createTable() failed to open Tables.tbl");
        return -1;
    }

    //Populate a record with (table-id, table-name, file-name) in the correct format
    record = malloc(100);
    memset(record, 0, 100);
    tablesId = ++nextTableId;
    //cout << "id: " << id << endl;
    prepareTablesRecord(record, tablesId, TABLES_NAME, filename);
    //insert it into Tables
    rbfm->insertRecord(fileHandle, tablesRecDesc, record, rid);
    //rbfm->printRecord(tablesRecDesc, record);

    //Clear the memory
    memset(record, 0, 100);

    //Get the filename that we'll use for the Columns table
    filename = toFilename(COLUMNS_NAME);

    //Populate a record for the columns table
    columnsId = ++nextTableId;
    prepareTablesRecord(record, columnsId, COLUMNS_NAME, filename);
    rbfm->insertRecord(fileHandle, tablesRecDesc, record, rid);
    //rbfm->printRecord(tablesRecDesc, record);
    //done with record
    free(record);

    if (rbfm->closeFile(fileHandle)) {
        perror("RelationManager: createTable() failed to close Tables.tbl");
        return -1;
    }

    ////Now we need to populate the Columns table with each of the fields in
    //Tables and Columns
    //Create the Columns.tbl file
    rbfm->createFile(filename);

    //Open it
    if (rbfm->openFile(filename, fileHandle) != SUCCESS) {
        perror("RelationManager: createTable() failed to open Columns.tbl");
        return -1;
    }

    insertColumns(fileHandle, tablesId, tablesRecDesc);
    insertColumns(fileHandle, columnsId, columnsRecDesc);

    //close the fileHandle
    if (rbfm->closeFile(fileHandle)) {
        perror("RelationManager: createTable() failed to close Columns.tbl");
        return -1;
    }
    return 0;
}

int RelationManager::insertColumns(FileHandle &fileHandle, const int id, const vector<Attribute> &attrs) {
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RID rid; //will we ever care about rid in this function?
    Attribute attr;
    void *record;
    int numAttrs;
    vector<Attribute> columnsRecDesc = getColumnsRecordDescriptor();

    numAttrs = attrs.size();
    record = malloc(100);
    for (int i = 0; i < numAttrs; i++) {
        attr = attrs[i];
        memset(record, 0, 100);
        //setting column-position to be i+1 because assignment spec looks like
        //it should be
        prepareColumnsRecord(record, id, attr.name, attr.type, attr.length, i + 1);
        rbfm->insertRecord(fileHandle, columnsRecDesc, record, rid);
    }
    free(record);
    return 0;
}

RC RelationManager::deleteCatalog()
{
    RecordBasedFileManager * rbfm = RecordBasedFileManager::instance();
    string filename;
    filename = toFilename(TABLES_NAME);

    RC rc; 
    rc = rbfm->destroyFile(filename);
    if (rc) {
        return rc;
    }

    filename = toFilename(COLUMNS_NAME);
    rc = rbfm->destroyFile(filename);
    if (rc) {
        return rc;
    }
    
    return SUCCESS;   
}


//To create a table, we need to insert a record into the Tables table
//and we need to insert each attr into the columns table
RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    //cout << "   CREATE TABLE   " << endl;
    //cout << tableName << endl;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RID rid;
    FileHandle fileHandle;
    string filename = toFilename(tableName);
    void *record = malloc(100);
    int id;

    //Check file doesn't exist
    if (rbfm->checkFile(filename)) {
        perror("RelationManager: createTable() file already exists");
        return -1;
    }
    //Create the file
    if (rbfm->createFile(filename) != SUCCESS) {
        perror("RelationManager: createTable() failed to create file");
        return -1;
    }

    //Open the Tables table to insert the new table entry
    // (new_id, tableName, filename)
    if (rbfm->openFile(toFilename(TABLES_NAME), fileHandle) != SUCCESS) {
        perror("RelationManager: createTable() failed to open Tables.tbl");
        return -1;
    }

    //Insert the record
    //Populate record with (table-id, table-name, file-name) in the correct format
    id = ++nextTableId;
    prepareTablesRecord(record, id, tableName, filename);
    //insert it into Tables
    rbfm->insertRecord(fileHandle, getTablesRecordDescriptor(), record, rid);
    // void* data = malloc(PAGE_SIZE);
    // rbfm->readRecord(fileHandle, getTablesRecordDescriptor(), rid, data);
    // rbfm->printRecord(getTablesRecordDescriptor(), data);
    rbfm->printRecord(getTablesRecordDescriptor(), record);
    free(record);

    //close the fileHandle
    if (rbfm->closeFile(fileHandle)) {
        perror("RelationManager: createTable() failed to close Tables.tbl");
        return -1;
    }

    if (rbfm->openFile("Columns.tbl", fileHandle) != SUCCESS) {
        perror("RelationManager: createTable() failed to open Columns.tbl");
        return -1;
    }
    insertColumns(fileHandle, id, attrs);
    //close the fileHandle
    if (rbfm->closeFile(fileHandle)) {
        perror("RelationManager: createTable() failed to close Columns.tbl");
        return -1;
    }


    return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return -1;
}

int RelationManager::getTableId(const string &tableName) {
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RBFM_ScanIterator iterator;
    FileHandle fileHandle;
    RID rid;
    vector<Attribute> tableRecDesc;
    string condAttr;
    void *value, *data;
    int valueSize, tableNameLength, id;
    vector<string> attrNames;

    if (rbfm->openFile(toFilename(TABLES_NAME), fileHandle) != SUCCESS) {
        perror("RelationManager: getTableId() failed to open Tables.tbl");
        return -1;
    }

    // scan Tables for the unique tableName
    // Set up each of the following values to be passed to scan
    //     (fileHandle, tableRecDesc, condAttr, EQ_OP, tableName, ["table-id"], rbfm_si)

    tableRecDesc = getTablesRecordDescriptor();
    condAttr = "table-name";

    //We need to fill *value with tableName's length followed by tableName
    tableNameLength = tableName.length();
    valueSize = tableNameLength + INT_SIZE;
    value = malloc(valueSize);
    memset(value, 0, valueSize);
    memcpy(value, &tableNameLength, INT_SIZE);
    memcpy((char *)value + INT_SIZE, tableName.c_str(), tableNameLength);

    attrNames.push_back("table-id");

    rbfm->scan(fileHandle, tableRecDesc, condAttr, EQ_OP, value, attrNames, iterator);

    data = malloc(1 + INT_SIZE);

    iterator.getNextRecord(rid, data);
    //data contains a null byte for the attr plus the attr itself
    //we know this attr can't be null so we can ignore the byte
    iterator.close();
    free(value);
    // rbfm->printRecord(tableRecDesc, data);
    //close the fileHandle
    if (rbfm->closeFile(fileHandle)) {
        perror("RelationManager: getTableId() failed to close Tables.tbl");
        return -1;
    }

    memcpy(&id, (char *)data + 1, INT_SIZE);
    free(data);
    return id;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
    RBFM_ScanIterator iterator;
    Attribute attr;
    FileHandle fileHandle;
    RID rid;
    vector<Attribute> columnRecDesc;
    string condAttr;
    void *value, *data;
    vector<string> attrNames;
    int id;
    //Get the id of tableName from Tables
    //use it to key into columns
    id = getTableId(tableName);
    cout << "Found id: " << id << endl;

    if (rbfm->openFile(toFilename(COLUMNS_NAME), fileHandle) != SUCCESS) {
        perror("RelationManager: getAttributes() failed to open Columns.tbl");
        return -1;
    }

    columnRecDesc = getColumnsRecordDescriptor();
    condAttr = "table-id";
    value = malloc(INT_SIZE);
    memcpy(value, &id, INT_SIZE);
    attrNames.push_back("column-name");
    attrNames.push_back("column-type");
    attrNames.push_back("column-length");

    //search for the entry's with that id and append them to attrs
    rbfm->scan(fileHandle, columnRecDesc, condAttr, EQ_OP, value, attrNames, iterator);

    data = malloc(1+50+4+4);
    while (iterator.getNextRecord(rid, data) != RBFM_EOF) {
        //Need to fill attr with data
        //skip the null byte
        int offset = 1;

        //column-name length
        int columnNameLength = 0;
        memcpy(&columnNameLength, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;

        //column-name
        char *columnName = (char *) malloc(columnNameLength + 1);
        memset(columnName, '\0', columnNameLength + 1);
        memcpy(columnName, (char*) data + offset, columnNameLength);
        offset += columnNameLength;
        attr.name = columnName;
        free(columnName);

        //column-type
        int columnType = 0;
        memcpy(&columnType, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.type = (AttrType)columnType;

        //column-length
        int columnLength = 0;
        memcpy(&columnLength, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.length = (AttrLength)columnLength;

        attrs.push_back(attr);
    }


    if (rbfm->closeFile(fileHandle)) {
        perror("RelationManager: getAttributes() failed to close Columns.tbl");
        return -1;
    }
    free(data);
    free(value);
    return 0;

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

string RelationManager::toFilename(const string &tableName) {
    string filename = "";
    filename.append(tableName);
    filename.append(".tbl");
    return filename;
}

//Populates *buffer with a formatted Tables record
//returns the size of the buffer
//We're assuming none of these fields will be null
int RelationManager::prepareTablesRecord(void *buffer, const int tableId, const string &tableName, const string &fileName) {
    int offset = 0;

    //Null Field
    char nullByte = 0;
    memcpy(buffer, &nullByte, sizeof(nullByte));
    offset += sizeof(nullByte);

    //Table Id
    //cout << "offset " << offset << " tableId " << tableId << endl;
    memcpy((char *)buffer + offset, &tableId, INT_SIZE);
    offset += INT_SIZE;

    //Table name
    //cout << "offset " << offset << " tableName " << tableName << endl;
    const int tableNameLength = tableName.length();
    memcpy((char *)buffer + offset, &tableNameLength, INT_SIZE);
    offset += INT_SIZE;

    memcpy((char *)buffer + offset, tableName.c_str(), tableNameLength);
    offset += tableNameLength;

    //Table file name
    //cout << "offset " << offset << " fileName " << fileName<< endl;
    const int fileNameLength = fileName.length();
    memcpy((char *)buffer + offset, &fileNameLength, INT_SIZE);
    offset += INT_SIZE;

    memcpy((char *)buffer + offset, fileName.c_str(), fileNameLength);
    offset += fileNameLength;

    return offset;
}

//Populates *buffer with a formatted Columns record
//returns the size of the buffer
//We're assuming none of these fields will be null
int RelationManager::prepareColumnsRecord(void *buffer, const int tableId, const string &columnName, const int columnType, const int columnLength, const int columnPosition) {
    int offset = 0;

    //Null Field
    char nullByte = 0;
    memcpy(buffer, &nullByte, sizeof(nullByte));
    offset += sizeof(nullByte);

    //table=Id
    memcpy((char *)buffer + offset, &tableId, INT_SIZE);
    offset += INT_SIZE;

    //column-name
    const int columnNameLength = columnName.length();
    memcpy((char *)buffer + offset, &columnNameLength, INT_SIZE);
    offset += INT_SIZE;

    memcpy((char *)buffer + offset, columnName.c_str(), columnNameLength);
    offset += columnNameLength;

    //column-type
    memcpy((char *)buffer + offset, &columnType, INT_SIZE);
    offset += INT_SIZE;

    //column-length
    memcpy((char *)buffer + offset, &columnLength, INT_SIZE);
    offset += INT_SIZE;

    //column-position
    memcpy((char *)buffer + offset, &columnPosition, INT_SIZE);
    offset += INT_SIZE;

    return offset;
}

//Helper to get the Tables recordDescriptor
vector<Attribute> RelationManager::getTablesRecordDescriptor() {
    vector<Attribute> record;
    Attribute attr;

    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    record.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    record.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    record.push_back(attr);

    return record;
}

//Helper to get the Columns recordDescriptor
vector<Attribute> RelationManager::getColumnsRecordDescriptor() {
    vector<Attribute> record;
    Attribute attr;

    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    record.push_back(attr);

    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    record.push_back(attr);

    attr.name = "column-type";
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    record.push_back(attr);

    attr.name = "column-length";
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    record.push_back(attr);

    attr.name = "column-position";
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    record.push_back(attr);

    return record;
}
