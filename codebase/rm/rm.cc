
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
    int tablesId, columnsId, recordSize;
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
    tablesId = nextTableId++;
    //cout << "id: " << id << endl;
    //recordSize is not needed for anything
    recordSize = prepareTablesRecord(record, tablesId, TABLES_NAME, filename);
    //insert it into Tables
    rbfm->insertRecord(fileHandle, tablesRecDesc, record, rid);
    //rbfm->printRecord(tablesRecDesc, record);

    //Clear the memory
    memset(record, 0, 100);

    //Get the filename that we'll use for the Columns table
    filename = toFilename(COLUMNS_NAME);

    //Populate a record for the columns table
    columnsId = nextTableId++;
    recordSize = prepareTablesRecord(record, columnsId, COLUMNS_NAME, filename);
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
        //rbfm->printRecord(columnsRecDesc, record);
    }
    free(record);
    return 0;
}

RC RelationManager::deleteCatalog()
{
    return -1;
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
    int recordSize, id;

    //Check file doesn't exist
    if (rbfm->checkFile(filename)) {
        perror("RelationManager: createTable() file already exists");
        return -1;
    }
    //Create the file
    if (rbfm->createFile(tableName) != SUCCESS) {
        perror("RelationManager: createTable() failed to create file");
        return -1;
    }

    //Open the Tables table to insert the new table entry
    // (new_id, tableName, filename)
    if (rbfm->openFile("Tables.tbl", fileHandle) != SUCCESS) {
        perror("RelationManager: createTable() failed to open Tables.tbl");
        return -1;
    }

    //Insert the record
    //Populate record with (table-id, table-name, file-name) in the correct format
    id = nextTableId++;
    recordSize = prepareTablesRecord(record, id, tableName, filename);
    //insert it into Tables
    rbfm->insertRecord(fileHandle, getTablesRecordDescriptor(), record, rid);
    free(record);

    //close the fileHandle
    if (rbfm->closeFile(fileHandle)) {
        perror("RelationManager: createTable() failed to close Tables.tbl");
        return -1;
    }

    if (rbfm->openFile("Columns.tbl", fileHandle) != SUCCESS) {
        perror("RelationManager: createTable() failed to close Tables.tbl");
        return -1;
    }
    insertColumns(fileHandle, id, attrs);
    //close the fileHandle
    if (rbfm->closeFile(fileHandle)) {
        perror("RelationManager: createTable() failed to close Tables.tbl");
        return -1;
    }


    return 0;
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
