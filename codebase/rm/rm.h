
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <iostream>
#include <string.h>

#include "../rbf/rbfm.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

#define MAX_TABLE_NAME_LENGTH 50;
#define TABLES_NAME "Tables"
#define COLUMNS_NAME "Columns"

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data) { return RM_EOF; };
  RC close() { return -1; };

  FileHandle fh;
  RBFM_ScanIterator rbfm_scan;
};


// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

  //Helper functions


protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;
    int nextTableId = 0;
    string toFilename(const string &tableName);
    vector<Attribute> getTablesRecordDescriptor();
    vector<Attribute> getColumnsRecordDescriptor();
    int prepareTablesRecord(void *buffer, const int tableId, const string &tableName, const string &fileName);
    int prepareColumnsRecord(void *buffer, const int tableId, const string &columnName, const int columnType, const int columnLength, const int columnPosition);
    int insertColumns(FileHandle &fileHandle, const int id, const vector<Attribute> &attrs);
    int getTableId(const string &tableName);
    bool isAdmin(const string &tableName);
};

#endif
