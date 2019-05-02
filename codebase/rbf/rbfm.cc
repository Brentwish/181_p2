#include "rbfm.h"
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <string.h>
#include <iomanip>

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = NULL;
PagedFileManager *RecordBasedFileManager::_pf_manager = NULL;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    // Creating a new paged file.
    if (_pf_manager->createFile(fileName))
        return RBFM_CREATE_FAILED;

    // Setting up the first page.
    void * firstPageData = calloc(PAGE_SIZE, 1);
    if (firstPageData == NULL)
        return RBFM_MALLOC_FAILED;
    newRecordBasedPage(firstPageData);
    // Adds the first record based page.

    FileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), handle))
        return RBFM_OPEN_FAILED;
    if (handle.appendPage(firstPageData))
        return RBFM_APPEND_FAILED;
    _pf_manager->closeFile(handle);

    free(firstPageData);

    return SUCCESS;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return _pf_manager->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pf_manager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    // Gets the size of the record.
    unsigned recordSize = getRecordSize(recordDescriptor, data);

    // Cycles through pages looking for enough free space for the new entry.
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;
    bool pageFound = false;
    unsigned i;
    unsigned numPages = fileHandle.getNumberOfPages();
    for (i = 0; i < numPages; i++)
    {
        if (fileHandle.readPage(i, pageData))
            return RBFM_READ_FAILED;

        // When we find a page with enough space (accounting also for the size that will be added to the slot directory), we stop the loop.
        if (getPageFreeSpaceSize(pageData) >= sizeof(SlotDirectoryRecordEntry) + recordSize)
        {
            pageFound = true;
            break;
        }
    }

    // If we can't find a page with enough space, we create a new one
    if(!pageFound)
    {
        newRecordBasedPage(pageData);
    }

    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    // SINCE WE MOVED THE RECORDS NEED TO ENSURE THAT THERE'S NO EMPTY SLOTS
    // FIND THE EMPTY SLOT
    SlotDirectoryRecordEntry sEntry;
    rid.slotNum = -1;
    for (int j=0; j < slotHeader.recordEntriesNumber; j++) {
        sEntry = getSlotDirectoryRecordEntry(pageData, j);
        if (sEntry.length == 4097) { // deleted record
            rid.slotNum = j;
        }
    }
    if (rid.slotNum == -1) // no slot found in the middle
        rid.slotNum = slotHeader.recordEntriesNumber;
    rid.pageNum = i;

    // Adding the new record reference in the slot directory.
    SlotDirectoryRecordEntry newRecordEntry;
    newRecordEntry.length = recordSize;
    newRecordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);

    // Updating the slot directory header.
    slotHeader.freeSpaceOffset = newRecordEntry.offset;
    slotHeader.recordEntriesNumber += 1;
    setSlotDirectoryHeader(pageData, slotHeader);

    // Adding the record data.
    setRecordAtOffset (pageData, newRecordEntry.offset, recordDescriptor, data);

    // Writing the page to disk.
    if (pageFound)
    {
        if (fileHandle.writePage(i, pageData))
            return RBFM_WRITE_FAILED;
    }
    else
    {
        if (fileHandle.appendPage(pageData))
            return RBFM_APPEND_FAILED;
    }

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    // Retrieve the specified page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

    // check if the slot is in this page
    if (recordEntry.length == 4097 ) {
    	free(pageData);
    	return 1;
    }
    if (recordEntry.length < 0) { // moved record
    	free(pageData);
    	RID temp;
        temp.slotNum = recordEntry.length * -1;
        temp.pageNum = recordEntry.offset;
        return readRecord(fileHandle, recordDescriptor, temp, data);
    }

    // Retrieve the actual entry data
    getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    // Parse the null indicator and save it into an array
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, data, nullIndicatorSize);
    
    // We've read in the null indicator, so we can skip past it now
    unsigned offset = nullIndicatorSize;

    cout << "----" << endl;
    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        cout << setw(10) << left << recordDescriptor[i].name << ": ";
        // If the field is null, don't print it
        bool isNull = fieldIsNull(nullIndicator, i);
        if (isNull)
        {
            cout << "NULL" << endl;
            continue;
        }
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                uint32_t data_integer;
                memcpy(&data_integer, ((char*) data + offset), INT_SIZE);
                offset += INT_SIZE;

                cout << "" << data_integer << endl;
            break;
            case TypeReal:
                float data_real;
                memcpy(&data_real, ((char*) data + offset), REAL_SIZE);
                offset += REAL_SIZE;

                cout << "" << data_real << endl;
            break;
            case TypeVarChar:
                // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
                uint32_t varcharSize;
                memcpy(&varcharSize, ((char*) data + offset), VARCHAR_LENGTH_SIZE);
                offset += VARCHAR_LENGTH_SIZE;

                // Gets the actual string.
                char *data_string = (char*) malloc(varcharSize + 1);
                if (data_string == NULL)
                    return RBFM_MALLOC_FAILED;
                memcpy(data_string, ((char*) data + offset), varcharSize);

                // Adds the string terminator.
                data_string[varcharSize] = '\0';
                offset += varcharSize;

                cout << data_string << endl;
                free(data_string);
            break;
        }
    }
    cout << "----" << endl;

    return SUCCESS;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid) 
{
    // get the page
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);

    // get the slot
    SlotDirectoryRecordEntry sEntry = getSlotDirectoryRecordEntry(page, rid.slotNum);

    // make sure that the slot is in this page
    if (sEntry.length < 0) { // if the record has been moved 
        RID temp;
        temp.slotNum = sEntry.length * -1;
        temp.pageNum = sEntry.offset;
        // do the delete on the correct page
        deleteRecord(fileHandle, recordDescriptor, temp);
        // remove the slots too
        sEntry.length = 4097;
        setSlotDirectoryRecordEntry(page, rid.slotNum, sEntry); // remove the slot's validity
        free(page);
        return SUCCESS;
    }
    else { // on this page, so just change header and compact
        sEntry.length = 4097;
        setSlotDirectoryRecordEntry(page, rid.slotNum, sEntry);
        compactPage(page);
    }

    // commit the changes to the disk
    fileHandle.writePage(rid.pageNum, page);
    free(page);
    return SUCCESS;

}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid) 
{
    // 1) check if the record is there
    // first get the page 
    void* page = malloc (PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);

    // get the slotDirectoryHeader
    SlotDirectoryHeader sHeader = getSlotDirectoryHeader(page);

    // check if the rid is in range
    // rid starts at 0
    if (sHeader.recordEntriesNumber - 1 < rid.slotNum) {
        free(page);
        return RBFM_SLOT_DN_EXIST;
    }

    // 2) get the previous space the record takes up
    // get the slot entry 
    // need to make sure it's correct one 
    // TODO!!!!
    SlotDirectoryRecordEntry sEntry = getSlotDirectoryRecordEntry(page, rid.slotNum);

    if (sEntry.length < 0 ) { // record moved 
        RID temp; // create a new RID
        temp.slotNum = sEntry.length * -1;
        temp.pageNum = sEntry.offset;
        RC ret = updateRecord(fileHandle, recordDescriptor, data, temp);
        free(page);
        return ret;
    }
    // get the space it takes up originally and the new one
    uint32_t prevLen = sEntry.length;
    unsigned newLen = getRecordSize(recordDescriptor, data); 

    // the sizes of the entries matches
    // just replace the old one 
    if (prevLen == newLen) {
        setRecordAtOffset(page, sEntry.offset, recordDescriptor, data);
    }
    // 3) check if more or less space 
    // since the new one is more space 
    else if (prevLen < newLen) {
        // check if there is space on the page 
        unsigned space = getPageFreeSpaceSize(page);
        // there's not enough space in the page
        // calculate as if the old record being deleted but slot still there
        if (space + sEntry.length < newLen) {
            // do the forwarding 
            RID temp;
            // get a new RID for the record
            insertRecord(fileHandle, recordDescriptor, data, temp);

            // put forwarding data in this slot 
            sEntry.length = temp.slotNum * -1;
            sEntry.offset = temp.pageNum;

            // set the slot 
            setSlotDirectoryRecordEntry(page, rid.slotNum, sEntry);
            // need to make room on the page since the record was moved
            compactPage(page);
        }
        else if (space + sEntry.length > newLen) { // enough space in page
            /// just place the new record where the freeSpace is 
            sEntry.length = 4097;
            setSlotDirectoryRecordEntry(page, rid.slotNum, sEntry); // make the entry null first to compact
            // compact the page as if the entry not there anymore
            compactPage(page);
            sHeader = getSlotDirectoryHeader(page); // need new header for the new freeSpaceOffset
            sEntry.length = newLen;

            // insert at the end of the freeSpace 
            setRecordAtOffset(page, sHeader.freeSpaceOffset - newLen, recordDescriptor, data);
            sEntry.offset = sHeader.freeSpaceOffset - newLen;
            // update the directory before compacting 
            // NEED TO DO THIS BECAUSE COMPACT PAGE USES SLOTS!!
            setSlotDirectoryRecordEntry(page, rid.slotNum, sEntry);
        }
    }
    // less space taken up
    // can fit on this page
    else if (prevLen > newLen) {
        // just place the new record where the freeSpace is 
        sEntry.length = 4097;
        setSlotDirectoryRecordEntry(page, rid.slotNum, sEntry); // make the entry null first to compact
        // compact the page as if the entry not there anymore
        compactPage(page);
        sHeader = getSlotDirectoryHeader(page);
        sEntry.length = newLen;

        // insert at the end of the freeSpace 
        setRecordAtOffset(page, sHeader.freeSpaceOffset - newLen, recordDescriptor, data);
        sEntry.offset = sHeader.freeSpaceOffset - newLen;
        // update the directory before compacting 
        // NEED TO DO THIS BECAUSE COMPACT PAGE USES SLOTS!!
        setSlotDirectoryRecordEntry(page, rid.slotNum, sEntry);  
    }

    // 4) free everything and write the page 
    // get rid of the old stuff and write 
    fileHandle.writePage(rid.pageNum, page);
    free(page);
    return SUCCESS;

}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data) 
{
    // get the page first 
    void *page = malloc(PAGE_SIZE);
    fileHandle.readPage(rid.pageNum, page);

    // get the slot header
    SlotDirectoryHeader sHeader;
    sHeader = getSlotDirectoryHeader(page);

    // get the actual slot
    SlotDirectoryRecordEntry sEntry;
    sEntry = getSlotDirectoryRecordEntry(page, rid.slotNum);

    // check if the record is still on this page 
    if (sEntry.length < 0) {
        RID temp;
        temp.pageNum = sEntry.offset;
        temp.slotNum = sEntry.length * -1;
        // do the operation on the correct page
        RC ret = readAttribute(fileHandle, recordDescriptor, temp, attributeName, data);

        if (ret == 0 ) {
            free(page);
            return SUCCESS;
        }
    }

    // find the index of the attribute we are looking for 
    int i=0;
    while(recordDescriptor[i].name != attributeName){
        i++;
    }

    insertAttrIntoData(page, sEntry, i, data);
    free(page);
    return SUCCESS;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator) 
{
    rbfm_ScanIterator.getIterator(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
    return SUCCESS;
}

// RBFM_ScanIterator::RBFM_ScanIterator()
// {
// }

// RBFM_ScanIterator::~RBFM_ScanIterator()
// {
// }
RC RBFM_ScanIterator::getIterator(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const string &conditionAttribute, const CompOp compOp, const void *value, const vector<string> &attributeNames)
{
	this->rbfm = RecordBasedFileManager::instance();

    this->fileHandle = fileHandle;
    this->rd = recordDescriptor;
    this->condAttr = conditionAttribute;
    this->compOp = compOp;
    this->val = value;
    this->attrNames = attributeNames;

    currRid.slotNum = 0;
    currRid.pageNum = 0;

    void *page = malloc(PAGE_SIZE);

    //read the first page into data 
    uint32_t totPages = this->fileHandle.getNumberOfPages();

    if (totPages > 0 ) // if the file has at least 1 page
    	this->fileHandle.readPage(0,page);

    // store the page header
    sHeader = rbfm->getSlotDirectoryHeader(page);

    free(page);
    return SUCCESS;
}

// uses current RID to get next record
RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
    // what I need 

    // find out which record we are at
    // check if it's last record in the page
	if (currRid.slotNum >= sHeader.recordEntriesNumber) {
		currRid.slotNum = 0;
		currRid.pageNum = currRid.pageNum + 1;
		// if the last page then return eof 
		if (currRid.pageNum >= fileHandle.getNumberOfPages()) {
			return RBFM_EOF;
		}

		// get the slot header for the next page then
		void *page1 = malloc(PAGE_SIZE);
		fileHandle.readPage(currRid.pageNum, page1);
		sHeader = rbfm->getSlotDirectoryHeader(page1);
		free(page1);
	}

	void *page = malloc(PAGE_SIZE);
    if (page == NULL){
        return RBFM_MALLOC_FAILED;
    }
	if (fileHandle.readPage(currRid.pageNum, page) ){ // if error
        free(page);
        return 2;
    }
	if (currRid.slotNum >= sHeader.recordEntriesNumber) {
        free(page);
		return RBFM_EOF;
    }
 	// get the slot
	SlotDirectoryRecordEntry sEntry = rbfm->getSlotDirectoryRecordEntry(page, currRid.slotNum);
    
    // check if slot is moved or dead
    if (sEntry.length == 4097 || sEntry.length < 0) {
    	// advance the slot
    	currRid.slotNum = currRid.slotNum + 1;
        free(page);
    	return getNextRecord(rid, data);
    }
    // check if record satisfies compOp
    bool valid;
    // Figure which attribute to get from the record to compare
    int idx = 0;
    while(rd[idx].name != condAttr) {
    	idx++;
    }
    // get the exact attribute
    rbfm->insertAttrIntoData(page, sEntry, idx, data); // has null indicator as 1st byte
    //check if field is null
    char nullBytes;
    memcpy(&nullBytes, data, 1);
    if (nullBytes) { // is null 
    	valid = false;
    }
    // copy into appropriate var and compare
    else if (rd[idx].type == TypeInt) {
    	// current slot's value 
    	int val1;
    	memcpy(&val1,(char*) data + 1,INT_SIZE);
    	// get the value that we are comparing to
    	int val2;
    	memcpy(&val2, val, INT_SIZE);

    	// compare attr
    	valid = compareInts(val1, val2);
    }
    else if (rd[idx].type == TypeReal) {
    	// current slot's value 
    	float val1;
    	memcpy(&val1, (char*) data + 1, REAL_SIZE);
    	// get the value that we are comparing to
    	float val2;
    	memcpy(&val2, val, REAL_SIZE);

    	// compare attr
    	valid = compareReals(val1, val2);
    }
    else if (rd[idx].type == TypeVarChar) {
    	int varLen; 
    	memcpy(&varLen, (char*) data + 1, VARCHAR_LENGTH_SIZE);
    	char varString[varLen + 1]; // for null terminate
    	memcpy(&varString, (char*) data + 1 + VARCHAR_LENGTH_SIZE, varLen);
    	varString[varLen] = '\0';

    	int varLen2;
    	memcpy(&varLen2, val, VARCHAR_LENGTH_SIZE);
    	char varString2[varLen2 + 1];
    	memcpy(&varString2, (char*) val + VARCHAR_LENGTH_SIZE, varLen2);
    	varString2[varLen2] = '\0';
    	// compare attr

    	valid = compareVarChars(varString, varString2);
    }
    // the current record didn't satisfy the condition
    // do the same earlier
    if (!valid) {
    	free(page);
        currRid.slotNum = currRid.slotNum + 1;
    	return getNextRecord(rid, data);
    }

    // fill the record into data but only the attributes they want 

    // first the null indicators
    unsigned size = rbfm->getNullIndicatorSize(attrNames.size());
    char nullIndicator[size];
    memset(nullIndicator,0, size );
    int dataOffset = 0; // used to insert into data 

    // need to check if the field is null or not 
    // do it as we go
    dataOffset += size;

    // create a list of the indexes we will access 
    vector<int> indexList; 
    for (int i = 0; i< attrNames.size(); i++) {
    	int j = 0;
    	Attribute attr = rd[j];
    	while(attr.name != attrNames[i]) { // get the index for this name
    		j++;
    		attr = rd[j];
    	}
    	indexList.push_back(j);
    }

    void *attrPage = malloc(4096);
    // to set the null indicator before the attributes
    // for (int i=0; i < indexList.size(); i++) {
    // 	if (fieldIsNull())
    // }
	
	// get each record and see if it's null 
    for (int i=0; i < indexList.size(); i++) {
    	idx = indexList[i];
    	Attribute attr = rd[idx];
    	rbfm->insertAttrIntoData(page, sEntry, idx, attrPage);

    	// check if this attr is null 
    	char nullField;
    	memcpy(&nullField, attrPage, 1);
    	// similar to field is null 
    	if (nullField) { // fill in the null indicator
    		int bitIdx = i / CHAR_BIT;
    		char bitMask = 1 << (CHAR_BIT - 1 - (1 % CHAR_BIT) );
    		nullIndicator[bitIdx] |= bitMask;
    	}
    	else if (attr.type == TypeVarChar) {
    		int varLen; 
    		memcpy(&varLen, (char*) attrPage + 1, VARCHAR_LENGTH_SIZE);
    		memcpy((char*) data + dataOffset, &varLen, VARCHAR_LENGTH_SIZE);
    		dataOffset += VARCHAR_LENGTH_SIZE;
    		char varString[varLen + 1];
    		memcpy(&varString, (char*) attrPage + 1 + VARCHAR_LENGTH_SIZE, varLen);
    		memcpy((char*) data + dataOffset, &varString, varLen);
    		dataOffset += varLen;
    	}
    	else { // must be int or real
    		memcpy((char*) data + dataOffset,(char*) attrPage + 1, INT_SIZE);
    		dataOffset += INT_SIZE;
    	}
    }

	// write the new null indicator 
	memcpy((char*) data, nullIndicator, size);

	// change the rid to be updated
	rid.pageNum = currRid.pageNum;
	currRid.slotNum = currRid.slotNum + 1; // increase to the next slot
	rid.slotNum = currRid.slotNum;
    free(attrPage);
    free(page);
    return SUCCESS;
}

bool RBFM_ScanIterator::compareInts(int val1, int val2) {
	if (compOp == EQ_OP) {
		return (val1 == val2);
	}
	else if (compOp == LT_OP) {
		return (val1 < val2);
	}
	else if (compOp == LE_OP) {
		return (val1 <= val2);
	}
	else if (compOp == GT_OP) {
		return (val1 > val2);
	}
	else if (compOp == GE_OP) {
		return (val1 >= val2);
	}
	else if (compOp == NE_OP) {
		return (val1 != val2);
	}
	else if (compOp == NO_OP) {
		return true;
	}
	return false;
}

bool RBFM_ScanIterator::compareReals(float val1, float val2) {
	if (compOp == EQ_OP) {
		return (val1 == val2);
	}
	else if (compOp == LT_OP) {
		return (val1 < val2);
	}
	else if (compOp == LE_OP) {
		return (val1 <= val2);
	}
	else if (compOp == GT_OP) {
		return (val1 > val2);
	}
	else if (compOp == GE_OP) {
		return (val1 >= val2);
	}
	else if (compOp == NE_OP) {
		return (val1 != val2);
	}
	else if (compOp == NO_OP) {
		return true;
	}
	return false;
}

bool RBFM_ScanIterator::compareVarChars(char* str1, char* str2) {
	int diff = strcmp(str1, str2); // get the difference in the strings 

	if (compOp == EQ_OP) {
		return (diff == 0);
	}
	else if (compOp == LT_OP) {
		return (diff < 0);
	}
	else if (compOp == LE_OP) {
		return (diff <= 0);
	}
	else if (compOp == GT_OP) {
		return (diff > 0);
	}
	else if (compOp == GE_OP) {
		return (diff >= 0);
	}
	else if (compOp == NE_OP) {
		return (diff != 0);
	}
	else if (compOp == NO_OP) {
		return true;
	}
	return false;
}

RC RBFM_ScanIterator::close() 
{
	return SUCCESS;
}

void RecordBasedFileManager::insertAttrIntoData(void* page, SlotDirectoryRecordEntry sEntry, int attrIdx, void* data) 
{
    // get to the start of the offset in the page
    void* recOffset = (char*) page + sEntry.offset;
    int dataOffset = 0; // Offset for the data page

    // insert the null pointer into the data first 
    // calculate null header
    // same as GetRecordAtOffset for most part
    // Allocate space for null indicator.
    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy (&len, recOffset, sizeof(RecordLength));

    int recordNullIndicatorSize = getNullIndicatorSize(len);
    char recordNullIndicator[recordNullIndicatorSize];

    // Read in the existing null indicator
    memcpy (recordNullIndicator, (char*) recOffset + sizeof(RecordLength), recordNullIndicatorSize);

    // check if the field is null
    if (fieldIsNull(recordNullIndicator, attrIdx))
        recordNullIndicator[0] |= (1 << 7); // make the first bit in the null indicator null 
    memcpy(data, &recordNullIndicator, 1); // place in the data
    dataOffset += 1;

    // move to the correct column for the offset
    // |Record Length | Null Bytes | Col 1 | Col 2| ... | Data1 | Data2 | 
    int colOffset = sizeof(RecordLength) + attrIdx * sizeof(ColumnOffset) + recordNullIndicatorSize;
    ColumnOffset recDataOffset;
    memcpy(&recDataOffset, (char*) recOffset + colOffset, sizeof(ColumnOffset));

    // have the end of the attribute 
    // need to check type of the attr 
    // get to the prev element to see size of the record
    ColumnOffset prevRecDataOffset;
    if (attrIdx == 0) { // start is the end of column offsets
        prevRecDataOffset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset) ;
    }
    else { // get the prev record's end 
        memcpy(&prevRecDataOffset, (char*) recOffset + colOffset - sizeof(ColumnOffset), sizeof(ColumnOffset));    
    }
    
    // use length to check type 
    int attrSize = recDataOffset - prevRecDataOffset;

    if (attrSize == INT_SIZE) { // is the size of an int or real 
        memcpy((char*) data + dataOffset, (char*) recOffset + prevRecDataOffset, attrSize);
    }
    else { // must be var char
        // have to copy the size of the var char first
        memcpy((char*) data + dataOffset, &attrSize, sizeof(VARCHAR_LENGTH_SIZE));
        dataOffset += VARCHAR_LENGTH_SIZE;
        // place the actual var char in the data 
        memcpy((char*) data + dataOffset, (char*) recOffset + prevRecDataOffset, attrSize);
    }
    // done data is formatted properly
}

// bring the free space towards the center of the page between the records and the slots
void RecordBasedFileManager::compactPage(void *page)
{
    // get the slot header 
    SlotDirectoryHeader sHeader = getSlotDirectoryHeader(page);

    // need to first get the slots for the whole page
    uint16_t numSlots = sHeader.recordEntriesNumber;

    // get the list of the records that are still on this page
    vector<pair <SlotDirectoryRecordEntry, int> > realSlots;

    for (int i=0; i < numSlots; i++) {
        SlotDirectoryRecordEntry sEntry = getSlotDirectoryRecordEntry(page, i);
        pair <SlotDirectoryRecordEntry, int> slotPair;
        slotPair.first = sEntry;
        slotPair.second = i;
        uint32_t len = sEntry.length;
        // check if the slot is still there
        // length of larger than page means that the slot DNE
        if (len != 4097) {
            // check if the slot is moved 
            if (sEntry.length > 0) { // not moved
                realSlots.push_back(make_pair(sEntry, i));
            }
        }
    }
    // https://stackoverflow.com/questions/4892680/sorting-a-vector-of-structs
    // change the index of the slot and sort it at the same time, but slot must stay the same
    // need to sort so we don't overwrite anything we haven't moved
    std::sort(realSlots.begin(), realSlots.end(), [](const std::pair<SlotDirectoryRecordEntry,int> &p2, const std::pair<SlotDirectoryRecordEntry,int> &p1) {
        return p1.first.offset > p2.first.offset;
    });

    // need to do the actual moving of the record now 
    // start filling from the top of the page
    int pageOffset = PAGE_SIZE;

    // iterate through our vector
    for (int i=0; i < realSlots.size(); i++) {
        pair <SlotDirectoryRecordEntry, int> vecPair;
        vecPair = realSlots[i];
        SlotDirectoryRecordEntry currSlot = vecPair.first;
        int recLen = currSlot.length;
        // create a new var the same size of the record
        void* temp = malloc(recLen);
        memcpy(temp, (char*) page + currSlot.offset, recLen);
        // put it in the proper place
        memcpy((char*) page + pageOffset - recLen, temp, recLen);
        pageOffset = pageOffset - recLen;
        currSlot.offset = pageOffset;
        // update the entry
        setSlotDirectoryRecordEntry(page, vecPair.second, currSlot);
        free(temp);
    }

    // update the header of the slots 
    sHeader.freeSpaceOffset = pageOffset;
    setSlotDirectoryHeader(page, sHeader);
}

SlotDirectoryHeader RecordBasedFileManager::getSlotDirectoryHeader(void * page)
{
    // Getting the slot directory header.
    SlotDirectoryHeader slotHeader;
    memcpy (&slotHeader, page, sizeof(SlotDirectoryHeader));
    return slotHeader;
}

void RecordBasedFileManager::setSlotDirectoryHeader(void * page, SlotDirectoryHeader slotHeader)
{
    // Setting the slot directory header.
    memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

SlotDirectoryRecordEntry RecordBasedFileManager::getSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber)
{
    // Getting the slot directory entry data.
    SlotDirectoryRecordEntry recordEntry;
    memcpy  (
            &recordEntry,
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            sizeof(SlotDirectoryRecordEntry)
            );

    return recordEntry;
}

void RecordBasedFileManager::setSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber, SlotDirectoryRecordEntry recordEntry)
{
    // Setting the slot directory entry data.
    memcpy  (
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            &recordEntry,
            sizeof(SlotDirectoryRecordEntry)
            );
}

// Configures a new record based page, and puts it in "page".
void RecordBasedFileManager::newRecordBasedPage(void * page)
{
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    SlotDirectoryHeader slotHeader;
    slotHeader.freeSpaceOffset = PAGE_SIZE;
    slotHeader.recordEntriesNumber = 0;
    memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data) 
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Offset into *data. Start just after the null indicator
    unsigned offset = nullIndicatorSize;
    // Running count of size. Initialize it to the size of the header
    unsigned size = sizeof (RecordLength) + (recordDescriptor.size()) * sizeof(ColumnOffset) + nullIndicatorSize;

    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        // Skip null fields
        if (fieldIsNull(nullIndicator, i))
            continue;
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                size += INT_SIZE;
                offset += INT_SIZE;
            break;
            case TypeReal:
                size += REAL_SIZE;
                offset += REAL_SIZE;
            break;
            case TypeVarChar:
                uint32_t varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
                size += varcharSize;
                offset += varcharSize + VARCHAR_LENGTH_SIZE;
            break;
        }
    }

    return size;
}

// Calculate actual bytes for null-indicator for the given field counts
int RecordBasedFileManager::getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

bool RecordBasedFileManager::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

// Computes the free space of a page (function of the free space pointer and the slot directory size).
unsigned RecordBasedFileManager::getPageFreeSpaceSize(void * page) 
{
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
    return slotHeader.freeSpaceOffset - slotHeader.recordEntriesNumber * sizeof(SlotDirectoryRecordEntry) - sizeof(SlotDirectoryHeader);
}

// Support header size and null indicator. If size is less than recordDescriptor size, then trailing records are null
void RecordBasedFileManager::getRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, void *data)
{
    // Pointer to start of record
    char *start = (char*) page + offset;

    // Allocate space for null indicator.
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy (&len, start, sizeof(RecordLength));
    int recordNullIndicatorSize = getNullIndicatorSize(len);

    // Read in the existing null indicator
    memcpy (nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

    // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
    for (unsigned i = len; i < recordDescriptor.size(); i++)
    {
        int indicatorIndex = (i+1) / CHAR_BIT;
        int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        nullIndicator[indicatorIndex] |= indicatorMask;
    }
    // Write out null indicator
    memcpy(data, nullIndicator, nullIndicatorSize);

    // Initialize some offsets
    // rec_offset: points to data in the record. We move this forward as we read data from our record
    unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
    // data_offset: points to our current place in the output data. We move this forward as we write to data.
    unsigned data_offset = nullIndicatorSize;
    // directory_base: points to the start of our directory of indices
    char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;
    
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        if (fieldIsNull(nullIndicator, i))
            continue;
        
        // Grab pointer to end of this column
        ColumnOffset endPointer;
        memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

        // rec_offset keeps track of start of column, so end-start = total size
        uint32_t fieldSize = endPointer - rec_offset;

        // Special case for varchar, we must give data the size of varchar first
        if (recordDescriptor[i].type == TypeVarChar)
        {
            memcpy((char*) data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
            data_offset += VARCHAR_LENGTH_SIZE;
        }
        // Next we copy bytes equal to the size of the field and increase our offsets
        memcpy((char*) data + data_offset, start + rec_offset, fieldSize);
        rec_offset += fieldSize;
        data_offset += fieldSize;
    }
}

void RecordBasedFileManager::setRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset (nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Points to start of record
    char *start = (char*) page + offset;

    // Offset into *data
    unsigned data_offset = nullIndicatorSize;
    // Offset into page header
    unsigned header_offset = 0;

    RecordLength len = recordDescriptor.size();
    memcpy(start + header_offset, &len, sizeof(len));
    header_offset += sizeof(len);

    memcpy(start + header_offset, nullIndicator, nullIndicatorSize);
    header_offset += nullIndicatorSize;

    // Keeps track of the offset of each record
    // Offset is relative to the start of the record and points to the END of a field
    ColumnOffset rec_offset = header_offset + (recordDescriptor.size()) * sizeof(ColumnOffset);

    unsigned i = 0;
    for (i = 0; i < recordDescriptor.size(); i++)
    {
        if (!fieldIsNull(nullIndicator, i))
        {
            // Points to current position in *data
            char *data_start = (char*) data + data_offset;

            // Read in the data for the next column, point rec_offset to end of newly inserted data
            switch (recordDescriptor[i].type)
            {
                case TypeInt:
                    memcpy (start + rec_offset, data_start, INT_SIZE);
                    rec_offset += INT_SIZE;
                    data_offset += INT_SIZE;
                break;
                case TypeReal:
                    memcpy (start + rec_offset, data_start, REAL_SIZE);
                    rec_offset += REAL_SIZE;
                    data_offset += REAL_SIZE;
                break;
                case TypeVarChar:
                    unsigned varcharSize;
                    // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                    memcpy(&varcharSize, data_start, VARCHAR_LENGTH_SIZE);
                    memcpy(start + rec_offset, data_start + VARCHAR_LENGTH_SIZE, varcharSize);
                    // We also have to account for the overhead given by that integer.
                    rec_offset += varcharSize;
                    data_offset += VARCHAR_LENGTH_SIZE + varcharSize;
                break;
            }
        }
        // Copy offset into record header
        // Offset is relative to the start of the record and points to END of field
        memcpy(start + header_offset, &rec_offset, sizeof(ColumnOffset));
        header_offset += sizeof(ColumnOffset);
    }
}

// return if the file exists
bool RecordBasedFileManager::checkFile(const string &fileName) {
	return ( access( fileName.c_str(), F_OK ) != -1 );
}
