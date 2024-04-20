#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		const string & attrName, 
		const Operator op,
		const Datatype type, 
		const char *attrValue)
{
	// part 6

	Status status;

	//	printf("Datatype: %i, integer: %i, float: %i, STRING: %i\n",
	//			type, INTEGER, FLOAT, STRING);//TODO
	//create a new object of HeapFileScan
	HeapFileScan heapfileScan(relation, status);
	if (status != OK) return status;

	//if argument is null, no need to delete anything
	//because STRING is 0, so can't check type == NULL cause it
	//will skip the case STRING
	if (*attrName.c_str() == NULL) {
		//delete the whole relation
		status = heapfileScan.startScan(0,0,STRING,NULL,EQ);
		if (status != OK) return status;

		RID rid;
		while(heapfileScan.scanNext(rid) == OK) {
			Record rec;
			status = heapfileScan.getRecord(rec);
			if (status != OK) return status;

			heapfileScan.deleteRecord();
		}
		return OK;
	}

	//create AttrDesc to obtain info about offset, etc.
	AttrDesc attrDesc1;
	status = attrCat->getInfo(relation, attrName, attrDesc1);
	if (status != OK) return status;

	//convert the attrValue to appropriate datatype
	if (type == INTEGER) {
		int* addr_filter = (int*)malloc(sizeof(int));
		int filter_val = atoi(attrValue);
		memcpy((void*)addr_filter, &filter_val, sizeof(int));
		status = heapfileScan.startScan(attrDesc1.attrOffset,
				attrDesc1.attrLen, type, (char*)addr_filter, op);
	}
	if (type == FLOAT) {
		float filter_val = atof(attrValue);
		//float* addr_filter = &filter_val;
		float* addr_filter = (float*)malloc(sizeof(float));
		memcpy((void*)addr_filter, &filter_val,
				sizeof(float));

		status = heapfileScan.startScan(attrDesc1.attrOffset,
				attrDesc1.attrLen, type, (char*)addr_filter, op);
	}
	if (type == STRING) {
		status = heapfileScan.startScan(attrDesc1.attrOffset,
				attrDesc1.attrLen, type, attrValue, op);
	}

	if (status != OK) return status;

	RID rid;
	while (heapfileScan.scanNext(rid) == OK) {
		Record rec;
		//get the record of the corresponding rid
		status = heapfileScan.getRecord(rec);
		if (status != OK) return status;

		//delete current record
		heapfileScan.deleteRecord();

	}

	//delete heapfileScan;
	return OK;
}


