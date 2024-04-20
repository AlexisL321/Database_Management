#include "catalog.h"
#include "query.h"
#include "stdio.h"
#include "stdlib.h"


// forward declaration
const Status ScanSelect(const string & result, 
		const int projCnt, 
		const AttrDesc projNames[],
		const AttrDesc *attrDesc, 
		const Operator op, 
		const char *filter,
		const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		const int projCnt, 
		const attrInfo projNames[],
		const attrInfo *attr, 
		const Operator op, 
		const char *attrValue)
{
	// Qu_Select sets up things and then calls ScanSelect to do the actual work
	cout << "Doing QU_Select " << endl;

	Status status;

	//project using the projName and projCnt given
	AttrDesc attrDescArray[projCnt];
	for (int i = 0; i < projCnt; i++) {
		Status status =
			attrCat->getInfo(projNames[i].relName,projNames[i].attrName,
					attrDescArray[i]);
		if (status!=OK) return status;
	}

	//TODO:not sure about the use of reclen, but will mimick join.C
	int reclen = 0;
	for (int i = 0; i < projCnt; i++) {
		reclen += attrDescArray[i].attrLen;
	}

	//if attr is null, do unconditional scan
	if (attr == NULL) {
		status = ScanSelect(result, projCnt, attrDescArray, NULL, op,
				attrValue, reclen);
		return status;
	}

	//if attr is not null
	//change attr from attrInfo to attrDesc
	AttrDesc attrDesc1;
	status = attrCat->getInfo(attr->relName, attr->attrName,
			attrDesc1);
	if (status != OK) return status;


	status = ScanSelect(result, projCnt, attrDescArray,
			&attrDesc1, op, attrValue, reclen);

	return status;
}

//This function is a helper function to QU_select in that it is
//doing the actual scanning 
//result is the table name to store the final returned result
//projCnt and projNames[] are for project list for projection
//attrDesc stores info about the attribute to be filtered upon
//op, filter are the actual filtering criteria
//reclen is the length of the final returned select result
const Status ScanSelect(const string & result, 
		const int projCnt, 
		const AttrDesc projNames[],
		const AttrDesc *attrDesc, 
		const Operator op, 
		const char *filter,
		const int reclen)
{
	cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

	Status status;
	int resultTupCnt = 0;


	//create a new object of HeapFileScan
	HeapFileScan heapfileScan(projNames->relName, status);
	if (status != OK) return status;

	//create a new object of InsertFileScan 
	InsertFileScan insertfileScan(result, status);
	if (status != OK) return status;

	char outputData[reclen];
	memset((void*) outputData, 0, reclen);//initialize it to 0

	//initialize outputRec
	Record outputRec;
	outputRec.data = (void*)outputData;
	outputRec.length = reclen;

	//do unconditional scan
	if (attrDesc == NULL) {
		status = heapfileScan.startScan(0,0,STRING,NULL,op);
		if (status != OK) return status;
	}

	//if attrDesc is not null then need filter
	else{
		//convert the type of attrValue
		if (attrDesc->attrType == INTEGER) {
			/*
			   int filter_val = atoi(filter);
			   int* addr_filter = &filter_val;
			 */
			int* addr_filter = (int*)malloc(sizeof(int));
			int filter_val = atoi(filter);
			memcpy((void*)addr_filter, &filter_val, sizeof(int));
			status = heapfileScan.startScan(attrDesc->attrOffset,
					attrDesc->attrLen, (Datatype)attrDesc->attrType,
					(char*)addr_filter, op);
			//printf("%d\n",*(char*)addr_filter);//TODO
		}
		else if (attrDesc->attrType == FLOAT) {
			float filter_val = atof(filter);
			//float* addr_filter = &filter_val;
			float* addr_filter = (float*)malloc(sizeof(float));
			memcpy((void*)addr_filter, &filter_val,
					sizeof(float));
			status = heapfileScan.startScan(attrDesc->attrOffset,
					attrDesc->attrLen, (Datatype)attrDesc->attrType,
					(char*)addr_filter, op);
			//printf("%f\n",*addr_filter);//TODO
		}
		else {
			status = heapfileScan.startScan(attrDesc->attrOffset,
					attrDesc->attrLen, (Datatype)attrDesc->attrType,
					filter, op);
		}
		if (status != OK) return status;
	}

	RID rid;
	//int count = 0;//TODO
	while(heapfileScan.scanNext(rid) == OK) {
		//count++;//TODO
		Record rec;
		status = heapfileScan.getRecord(rec);
		if (status != OK) return status;

		int outputOffset = 0;
		for (int i = 0; i < projCnt; i++) {
			if (attrDesc != NULL) {
				if (0 != strcmp(projNames[i].relName, attrDesc->relName))
					continue;
			}

			//copy rec data into outputData
			memcpy(outputData  + outputOffset, (char*)rec.data +
					projNames[i].attrOffset, projNames[i].attrLen);

			outputOffset += projNames[i].attrLen;
		}

		RID outRID;
		status = insertfileScan.insertRecord(outputRec, outRID);
		if (status != OK) return status;
		resultTupCnt++;
	}
	//printf("num_select: %d, %d\n", count, resultTupCnt);//TODO

	//destroy heapfileScan and insertfileScan
	//delete heapfileScan;
	//delete insertfileScan;
	return OK;

}
