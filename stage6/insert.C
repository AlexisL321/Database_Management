#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
		const int attrCnt, 
		const attrInfo attrList[])
{
	// part 6

	Status status;

	int attrCnt_tmp;
	//get all attributes of a relation
	AttrDesc* attrDesc_addr = (AttrDesc*)malloc(attrCnt*sizeof(AttrDesc));
	//AttrDesc attrDescArray[attrCnt];
	memset(attrDesc_addr, 0, attrCnt*sizeof(AttrDesc));
	status = attrCat->getRelInfo(relation, attrCnt_tmp,
	attrDesc_addr);
	if (status != OK) return status;

	//sum up the length of the data to be added
	int sum = 0;
	for (int i = 0; i < attrCnt; i++) {
		sum += attrDesc_addr[i].attrLen;
	}

	//create outputData to be stored in rec.data later on
	char outputData[sum+1];
	memset(outputData, 0, sum+1);
	Record rec;
	rec.length = sum;
	rec.data = (void*) outputData;

	int insert_cnt = 0;
	//int i = 0;
	int outputOffset = 0;
	while (insert_cnt < attrCnt) {
		for (int j = 0; j < attrCnt; j++) {
			if (attrList[j].attrValue == NULL) continue;
			if (strcmp(attrList[j].attrName,
						attrDesc_addr[insert_cnt].attrName) == 0) {

				int data_type =
					attrDesc_addr[insert_cnt].attrType;
				if (data_type == INTEGER) {
					int* addr = (int*)malloc(sizeof(int));
					int value = atoi((char*)attrList[j].attrValue);
					memcpy((void*)addr, &value, sizeof(int));

					//copy the actual data
					memcpy((outputData+outputOffset), (void*)addr,
							attrDesc_addr[insert_cnt].attrLen);
				}
				else if (data_type == FLOAT) {
					float* addr = (float*) malloc(sizeof(float));
					float value = atof((char*)attrList[j].attrValue);
					memcpy((void*)addr, &value, sizeof(float));
					memcpy((outputData+outputOffset), (void*)addr,
							attrDesc_addr[insert_cnt].attrLen);
				}
				else {
					memcpy((outputData + outputOffset),
							(void*)attrList[j].attrValue,
							attrDesc_addr[insert_cnt].attrLen);
				}

				outputOffset += attrDesc_addr[insert_cnt].attrLen;
				insert_cnt++;
				break;
			}
		}
	}

	//create an insertfileScan object to insertRecord into table
	InsertFileScan insertfileScan(relation, status);

	RID outRID;
	status = insertfileScan.insertRecord(rec, outRID);
	if (status != OK) return status;
	//		delete insertfileScan;
	return OK;
}

