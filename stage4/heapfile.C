#include "heapfile.h"
#include "error.h"

/*
//print out the file
const Status HeapFile::printoutfile()
{
	//Status status;
	FileHdrPage* hdrPage;
	//int hdrPageNo;
	int newPageNo;
	Page* newPage;
	int nextPageNo;

	//HeapFile* file = new HeapFile(fileName, status);
	//if (status != OK) return status;

	hdrPage = headerPage;
	//print out header file info
	cout<<endl;
	printf("header file:\n");
	printf("firstPageNo:%d lastPageNo:%d pageCnt:%d recCnt:%d"
			"\n", hdrPage->firstPage, hdrPage->lastPage,
			hdrPage->pageCnt, hdrPage->recCnt);

	//print out pages within this heapfile
	newPageNo = hdrPage->firstPage;
	bufMgr->readPage(filePtr, newPageNo, newPage);
	newPage->getNextPage(nextPageNo);
	printf("Pages within this heapfile\n");
	printf("curPageNo:%d nextPageNo:%d slotCnt:%d\n",
			newPageNo, nextPageNo, newPage->getslotCnt());
	printf("dumping page content:\n");
	newPage->dumpPage();
	//unpin the first page
	bufMgr->unPinPage(filePtr, newPageNo, false);

	while(nextPageNo != -1) {
		newPageNo = nextPageNo;
		bufMgr->readPage(filePtr, newPageNo, newPage);
		newPage->getNextPage(nextPageNo);
		printf("curPageNo:%d nextPageNo:%d slotCnt:%d\n",
				newPageNo, nextPageNo, newPage->getslotCnt());
		printf("dumping page content:\n");
		newPage->dumpPage();
		//unpin this page
		bufMgr->unPinPage(filePtr, newPageNo, false);
	}
	return OK;
}
*/

/*
   This function creates an empty heapfile. It opens the file with
   fileName and returns FILEEXISTS if the file has been opened.

   fileName is the name of the file that the heapfile will be based
   on
 */
// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
	File* 		file = NULL;
	Status 		status;
	FileHdrPage*	hdrPage;
	int			hdrPageNo;
	int			newPageNo;
	Page*		newPage;

	// try to open the file. This should return an error
	status = db.openFile(fileName, file);
	if (status != OK)
	{
		// file doesn't exist. First create it and allocate
		// an empty header page and data page.
		status = db.createFile(fileName);		
		if (status != OK) return status;
		//initialize file
		status = db.openFile(fileName, file);
		if (status!=OK) return status;

		//allocate page for header file
		status = bufMgr->allocPage(file, hdrPageNo, newPage);
		if (status != OK) return status;
		hdrPage = (FileHdrPage*) newPage;

		//initialize fields in header file
		for (int i = 0; i < (int)MAXNAMESIZE; i++) {
			if (fileName[i] == '\0') {
				hdrPage->fileName[i] = '\0';
				break;
			}
			hdrPage->fileName[i] = fileName[i];
		}
		hdrPage->firstPage = 0;
		hdrPage->lastPage = 0;
		hdrPage->pageCnt = 0;
		//hdrPage->recCnt = 0;//TODO: maybe newPage->slotCnt?

		//allocate the first page
		status = bufMgr->allocPage(file, newPageNo, newPage);
		if (status != OK) return status;

		//initialize newPage
		newPage->init(newPageNo);

		//change the headerfile fields
		hdrPage->firstPage = newPageNo;
		hdrPage->lastPage = newPageNo;
		hdrPage->pageCnt = 1;

		//unpin pages and mark them as dirty
		status = bufMgr->unPinPage(file, newPageNo, true);
		if (status != OK) return status;
		status = bufMgr->unPinPage(file, hdrPageNo, true);
		if (status != OK) return status;

		//flush the file and close the file
		status = bufMgr->flushFile(file);
		if (status != OK) return status;
		status = db.closeFile(file);
		if (status!=OK) return status;
		return OK;
	}

	return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

/*
   constructor opens the underlying file

   fileName is the name of the file to be opened and the heapfile
   will be based on this file
   returnStatus is the return status of this function
 */
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
	Status 	status;
	//Page*	pagePtr;

	cout << "opening file " << fileName << endl;

	// open the file and read in the header page and the first data page
	if ((status = db.openFile(fileName, filePtr)) == OK)
	{
		File* file = filePtr;

		//read and pin the headerpage in the bufPool
		filePtr->getFirstPage(headerPageNo);
		status = bufMgr->readPage(file, headerPageNo, curPage);
		if (status != OK) {
			returnStatus = status;
			return;
		}

		headerPage = (FileHdrPage*) curPage;
		hdrDirtyFlag = false;

		//read and pin the first page in the bufPool
		curPageNo = headerPage->firstPage;
		status = bufMgr->readPage(file, curPageNo, curPage);
		if (status != OK) {
			returnStatus = status;
			return;
		}

		curDirtyFlag = false;
		curRec = NULLRID;
		returnStatus = status;

	}
	else
	{
		cerr << "open of heap file failed\n";
		returnStatus = status;
		return;
	}
}

// the destructor closes the file
HeapFile::~HeapFile()
{
	Status status;
	cout << "invoking heapfile destructor on file " << headerPage->fileName 
		<< endl;

	// see if there is a pinned data page. If so, unpin it 
	if (curPage != NULL)
	{
		status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
	}

	// unpin the header page
	status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
	if (status != OK) cerr << "error in unpin of header page\n";

	// before close the file
	status = db.closeFile(filePtr);
	if (status != OK)
	{
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
	}
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
	return headerPage->recCnt;
}

/*
// retrieve record specified by rid from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter
 */
const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
	Status status;

	//check whether the curPage is empty
	if (curPage == NULL) {//page empty

		//read the appropriate page with rid into buffer
		curPageNo = rid.pageNo;
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;

		//call Page::getRecord
		curPage->getRecord(rid, rec);
		curDirtyFlag = false;
		curRec.pageNo = rid.pageNo;
		curRec.slotNo = rid.slotNo;
		return OK;
	}
	else {//page not empty

		//first check whether the current pinned page contain rid
		if (curPageNo == rid.pageNo)
		{//contains it

			status = curPage->getRecord(rid, rec);
			//Page::getRecord doesn't check whether it is on
			//the correct page so you have to check the page
			//is the correct page manually youself

			if (status != OK) return status;
			curRec.pageNo = rid.pageNo;
			curRec.slotNo = rid.slotNo;
			/*
			   cout<<"int "<<*((int*)rec.data)<<" data "<< 
			   (char*)((char*)rec.data + 8)<< " length "
			   << rec.length<<endl;//TODO
			   cout<<"curPageNo: "<<curPageNo<<endl;
			   cout<<"rid, pageNo: "<<rid.pageNo<<" slotNo: "
			   <<rid.slotNo<<endl;
			 */
			//	curRec = rid;
			return OK;
		}

		//if it doesn't contain
		//first unpin this page
		bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);

		//get the current page to curPage
		status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
		if (status != OK) return status;

		curPage->getRecord(rid, rec);
		curPageNo = rid.pageNo;
		curDirtyFlag = false;
		curRec.pageNo = rid.pageNo;
		curRec.slotNo = rid.slotNo;
		return OK;
	}
}

HeapFileScan::HeapFileScan(const string & name,
		Status & status) : HeapFile(name, status)
{
	filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
		const int length_,
		const Datatype type_, 
		const char* filter_,
		const Operator op_)
{
	if (!filter_) {                        // no filtering requested
		filter = NULL;
		return OK;
	}

	if ((offset_ < 0 || length_ < 1) ||
			(type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
			((type_ == INTEGER && length_ != sizeof(int))
			 || (type_ == FLOAT && length_ != sizeof(float))) ||
			(op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE 
			 && op_ != GT && op_ != NE))
	{
		return BADSCANPARM;
	}

	offset = offset_;
	length = length_;
	type = type_;
	filter = filter_;
	op = op_;

	return OK;
}


const Status HeapFileScan::endScan()
{
	Status status;
	// generally must unpin last page of the scan
	if (curPage != NULL)
	{
		status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		return status;
	}
	return OK;
}

HeapFileScan::~HeapFileScan()
{
	endScan();
}

const Status HeapFileScan::markScan()
{
	// make a snapshot of the state of the scan
	markedPageNo = curPageNo;
	markedRec = curRec;
	return OK;
}

const Status HeapFileScan::resetScan()
{
	Status status;
	if (markedPageNo != curPageNo) 
	{
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
	}
	else curRec = markedRec;
	return OK;
}


/*
   Returns (via the outRid parameter) the RID of the next record 
   that satisfies the scan predicate.
 */
const Status HeapFileScan::scanNext(RID& outRid)
{
	Status 	status = OK;
	RID		nextRid;
	//RID		tmpRid;
	int 	nextPageNo;
	Record      rec;

	//TODO
	if (curPageNo == -1) {
		return FILEEOF;
	}

	//cout<<"fileter:"<<(char*) filter << endl;
	//check whether curPage is null
	if (curPage == NULL) {
		//cout<<"CurPage is null"<<endl;//TODO
		status = bufMgr->readPage(filePtr, headerPage->firstPage, curPage);
		if (status != OK) return status;
		curPageNo = headerPage->firstPage;
		curDirtyFlag = false;
	}

	//TODO
	/*
	//check whether filter is NULL
	if (!filter) {
	//cout<<"filter is null\n";//TODO
	//check whether curRec is NULL
	//cout<<"curRec.pageNo: "<<curRec.pageNo<<" curRec.slotNo: "
	//<<curRec.slotNo<<endl;//TODO
	if (curRec.pageNo == -1 && curRec.slotNo == -1) {
	//if curRec is NULLRID, then just return the first rec of the 
	//curPage
	status = curPage->firstRecord(nextRid);
	if (status == NORECORDS) {
	//check whether this is the end of file
	status = bufMgr->unPinPage(filePtr, curPageNo,
	curDirtyFlag);
	if (status != OK) return status;
	curPageNo = -1;
	curPage = NULL;
	return FILEEOF;
	}
	curRec.pageNo = nextRid.pageNo;
	curRec.slotNo = nextRid.slotNo;
	outRid.pageNo = nextRid.pageNo;
	outRid.slotNo = nextRid.slotNo;
	return OK;
	}
	//get the next record
	status = curPage->nextRecord(curRec, nextRid);
	//if reached the end of this curPage
	if (status == ENDOFPAGE) {
	curPage->getNextPage(nextPageNo);
	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
	if (status != OK) return status;
	if (nextPageNo == -1) {
	//if it is the last page
	curPageNo = -1;
	curPage = NULL;
	return FILEEOF;
	}
	status = bufMgr->readPage(filePtr, nextPageNo, curPage);
	if (status != OK) return status;
	curPageNo = nextPageNo;
	curDirtyFlag = false;

	status = curPage->firstRecord(nextRid);
	if (status == NORECORDS) {
	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
	if (status != OK) return status;
	curPageNo = -1;
	curPage = NULL;
	return FILEEOF;
	}
	}

	curRec.pageNo = nextRid.pageNo;
	curRec.slotNo = nextRid.slotNo;
	outRid.pageNo = nextRid.pageNo;
	outRid.slotNo = nextRid.slotNo;
	return OK;
	}

	 */
	//when the filter is not NULL
	if (curRec.pageNo == -1 || curRec.slotNo == -1) {
		status = curPage->firstRecord(nextRid);
		//TODO
		/*
		   if (status == NORECORDS) {
		//check whether this is the end of file
		status = bufMgr->unPinPage(filePtr, curPageNo,
		curDirtyFlag);
		if (status != OK) return status;
		curPageNo = -1;
		curPage = NULL;
		return FILEEOF;
		}		
		 */
		if (status == OK) {//TODO
			curRec.pageNo = nextRid.pageNo;
			curRec.slotNo = nextRid.slotNo;
			curPage->getRecord(curRec, rec);
			if (matchRec(rec) == true) {
				outRid.pageNo = nextRid.pageNo;
				outRid.slotNo = nextRid.slotNo;
				return OK;
			}
		}
	}
	/*
	   tmpRid.pageNo = curRec.pageNo;
	   tmpRid.slotNo = curRec.slotNo;
	 */

	//check all rec in the current page
	while (curPage->nextRecord(curRec, nextRid) == OK) {
		curRec.pageNo = nextRid.pageNo;
		curRec.slotNo = nextRid.slotNo;
		status = curPage->getRecord(nextRid, rec);
		if (status != OK) return status;

		if (matchRec(rec) == true) {
			outRid.pageNo = nextRid.pageNo;
			outRid.slotNo = nextRid.slotNo;
			return OK;
		}
		//if doesn't match the predicate
	}

	//loop through each record in each page
	while (curPage->getNextPage(nextPageNo) == OK &&
			nextPageNo != -1) {
		//unpin curPage
		status = bufMgr->unPinPage(filePtr, curPageNo,
				curDirtyFlag);
		if (status != OK) return status;
		//read nextpage & update curPage to nextPage
		status = bufMgr->readPage(filePtr, nextPageNo,
				curPage);
		if (status != OK) return status;
		curPageNo = nextPageNo;
		curDirtyFlag = false;

		//check all recs in next page
		status = curPage->firstRecord(curRec);
		if (status == NORECORDS) {
			/*
			//check whether this is the end of file
			status = bufMgr->unPinPage(filePtr, curPageNo,
			curDirtyFlag);
			if (status != OK) return status;
			curPageNo = -1;
			curPage = NULL;
			return FILEEOF;
			 */
			continue;
		}
		curPage->getRecord(curRec, rec);
		if (matchRec(rec) == true) {
			outRid.pageNo = nextRid.pageNo;
			outRid.slotNo = nextRid.slotNo;
			return OK;
		}

		//if first record doesn't match, loop through the rest of
		//the record
		while (curPage->nextRecord(curRec, nextRid) == OK) {
			curRec.pageNo = nextRid.pageNo;
			curRec.slotNo = nextRid.slotNo;
			curPage->getRecord(nextRid, rec);

			if (matchRec(rec) == true) {
				outRid.pageNo = nextRid.pageNo;
				outRid.slotNo = nextRid.slotNo;
				return OK;
			}
		}
	}

	//unpin the last page if the scanner ever hits the last page
	if (curPage->getNextPage(nextPageNo) == OK &&
			nextPageNo == -1) {
		status = bufMgr->unPinPage(filePtr, curPageNo,
				curDirtyFlag);
		if (status != OK) return status;
		curPage = NULL;
		curPageNo = -1;
	}
	return FILEEOF;
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
	return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
	Status status;

	// delete the "current" record from the page
	status = curPage->deleteRecord(curRec);
	curDirtyFlag = true;

	// reduce count of number of records in the file
	headerPage->recCnt--;
	hdrDirtyFlag = true; 
	return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
	curDirtyFlag = true;
	return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
	// no filtering requested
	if (!filter) return true;

	// see if offset + length is beyond end of record
	// maybe this should be an error???
	if ((offset + length -1 ) >= rec.length)
		return false;

	float diff = 0;                       // < 0 if attr < fltr
	switch(type) {

		case INTEGER:
			int iattr, ifltr;    // word-alignment problem possible
			memcpy(&iattr,
					(char *)rec.data + offset,
					length);
			memcpy(&ifltr,
					filter,
					length);
			diff = iattr - ifltr;
			break;

		case FLOAT:
			float fattr, ffltr; // word-alignment problem possible
			memcpy(&fattr,
					(char *)rec.data + offset,
					length);
			memcpy(&ffltr,
					filter,
					length);
			diff = fattr - ffltr;
			break;

		case STRING:
			diff = strncmp((char *)rec.data + offset,
					filter,
					length);
			break;
	}

	switch(op) {
		case LT:  if (diff < 0.0) return true; break;
		case LTE: if (diff <= 0.0) return true; break;
		case EQ:  if (diff == 0.0) return true; break;
		case GTE: if (diff >= 0.0) return true; break;
		case GT:  if (diff > 0.0) return true; break;
		case NE:  if (diff != 0.0) return true; break;
	}

	return false;
}

InsertFileScan::InsertFileScan(const string & name,
		Status & status) : HeapFile(name, status)
{
	//Do nothing. Heapfile constructor will bread the header page and the first
	// data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
	Status status;
	// unpin last page of the scan
	if (curPage != NULL)
	{
		status = bufMgr->unPinPage(filePtr, curPageNo, true);
		curPage = NULL;
		curPageNo = 0;
		if (status != OK) cerr << "error in unpin of data page\n";
	}
}

/*
// Insert a record specified by rec into the file
// returning the RID of the inserted record in outRid.
*/
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
	Page*	newPage;
	int		newPageNo;
	Status	status, unpinstatus;
	//RID		rid;

	// check for very large records
	if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
	{
		// will never fit on a page, so don't even bother looking
		return INVALIDRECLEN;
	}

	if (curPage == NULL) {
		//read in last page into the curPage
		status = bufMgr->readPage(filePtr, headerPage->lastPage,
				curPage);
		if (status != OK) return status;
		curPageNo = headerPage->lastPage;
	}

	int cur_next_pageNo;//the next pageNo for this cur page
	curPage->getNextPage(cur_next_pageNo);

	//insert record into the current page
	status = curPage->insertRecord(rec, outRid);
	if (status != OK) {
		//first unpin the last page
		//must set the dirty to true, but why? I have
		//literally flush the page after every insert
		bufMgr->unPinPage(filePtr, curPageNo, true);

		//no space, create a new page
		unpinstatus = bufMgr->allocPage(filePtr, newPageNo, newPage);
		if (unpinstatus != OK) return unpinstatus;
		newPage->init(newPageNo);
		//curPage = newPage;

		//update header file fields
		if (curPageNo == headerPage->lastPage) {
			//when the current page is the last page
			headerPage->lastPage = newPageNo;
		}
		curPage->setNextPage(newPageNo);//curPate-->newPage

		newPage->setNextPage(cur_next_pageNo);//newPage-->curPage.nextPage
		curPage = newPage;
		curPageNo = newPageNo;
		curDirtyFlag = true;
		headerPage->pageCnt++;

		//insert record
		unpinstatus = curPage->insertRecord(rec, outRid);
		if (unpinstatus != OK) return unpinstatus;

		headerPage->recCnt ++;
		hdrDirtyFlag = true;
		bufMgr->flushFile(filePtr);//TODO
		return OK;
	}

	else {
		//successful, do the bookkeeping
		headerPage->recCnt++;
		curDirtyFlag = true;
		hdrDirtyFlag = true;
		bufMgr->flushFile(filePtr);

		return OK;
	}
}

