#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
	cerr << "At line " << __LINE__ << ":" << endl << "  "; \
	cerr << "This condition should hold: " #c << endl; \
	exit(1); \
} \
}

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
	numBufs = bufs;

	bufTable = new BufDesc[bufs];
	//	memset(bufTable, 0, bufs * sizeof(BufDesc));
	for (int i = 0; i < bufs; i++) {
		bufTable[i] = BufDesc();
	}

	for (int i = 0; i < bufs; i++) 
	{
		bufTable[i].frameNo = i;
		bufTable[i].valid = false;
	}

	bufPool = new Page[bufs];
	memset(bufPool, 0, bufs * sizeof(Page));

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
	hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

	clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

	// flush out all unwritten pages
	for (int i = 0; i < numBufs; i++) 
	{
		BufDesc* tmpbuf = &bufTable[i];
		if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
			cout << "flushing page " << tmpbuf->pageNo
				<< " from frame " << i << endl;
#endif

			tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
		}
	}

	delete [] bufTable;
	delete [] bufPool;
	delete hashTable;
}

/*
   Allocates a free frame using the clock algorithm; if necessary, writin
   a dirty page back to disk. Returns BUFFEREXCEEDED if all buffer frames
   are pinned, UNIXERR if the call to the I/O layer returned an error 
   when a dirty page was being written to disk and OK otherwise.

   frame is the frameNo that is allocated by this function
   return BUFFEREXCEEDED if all pages are pinned
 */
const Status BufMgr::allocBuf(int & frame) 
{
	//printf("%d clockhand\n", clockHand);
	int temp_pinCnt = 0;
	for (int i = 1; i <= numBufs; i++) {
		int index = (clockHand + i) % numBufs;
		if (bufTable[index].valid == false) {//valid bit isn't set
			//then this is the frame to be returned
			frame = bufTable[index].frameNo;
			clockHand = index;
			return OK; 
		}   

		//if this page is valid
		if (bufTable[index].refbit == true) {//if referenced recently
			//clear refbit and continue with the next entry in buf pool
			bufTable[index].refbit = false;
			if (i == numBufs && temp_pinCnt != numBufs) {
				//check to see whether all frames have been pinned
				//	printf("%d temp_pinCnt\n", temp_pinCnt);//TODO
				temp_pinCnt = 0;
				i = 0;
			}
			continue;
		}   

		//if page is valid but not referenced recently
		if (bufTable[index].pinCnt != 0) {//if page is pinned
			//continue with next entry in buf pool
			temp_pinCnt ++;
			//printf("%d middle pinCnt\n", temp_pinCnt);//TODO
			if (i == numBufs && temp_pinCnt != numBufs) {
				//check to see whether all frames have been pinned
				//	printf("%d temp_pinCnt\n", temp_pinCnt);//TODO
				temp_pinCnt = 0;
				i = 0;
			}
			continue;
		}   

		//if page is valid, not referenced recently & not pinned
		if (bufTable[index].dirty == false) {//if it is not dirty
			frame = bufTable[index].frameNo;
			clockHand = index;
			//remove the entry from hash table
			hashTable->remove(bufTable[index].file,
					bufTable[index].pageNo);
			return OK;
		}

		//if page is valid, not referenced recently/pinned & dirty
		if (bufTable[index].dirty == true) {
			//flush page to disk
			if (bufTable[index].file->writePage(bufTable[index].pageNo,
			&(bufPool[index])) != OK) {
				clockHand = index;
				return UNIXERR;
			}
			frame = bufTable[index].frameNo;
			//remove the entry from hash table
			hashTable->remove(bufTable[index].file,
					bufTable[index].pageNo);
			clockHand = index;
			return OK;
		}
		if (i == numBufs && temp_pinCnt != numBufs) {
			//check to see whether all frames have been pinned
			//	printf("%d temp_pinCnt\n", temp_pinCnt);//TODO
			temp_pinCnt = 0;
			i = 0;
		}
	}

	//if all frames are not available

	return BUFFEREXCEEDED;
	}


/*
   readPage is used to access a file stored in the buffer. 
   It first checks if the file is already stored in the buffer. 
   If it is it will pin it and update the page pointer. If the file is 
   not already in the buffer it will call allocBuff. Assuming that passes 
   without an error code it will add the file to the page that
   returned and return it via the page pointer.

   file is the file that wants to be put and read from the buffer
   PageNo is the page within the file
   page is the return pointer that points to the file in the buffer
   returns a status code depending on outcome
   OK if no errors occurred, UNIXERR if a Unix error occurred, 
   BUFFEREXCEEDED if all buffer frames are pinned, HASHTBLERROR 
   if a hash table error occurred.

 */
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
	//	printf("Reading: %d\n", PageNo);
	Status status = OK;
	int frameNo = 0;
	status = hashTable->lookup(file, PageNo, frameNo);
	if (status != OK) {
		//allocate buffer
		status = allocBuf(frameNo);
		if(status != OK){return status;}
		//read in page into bufpool
		status = file->readPage(PageNo, &bufPool[frameNo]);
		if(status != OK){return status;}
		//insert hashtable
		status = hashTable->insert(file, PageNo, frameNo);
		if(status != OK){return status;}
		bufTable[frameNo].Set(file, PageNo);
		page = &bufPool[frameNo];
	//	bufPool[frameNo]= *page;
		return OK;
	} else {
		//Case 2
		//cout << "case 2 \n";
		//set refbit to true
		bufTable[frameNo].refbit = true;
		//increment pinCnt
		bufTable[frameNo].pinCnt++;
		page = &bufPool[frameNo];
		return OK;
	}
}

/**
 * unPinPage is used to remove a pin from a frame stored in the buffer
 * It first decrements the pinCnt of the frame containing (file, PageNo)
 * Then if dirty is true, it updates the dirty bit
 * 
 * file is the file containing the frame to be unpinned
 * PageNo is the page number within the file
 * dirty is a bool representing what the dirty bit for this
 * frame should be
 * 
 * return: OK iff no errors occurred, HASHNOTFOUND if the page is not 
 *in the buffer pool hash table,
 * PAGENOTPINNED if the pin count is already set to 0
 */
const Status BufMgr::unPinPage(File* file, const int PageNo, 
		const bool dirty) 
{
	// Find frame containing (file, pageNo)
	Status status = OK;
	int frameNo = 0;
	status = hashTable->lookup(file, PageNo, frameNo);
	//printf("1\n");
	if (status != OK) {
		return status; // Status will be HASHNOTFOUND in this case
	} else {
		// We now have frameNo, decrement pinCnt
		if (bufTable[frameNo].pinCnt == 0) {return PAGENOTPINNED;} 
		// pinCnt 0 already return PAGENOTPINNED

		bufTable[frameNo].pinCnt = bufTable[frameNo].pinCnt - 1;
		// If dirty == true set the dirty bit
		if (dirty == true) {
			bufTable[frameNo].dirty = dirty;
		}
		return OK;
	}

}

/**
 * Allocate an empty page in a specified file and returnt eh page 
 number of the allocated page
 * Then it obtains a buffer pool fram and inserts an entry into the hash
 table, setting the entry up properly
 * 
 * file is the file which a page is being allocated in
 * pageNo is the page number which was allocated in the file
 * page is a pointer to the buffer frame allocated for the page
 * 
 * Return: OK iff no errors occured, otherwise
 * UNIXERR if a Unix error occurred
 * BUFFEREXCEEDED if all buffer frames are pinned
 * HASHTABLERROR if a hash table error occurred
 */
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
	Status status = OK;
	// Allocate an empty page in the specified file
	status = file->allocatePage(pageNo); // can return UNIXERR
										 //printf("%d page number\n", pageNo);
	if (status != OK) { return status; }
	// We now have pageNo of allocated page
	int frameNo = 0;
	status = allocBuf(frameNo);
	//printf("%d frame number\n", frameNo);
	//printSelf();
	if (status != OK) { return status; } 
	// can return UNIXERR or BUFFEREXCEEDED
	// Now an entry is inserted into the hashTable
	status = hashTable->insert(file, pageNo, frameNo);
	if (status != OK) { return status; } 
	// can return HAShBLERROR
	// Set() is invoked on the frame to set it up
	bufTable[frameNo].Set(file, pageNo);
	page = &bufPool[frameNo];
	//printSelf();
	//printf("here\n");
	return OK;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
	// see if it is in the buffer pool
	Status status = OK;
	int frameNo = 0;
	status = hashTable->lookup(file, pageNo, frameNo);
	if (status == OK)
	{
		// clear the page
		bufTable[frameNo].Clear();
	}
	status = hashTable->remove(file, pageNo);

	// deallocate it in the file
	return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
	Status status;

	for (int i = 0; i < numBufs; i++) {
		BufDesc* tmpbuf = &(bufTable[i]);
		if (tmpbuf->valid == true && tmpbuf->file == file) {

			if (tmpbuf->pinCnt > 0)
				return PAGEPINNED;

			if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
				cout << "flushing page " << tmpbuf->pageNo
					<< " from frame " << i << endl;
#endif
				if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
								&(bufPool[i]))) != OK)
					return status;

				tmpbuf->dirty = false;
			}

			hashTable->remove(file,tmpbuf->pageNo);

			tmpbuf->file = NULL;
			tmpbuf->pageNo = -1;
			tmpbuf->valid = false;
		}

		else if (tmpbuf->valid == false && tmpbuf->file == file)
			return BADBUFFER;
	}

	return OK;
}


void BufMgr::printSelf(void) 
{
	BufDesc* tmpbuf;

	cout << endl << "Print buffer...\n";
	for (int i=0; i<numBufs; i++) {
		tmpbuf = &(bufTable[i]);
		cout << i << "\t" << (char*)(&bufPool[i]) 
			<< "\tpinCnt: " << tmpbuf->pinCnt;
		if (tmpbuf->refbit == true)
			cout << "\tref set";

		if (tmpbuf->valid == true)
			cout << "\tvalid\n";
		cout << endl;
	};
}


