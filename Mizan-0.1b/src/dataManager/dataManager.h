/*
 * dataManager.h
 *
 *  Created on: Mar 25, 2012!]
 *      Author: Zuhair Khayyat
 */

#ifndef DATAMANAGER_H_
#define DATAMANAGER_H_
#include "graphReaders/IgraphReader.h"
#include "graphReaders/hdfsGraphReader.h"
#include "graphWriters/hdfsGraphWriter.h"
#include "graphReaders/sharedDiskGraphReader.h"
#include "dataStructures/graph/mObject.h"
#include "dataStructures/graph/mObjectMini.h"
#include "dataStructures/data/mLong.h"
#include "../communication/dataStructures/general.h"
#include <cstdio>
#include <unistd.h>
#include <iostream>
#include <map>
#include <vector>
#include <list>
#include <iterator>
#include "../dataManager/dataStructures/data/mGPMType.h"
//#include "../algorithms/patternMatching.h"

typedef struct{
	long long srcId;
	long long dstId;
}SInputVertices;


static std::vector<SInputVertices> g_inputVertices;

static std::vector<SInputVertices> g_validVertices;

static std::vector<STriplets> g_queryTriplets;
static std::vector<SVertexArray> g_queryVertex;

//work as input data graph
static std::vector<SVertexArray> g_dataVertex;

static std::map<long long, std::vector<long long> > g_queryCollect;

//work as output data graph
static std::vector<SVertexArray> check_dataVertex;



template<class K, class V1, class M, class A> class DHT;

template<class K, class V1, class M, class A>
class dataManager {
private:
	static const fileSystem defInType = HDFS;

	//Mizan<K, V> * mizanPtr;
	int itIndex;
	//Variables
	distType dataDist;
	char * inpupGraphPath;
	fileSystem fsInType;
	int readBufferSize;

	IgraphReader * myDataConnector;
	std::vector<mObject<K, V1, M> *> data;
	std::map<K, int> dataLocation;

	std::vector<mObjectMini<K, V1> *> nbrData;
	std::map<K, int> nbrDataLocation;

	std::vector<mObject<K, V1, M> *> softDynamicVertexSend;
	std::map<K, int> softDynamicVertexSendDst;

	int stolenIndex;
	std::list<K> stolen;

	int myComputeRank;

	int debugFlag;
	int myID;

	int countEdges;
	K minVertex;
	K maxVertex;

	boost::mutex dataAccessLock;
	boost::mutex stolenVertexLock;
	boost::mutex stolenVertexContinueLock;
	//boost::mutex insert_message;

	int storageType;

public:

	//Constructors

	dataManager(char * inputPath, fileSystem inType, int inStorageType) :
			inpupGraphPath(inputPath), fsInType(inType), readBufferSize(-1), storageType(
					inStorageType) {
		init();
	}

	dataManager(char * inputPath, int maxBufferSize, int inStorageType) :
			inpupGraphPath(inputPath), fsInType(defInType), readBufferSize(
					maxBufferSize), storageType(inStorageType) {
		init();
	}

	dataManager(char * inputPath, fileSystem inType, int maxBufferSize,
			int computeRank, int inStorageType) :
			inpupGraphPath(inputPath), fsInType(inType), readBufferSize(
					maxBufferSize), storageType(inStorageType) {
		init();
	}

	int getCountEdges() {
		return countEdges;
	}
	K getMaxVertex() {
		return maxVertex;
	}
	K getMinVertex() {
		return minVertex;
	}

	long getTotalSystemMemory() {
		long pages = sysconf(_SC_PHYS_PAGES);
		long page_size = sysconf(_SC_PAGE_SIZE);
		return pages * page_size;
	}
	long getAvaliableSystemMemory() {
		long pages = sysconf(_SC_AVPHYS_PAGES);
		long page_size = sysconf(_SC_PAGE_SIZE);
		return pages * page_size;
	}
	double getAvaliableSystemMemoryPercent() {
		return (double) ((double) getAvaliableSystemMemory()
				/ (double) getTotalSystemMemory());
	}
	bool haveMoreMemory() {
		return ((float) getAvaliableSystemMemory()
				/ (float) getTotalSystemMemory() > 0.1);
	}

	void setComputeRank(int computeRank) {
		myComputeRank = computeRank;
	}

	/*
	 void setMizan(Mizan<K, V> * mizanInstance) {
	 mizanPtr = mizanInstance;
	 }

	 int getPartID() {
	 mizanPtr->getPeID();
	 }
	 */

	void initDebug() {
		debugFlag = 1;
		int start = myComputeRank * 10;
		int end = (myComputeRank + 1) * 10;

		for (int i = start; i < end; i++) {
			mLong a(i);
			mLong a1(i + 1);
			mLong a2(i - 1);
			int location = data.size();
			dataLocation[a] = location;
			mObject<K, V1, M> * vertexObj = new mObject<K, V1, M>(a);
			vertexObj->addOutEdge(a1);
			//vertexObj->addInEdge(a2);
			data.push_back(vertexObj);
		}
		if (myComputeRank == 2) {
			mLong a(30);
			mLong a1(0);
			mLong a2(29);
			int location = data.size();
			dataLocation[a] = location;
			mObject<K, V1, M> * vertexObj = new mObject<K, V1, M>(a);
			vertexObj->addOutEdge(a1);
			vertexObj->addInEdge(a2);
			data.push_back(vertexObj);
		}
	}
	void init() {
		itIndex = 0;
		countEdges = 0;
		stoleVertexCnt = 0;
		debugFlag = 0;

//		std::cout << "fsInType : " << fsInType << endl;

		if (fsInType == HDFS) {
			myDataConnector = new hdfsGraphReader(inpupGraphPath,
					readBufferSize);
//			std::cout << "\t-Using HDFS as input source with file: "
//					<< inpupGraphPath << ", HDFS status = "
//					<< myDataConnector->isFileReadable() << std::endl;
		} else if (fsInType == OS_DISK_ALL) {
			myDataConnector = new sharedDiskGraphReader(inpupGraphPath,
					readBufferSize);
			std::cout << "\t-Using Shared Disk as input source with file: "
					<< inpupGraphPath << ", status = "
					<< myDataConnector->isFileReadable() << std::endl;
		}
	}
	void readIntoMemory() {
		line = 0;
#ifdef Verbose
		const clock_t begin_time = clock();
#endif

		if (debugFlag == 0) {
			int bufferSize = 1;
			std::vector<char *> tmpStorage;
			fileStatus stat = success;
			//cout << "I am before the while " << endl;
			bool srcCut = false;
			int reqBlockSize = myDataConnector->getDefaultIBlockSize();
			char * buffer = (char*) calloc(reqBlockSize + 1, sizeof(char));
			while (bufferSize > 0 && stat == success) {
				buffer[0] = 0;
				stat = myDataConnector->readBlock(buffer, bufferSize,
						reqBlockSize);
				buffer[reqBlockSize] = 0;
				buffer[bufferSize] = 0;
//				cout << "PE" << myID << " bufferSize = " << bufferSize << std::endl;
//				cout.flush();
				if (stat == success && bufferSize > 0) {
					transBuffer(buffer, bufferSize, tmpStorage, srcCut);
				}
			}
			if (tmpStorage.size() == 4) {
				readOnce(tmpStorage);
			}
			free(buffer);
//			cout << " tmpStorage.size() = " << tmpStorage.size() << " for file "
//					<< inpupGraphPath << std::endl;
		}
#ifdef Verbose
//		std::cout << "-----TIME: Data Loading for " << myID << " = " << float( clock () - begin_time ) / CLOCKS_PER_SEC << std::endl;
#endif
	}

	void readOnce(std::vector<char *> &tmpStorage) {
		char tmpSrc[1000], tmpDst[1000], tmpSrcLoc[10], tmpDstLoc[10];
		char * tmpStrPtr1 = tmpStorage.back();
		strcpy(tmpSrc, tmpStorage.back());
		tmpStorage.pop_back();
		char * tmpStrPtr2 = tmpStorage.back();
		strcpy(tmpSrcLoc, tmpStorage.back());
		tmpStorage.pop_back();
		char * tmpStrPtr3 = tmpStorage.back();
		strcpy(tmpDst, tmpStorage.back());
		tmpStorage.pop_back();
		char * tmpStrPtr4 = tmpStorage.back();
		strcpy(tmpDstLoc, tmpStorage.back());
		tmpStorage.pop_back();
		free(tmpStrPtr1);
		free(tmpStrPtr2);
		free(tmpStrPtr3);
		free(tmpStrPtr4);

		K srcVer;
		K dstVer;
		int srcVerLoc = atoi(tmpSrcLoc);
		int dstVerLoc = atoi(tmpDstLoc);

		srcVer.readFromCharArray(tmpSrc);
		dstVer.readFromCharArray(tmpDst);

		printf("readOnce\n");

		writeToMemGraph(srcVer, srcVerLoc, dstVer, dstVerLoc);
	}
	void transBuffer(char * inputBuffer, int size,
			std::vector<char *> &tmpStorage, bool &strCut) {


		char tmpSrc[1000], tmpDst[1000], tmpSrcLoc[10], tmpDstLoc[10];
		int ptr = 0;
		int bla = 0;
		int itemPtr = 0;
		int ignoreList = 0;

		int loop = 0;

		char lastChar = inputBuffer[ptr];

		if (lastChar == ' ' || lastChar == 10 || lastChar == 13) {
			strCut = false;
			while ((lastChar == ' ' || lastChar == 10 || lastChar == 13)
					&& ptr < size) {
				ptr++;
				lastChar = inputBuffer[ptr];
			}
		}

		if (tmpStorage.size() != 0) {
			if (tmpStorage.size() == 1) {
				char * tmpStrPtr = tmpStorage.back();
				strcpy(tmpSrc, tmpStorage.back());
				tmpStorage.pop_back();
				ignoreList = 1;
				itemPtr = 1;
				free(tmpStrPtr);
			} else if (tmpStorage.size() == 2) {
				char * tmpStrPtr1 = tmpStorage.back();
				strcpy(tmpSrc, tmpStorage.back());
				tmpStorage.pop_back();
				char * tmpStrPtr2 = tmpStorage.back();
				strcpy(tmpSrcLoc, tmpStorage.back());
				tmpStorage.pop_back();
				ignoreList = 2;
				itemPtr = 2;
				free(tmpStrPtr1);
				free(tmpStrPtr2);
			} else if (tmpStorage.size() == 3) {
				char * tmpStrPtr1 = tmpStorage.back();
				strcpy(tmpSrc, tmpStorage.back());
				tmpStorage.pop_back();
				char * tmpStrPtr2 = tmpStorage.back();
				strcpy(tmpSrcLoc, tmpStorage.back());
				tmpStorage.pop_back();
				char * tmpStrPtr3 = tmpStorage.back();
				strcpy(tmpDst, tmpStorage.back());
				tmpStorage.pop_back();
				ignoreList = 3;
				itemPtr = 3;
				free(tmpStrPtr1);
				free(tmpStrPtr2);
				free(tmpStrPtr3);
			} else if (tmpStorage.size() == 4) {
				char * tmpStrPtr1 = tmpStorage.back();
				strcpy(tmpSrc, tmpStorage.back());
				tmpStorage.pop_back();
				char * tmpStrPtr2 = tmpStorage.back();
				strcpy(tmpSrcLoc, tmpStorage.back());
				tmpStorage.pop_back();
				char * tmpStrPtr3 = tmpStorage.back();
				strcpy(tmpDst, tmpStorage.back());
				tmpStorage.pop_back();
				char * tmpStrPtr4 = tmpStorage.back();
				strcpy(tmpDstLoc, tmpStorage.back());
				tmpStorage.pop_back();
				ignoreList = 4;
				itemPtr = 4;
				free(tmpStrPtr1);
				free(tmpStrPtr2);
				free(tmpStrPtr3);
				free(tmpStrPtr4);
				tmpStorage.clear();
			}
			if (strCut) {
				char tmpStr[1000];
				sscanf(&inputBuffer[ptr], "%s", tmpStr);
				ptr += strlen(tmpStr) + 1;
				if (itemPtr == 1) {
					strcat(tmpSrc, tmpStr);
				}
				if (itemPtr == 2) {
					strcat(tmpSrcLoc, tmpStr);
				}
				if (itemPtr == 3) {
					strcat(tmpDst, tmpStr);
				}
				if (itemPtr == 4) {
					strcat(tmpDstLoc, tmpStr);
				}
			}
		}

		//int aa = 1;
		while (ptr < size) {
			if (ptr < size && (ignoreList--) < 1) {
				sscanf(&inputBuffer[ptr], "%s", tmpSrc);
				ptr += strlen(tmpSrc) + 1;
				itemPtr++;
			}
			if (ptr < size && (ignoreList--) < 1) {
				sscanf(&inputBuffer[ptr], "%s", tmpSrcLoc);
				ptr += strlen(tmpSrcLoc) + 1;
				itemPtr++;
			}
			if (ptr < size && (ignoreList--) < 1) {
				sscanf(&inputBuffer[ptr], "%s", tmpDst);
				ptr += strlen(tmpDst) + 1;
				itemPtr++;
			}
			if (ptr < size && (ignoreList--) < 1) {
				sscanf(&inputBuffer[ptr], "%s", tmpDstLoc);
				ptr += strlen(tmpDstLoc) + 1;
				itemPtr++;
			}

			bool cut = false;
			if (ptr != 0) {
				cut = checkForCutPtr(inputBuffer, ptr);
			}

			if (itemPtr == 4 && !cut) {
				K srcVer;
				K dstVer;
				int srcVerLoc = atoi(tmpSrcLoc);
				int dstVerLoc = atoi(tmpDstLoc);

				srcVer.readFromCharArray(tmpSrc);
				dstVer.readFromCharArray(tmpDst);

				//				if (myComputeRank == 0) {
				//					cout << ((mLong) srcVer).getValue() << " " << srcVerLoc
				//							<< " " << ((mLong) dstVer).getValue() << " "
				//							<< dstVerLoc << endl;
				//				}
				//				loop++;

				writeToMemGraph(srcVer, srcVerLoc, dstVer, dstVerLoc);

			} else if (itemPtr == 1) {
				char * tmpSrcHeap = (char*) calloc(strlen(tmpSrc) + 1,
						sizeof(char));
				strcpy(tmpSrcHeap, tmpSrc);
				tmpStorage.push_back(tmpSrcHeap);
				/*if (myComputeRank == 0) {
				 std::cout << "\t-Pushing 1: " << tmpSrc << std::endl;
				 }*/
			} else if (itemPtr == 2) {
				char * tmpSrcHeap = (char*) calloc(strlen(tmpSrc) + 1,
						sizeof(char));
				strcpy(tmpSrcHeap, tmpSrc);

				char * tmpSrcLocHeap = (char*) calloc(strlen(tmpSrcLoc) + 1,
						sizeof(char));
				strcpy(tmpSrcLocHeap, tmpSrcLoc);

				tmpStorage.push_back(tmpSrcLocHeap);
				tmpStorage.push_back(tmpSrcHeap);
				/*if (myComputeRank == 0) {
				 std::cout << "\t-Pushing 2: " << tmpSrc << " " << tmpSrcLoc
				 << std::endl;
				 }*/
			} else if (itemPtr == 3) {
				char * tmpSrcHeap = (char*) calloc(strlen(tmpSrc) + 1,
						sizeof(char));
				strcpy(tmpSrcHeap, tmpSrc);

				char * tmpSrcLocHeap = (char*) calloc(strlen(tmpSrcLoc) + 1,
						sizeof(char));
				strcpy(tmpSrcLocHeap, tmpSrcLoc);

				char * tmpDstHeap = (char*) calloc(strlen(tmpDst) + 1,
						sizeof(char));
				strcpy(tmpDstHeap, tmpDst);

				tmpStorage.push_back(tmpDstHeap);
				tmpStorage.push_back(tmpSrcLocHeap);
				tmpStorage.push_back(tmpSrcHeap);
				/*if (myComputeRank == 0) {
				 std::cout << "\t-Pushing 3: " << tmpSrc << " " << tmpSrcLoc
				 << " " << tmpDst << std::endl;
				 }*/
			} else if (itemPtr == 4) {
				char * tmpSrcHeap = (char*) calloc(strlen(tmpSrc) + 1,
						sizeof(char));
				strcpy(tmpSrcHeap, tmpSrc);

				char * tmpSrcLocHeap = (char*) calloc(strlen(tmpSrcLoc) + 1,
						sizeof(char));
				strcpy(tmpSrcLocHeap, tmpSrcLoc);

				char * tmpDstHeap = (char*) calloc(strlen(tmpDst) + 1,
						sizeof(char));
				strcpy(tmpDstHeap, tmpDst);

				char * tmpDstLocHeap = (char*) calloc(strlen(tmpDstLoc) + 1,
						sizeof(char));
				strcpy(tmpDstLocHeap, tmpDstLoc);

				tmpStorage.push_back(tmpDstLocHeap);
				tmpStorage.push_back(tmpDstHeap);
				tmpStorage.push_back(tmpSrcLocHeap);
				tmpStorage.push_back(tmpSrcHeap);
				/*if (myComputeRank == 0) {
				 std::cout << "\t-Pushing 3: " << tmpSrc << " " << tmpSrcLoc
				 << " " << tmpDst << std::endl;
				 }*/
			}

			itemPtr = 0;
			ignoreList = 0;
		}
		strCut = checkForCut(inputBuffer, size, ptr);
		/*if (myComputeRank == 2) {
		 cout << "Last char = " << ((int) inputBuffer[size - 1]) << << endl;
		 }*/

		//std::cout << "I finished transBuffer" << std::endl;
		//std::cout << "ptr " << ptr << " size " << size << " itemPtr " << itemPtr
		//<< std::endl;
	}
	bool checkForCutPtr(char * inputBuffer, int ptr) {

		//cout << "bla = " << (int)inputBuffer[size-3] << (int)inputBuffer[size-2] << (int)inputBuffer[size-1] << endl;

		bool tmp = true;
		char lastChar;
		lastChar = inputBuffer[ptr - 1];

		if (lastChar == ' ' || lastChar == 10 || lastChar == 13) {
			tmp = false;
		}
		return tmp;
	}
	bool checkForCut(char * inputBuffer, int size, int ptr) {

		//cout << "bla = " << (int)inputBuffer[size-3] << (int)inputBuffer[size-2] << (int)inputBuffer[size-1] << endl;

		bool tmp = true;
		char lastChar;
		if (ptr < size) {
			lastChar = inputBuffer[ptr - 1];
		} else {
			lastChar = inputBuffer[size - 1];
		}
		if (lastChar == ' ' || lastChar == 10 || lastChar == 13
				|| lastChar == 0) {
			tmp = false;
		}
		return tmp;
	}
	int line;


	bool myfunction (int i, int j) {
	  return (i==j);
	}

	vector<long long > UniqueVec(vector<long long > &verticesQuery, long long id){

		std::vector<long long>::iterator it;

		for(it=verticesQuery.begin(); it!=verticesQuery.end();it++){
			if(*it == id){
				return verticesQuery;
			}
		}

		verticesQuery.push_back(id);
		return verticesQuery;
	}


	void CreateTriplets(){

//		std::cout << "g_inputVertices size: " << g_inputVertices.size() <<std::endl;
//		std::cout << "g_dataVertex size: " << g_dataVertex.size() <<std::endl;
//		std::cout << "verticesQuery size: " << verticesQuery.size() <<std::endl;

		if(g_dataVertex.size()>0){
			return;
		}

		std::list<long long> vertices;

		std::vector<SInputVertices>::iterator itV;
		for(itV = g_inputVertices.begin(); itV!=g_inputVertices.end(); ++itV){
			vertices.push_back((long long)(*itV).srcId);
			vertices.push_back((long long)(*itV).dstId);
		}

		vertices.sort();
		vertices.unique();

		srand (time(NULL));

		std::list<long long>::iterator itLi;
		for(itLi = vertices.begin(); itLi!=vertices.end(); ++itLi){

			//the label between 1 ~ 200
			long long label = rand() % 200 + 1;

			SVertexArray temp = {};
			temp.vertexId = (*itLi);
			temp.label = label;
//			printf("	vertexId : %d,	label : %d", temp.vertexId , label);
			temp.flag = true;
			g_dataVertex.push_back(temp);
		}
		//finish datavertex




		int range = g_inputVertices.size()*(4/5);
		srand (time(NULL));
//		int num = rand() % range;
		int num = 100;

		STriplets temp = {};
		int i=0;

		std::vector<SInputVertices>::iterator it = g_inputVertices.begin();
		for(i=0; i<num; i++){
			it++;
		}


		int pattern = 20;

		if(g_dataVertex.size() <= pattern){
			printf("pattern size error\n");
			return;
		}

		temp.src.Id = (*it).srcId;
		temp.dst.Id = (*it).dstId;
		g_queryTriplets.push_back(temp);

		std::vector<long long> verticesQuery;

		verticesQuery.push_back(temp.src.Id);
//		verticesQuery.push_back(temp.dst.Id);
		i=0;

		do{

			 long long currentId = verticesQuery[i];
			 i++;

			 for(it = g_inputVertices.begin(); it!= g_inputVertices.end(); ++it){

				 if(currentId == (*it).srcId || currentId == (*it).dstId){

					temp.src.Id = (*it).srcId;
					temp.dst.Id = (*it).dstId;

					if( verticesQuery.size() < pattern){
							g_queryTriplets.push_back(temp);

							verticesQuery = UniqueVec(verticesQuery, temp.src.Id);
							verticesQuery = UniqueVec(verticesQuery, temp.dst.Id);
					 }
					 else{
					   break;
					 }
				 }
			 }


		}while(verticesQuery.size() < pattern);


//		verticesQuery.unique();

		std::vector<long long>::iterator itLong;

		SVertexArray query;
		std::vector<SVertexArray>::iterator itData;
		for(itLong = verticesQuery.begin(); itLong != verticesQuery.end();itLong++){

			for(itData = g_dataVertex.begin(); itData !=g_dataVertex.end();itData++){

				if( (*itLong) == (*itData).vertexId){

					query.label = (*itData).label;
					query.vertexId = (*itLong);
					query.flag = true;
					g_queryVertex.push_back(query);
//					printf("id : %d\n", query.vertexId);
				}

			}

		}


		std::vector<SVertexArray>::iterator itVertex = g_queryVertex.begin();
		std::map<long long, std::vector<long long> >::iterator itM;

		long long verLabel = (*itVertex).label;
		long long verId = (*itVertex).vertexId;

		std::vector<long long> tempV;
		tempV.push_back(verId);
		g_queryCollect.insert( std::pair<long long, std::vector<long long> >(verLabel, tempV) );
		std::vector<long long> tempVer;
		static bool insert = false;

		for(itVertex = g_queryVertex.begin()+1; itVertex!= g_queryVertex.end(); ++itVertex){
			verLabel = (*itVertex).label;
			verId = (*itVertex).vertexId;
//			printf("id : %d ,  label : %d	\n", verId, verLabel);

			for(itM = g_queryCollect.begin(); itM!=g_queryCollect.end(); ++itM){

				if( itM->first == verLabel){
//					printf("push id : %d 	\n", verId);
					g_queryCollect[verLabel].push_back(verId);
					insert = true;
					break;
				}

			}

			if(!insert){
//				printf("insert id : %d 	\n", verId);
				tempVer.clear();
				tempVer.push_back(verId);
				g_queryCollect.insert( std::pair<long long, std::vector<long long> >(verLabel, tempVer) );
			}
			insert = false;
		}


//		std::vector<long long>::iterator itA;
//		for(itM = g_queryCollect.begin(); itM!=g_queryCollect.end(); ++itM){
//
//			printf("key: %d  ;", itM->first);
//			printf( " value : ");
//			for(itA = itM->second.begin(); itA != itM->second.end(); ++itA){
//				printf( " %d ,", *itA);
//			}
//			printf( "\n");
//		}


		check_dataVertex = g_dataVertex;

//		std::cout << "g_dataVertex size:  " << g_dataVertex.size()  << std::endl;
//		std::cout << "g_queryTriplets size:  " << g_queryTriplets.size()  << std::endl;
//		std::cout << "g_queryVertex size:  " << g_queryVertex.size()  << std::endl;
//		std::cout << "g_queryCollect size:  " << g_queryCollect.size()  << std::endl;

//		queryTriplets = g_queryTriplets;
//		queryVertex = g_queryVertex;
//		dataVertex = g_dataVertex;
//		queryCollect = g_queryCollect;
		g_validVertices = g_inputVertices;
	}


	template <class ForwardIterator, class T>
	  ForwardIterator remove (ForwardIterator first, ForwardIterator last, const T& val)
	{
	  ForwardIterator result = first;
	  while (first!=last) {
	    if (!(   (*first).srcId == val   ||    (*first).dstId == val  ) ) {
	      *result = *first;
	      ++result;
	    }
	    ++first;
	  }
	  return result;
	}


	void ResetValid(){

		std::vector<SInputVertices>::iterator it;
		std::vector<SVertexArray>::iterator itVer;

		g_validVertices = g_inputVertices;
		for(itVer = check_dataVertex.begin(); itVer!= check_dataVertex.end(); ++itVer){

			if(!((*itVer).flag)){

				for(it =g_inputVertices.begin(); it!=g_inputVertices.end();++it ){

					if(  (*it).srcId == (*itVer).vertexId  ){

						g_validVertices.erase(remove(g_validVertices.begin(), g_validVertices.end(), (*it).srcId), g_validVertices.end());

					}

					if( (*it).dstId == (*itVer).vertexId){
						g_validVertices.erase(remove(g_validVertices.begin(), g_validVertices.end(), (*it).dstId), g_validVertices.end());

					}
				}
			}


		}

	}


	void writeToMemGraph(K &src, int srcLoc, K &dst, int dstLoc) {
		//std::cout << "myComputeRank =" << myComputeRank << " test output: " << dataLocation[src] << std::endl;
		int myID = myComputeRank;

		if (data.size() == 0) {
			minVertex = src;
			maxVertex = src;
		}
		if (src > maxVertex) {
			maxVertex = src;
		} else if (src < minVertex) {
			minVertex = src;
		}
		if (dst > maxVertex) {
			maxVertex = dst;
		} else if (dst < minVertex) {
			minVertex = dst;
		}

		SInputVertices tem;
		tem.dstId = dst.getValue();
		tem.srcId = src.getValue();

		g_inputVertices.push_back(tem);


		/*	if(myID == 8){
		 std::cout << src.toString() << " " << srcLoc << " " << dst.toString() << " " << dstLoc << std::endl;
		 }*/
		if (srcLoc == myID) {
			//if (((mLong) src).getValue() == 643692) {
			//	cout << ((mLong) src).getValue() << endl;
			//}
			if (!vertexExists(src)) {
				int location = data.size();
				dataLocation[src] = location;
				mObject<K, V1, M> * vertexObj = new mObject<K, V1, M>(src);
				if (storageType == InOutNbrStore
						|| storageType == OutNbrStore) {
					vertexObj->addOutEdge(dst);
					countEdges++;
				}
				data.push_back(vertexObj);
			} else if (storageType == InOutNbrStore
					|| storageType == OutNbrStore) {
				mObject<K, V1, M> * vertexObj = this->getVertexObjByKey(src);
				vertexObj->addOutEdge(dst);
				countEdges++;
			}


		} /*else if (srcLoc != myID && dstLoc == myID
		 && (storageType == InOutNbrStore || storageType == OutNbrStore)) {
		 if (!vertexMiniExists(src)) {
		 int location = nbrData.size();
		 nbrDataLocation[src] = location;
		 mObjectMini<K, V1> * miniVertexObj = new mObjectMini<K, V1>(
		 src);

		 miniVertexObj->addOutEdge(dst);

		 nbrData.push_back(miniVertexObj);
		 } else {
		 int location = nbrDataLocation[src];
		 mObjectMini<K, V1> * miniVertex = nbrData[location];
		 miniVertex->addOutEdge(dst);
		 }
		 }*/

		if (dstLoc == myID) {
//			if (((mLong) dst).getValue() == 643692) {
//				cout << ((mLong) dst).getValue() << endl;
//			}

			if (!vertexExists(dst)) {
				int location = data.size();
				dataLocation[dst] = location;
				mObject<K, V1, M> * vertexObj = new mObject<K, V1, M>(dst);
				if (storageType == InOutNbrStore || storageType == InNbrStore) {
					vertexObj->addInEdge(src);
					countEdges++;
				}
				data.push_back(vertexObj);
			} else if (storageType == InOutNbrStore
					|| storageType == InNbrStore) {
				mObject<K, V1, M> * vertexObj = this->getVertexObjByKey(dst);
				vertexObj->addInEdge(src);
				countEdges++;
			}
		} /*else if (dstLoc != myID && srcLoc == myID
		 && (storageType == InOutNbrStore || storageType == InNbrStore)) {
		 if (!vertexMiniExists(dst)) {
		 int location = nbrData.size();
		 nbrDataLocation[dst] = location;
		 mObjectMini<K, V1> * miniVertexObj = new mObjectMini<K, V1>(
		 dst);
		 miniVertexObj->addInEdge(src);
		 nbrData.push_back(miniVertexObj);
		 } else {
		 int location = nbrDataLocation[dst];
		 mObjectMini<K, V1> * miniVertex = nbrData[location];
		 miniVertex->addInEdge(src);
		 }*/
		//}

	}

	bool vertexExists(const K &src) {
		if (dataLocation.find(src) == dataLocation.end()) {
			return false;
		} else
			return true;
	}
	bool vertexMiniExists(K &src) {
		/*if (nbrDataLocation.find(src) == nbrDataLocation.end()) {
		 return false;
		 } else*/
		return true;
	}

	mObject<K, V1, M> * getVertexObjByKey(K &vertex) {
		int location = dataLocation[vertex];
		return data[location];
	}
	mObject<K, V1, M> * getVertexObjByPos(int pos) {
		return data[pos];
	}
	void setVertexObjByPos(int pos, mObject<K, V1, M> * vertex) {
		data[pos] = vertex;
	}
	int getVertexIndex(const K &vertex) {
		return dataLocation[vertex];
	}

	int vertexSetSize() {
		return data.size();
	}
	bool appendMessage(const K &dst, M &message, int curSS) {
		if (vertexExists(dst)) {
			boost::mutex::scoped_lock insert_lock = boost::mutex::scoped_lock(
					dataAccessLock);
			//this->insert_message);
			int location = dataLocation.find(dst)->second;
			/*	if (getAvaliableSystemMemoryPercent < 0.051) {
			 ((mObject<K, V1, M>*) data[location])->appendMessage(message,
			 curSS);
			 } else {

			 */
			((mObject<K, V1, M>*) data[location])->appendMessage(message,
					curSS);
			//}

			insert_lock.unlock();
			return true;
		} else {
			/*std::cout << "PE" << myComputeRank
			 << " -------- rabina ma3ak --------: I don't have vertex "
			 << ((mLong) dst).getValue() << std::endl;*/
			return false;
		}
	}
	void appendLocalMessage(const K &dst, M &message, int curSS) {
		boost::mutex::scoped_lock insert_lock = boost::mutex::scoped_lock(
				dataAccessLock);
		//this->insert_message);
		int location = dataLocation.find(dst)->second;
		((mObject<K, V1, M>*) data[location])->appendLocalMessage(message,
				curSS);
		insert_lock.unlock();
	}

	void closeFile() {
		myDataConnector->closeTheFile();
	}
	~dataManager() {
		for (int i = 0; i < data.size(); i++) {
			delete (data[i]);
		}
		for (int i = 0; i < nbrData.size(); i++) {
			//delete (nbrData[i]);
		}
		delete (myDataConnector);
	}

	void swapVertexBuffes(K &v1, K &v2) {
		/*boost::mutex::scoped_lock dataLock = boost::mutex::scoped_lock(
		 this->dataAccessLock);*/

		//std::vector<mObject<K, V> *> data;
		//std::map<K, int> dataLocation;
		int locV1 = dataLocation[v1];
		int locV2 = dataLocation[v2];

		mObject<K, V1, M> * objV1 = data[locV1];
		mObject<K, V1, M> * objV2 = data[locV2];

		data[locV1] = objV2;
		data[locV2] = objV1;
		dataLocation[v1] = locV2;
		dataLocation[v2] = locV1;

		/*dataLock.unlock();*/
	}
	void removeMovedVertexs(std::set<K> * migrateFail) {
		for (int i = 0; i < softDynamicVertexSend.size(); i++) {
			K v1 = softDynamicVertexSend[i]->getVertexID();
			if (migrateFail->find(v1) == migrateFail->end()) {
				deleteVertex(v1);
			}
		}
	}
	void deleteVertex(K &v1) {
		boost::mutex::scoped_lock dataLock = boost::mutex::scoped_lock(
				this->dataAccessLock);

		K v2 =
				this->getVertexObjByPos(this->vertexSetSize() - 1)->getVertexID();
		swapVertexBuffes(v1, v2);
		dataLocation.erase(v1);
		data.pop_back();

		dataLock.unlock();
	}
	void lockDataManager() {
		this->dataAccessLock.lock();
	}
	void unlockDataManager() {
		this->dataAccessLock.unlock();
	}
	/*void copyVertexToSoftDynamicContainer(K &v1, int dst) {
		mObject<K, V1, M> * vertexPtr = this->getVertexObjByKey(v1);
		copyVertexToSoftDynamicContainer(vertexPtr, dst);
	}*/
	void copyVertexToSoftDynamicContainer(mObject<K, V1, M> * vertex, int dst) {
		softDynamicVertexSend.push_back(vertex);
		softDynamicVertexSendDst[vertex->getVertexID()] = dst;
	}
	void addNewVertex(K &vertex) {
		boost::mutex::scoped_lock dataLock = boost::mutex::scoped_lock(
				this->dataAccessLock);

		mObject<K, V1, M> * newV = new mObject<K, V1, M>(vertex);
		newV->setMigrationMark();
		//newV->voteToHalt();
		int position = data.size();
		data.push_back(newV);
		dataLocation[vertex] = position;

		dataLock.unlock();
	}
	void addNewVertex(mObject<K, V1, M> * vertexObj) {
		//cout << "PE"<<this->myID<< " lock add new vertex" << std::endl;
		this->lockDataManager();

		//Adding vertexObj to the memory
		int position = data.size();
		data.push_back(vertexObj);
		dataLocation[vertexObj->getVertexID()] = position;
		registerVertexNBR(vertexObj);

		this->unlockDataManager();
		//cout << "PE"<<this->myID<< " unlock new vertex" << std::endl;
	}
	void registerVertexNBR(mObject<K, V1, M> * vertexObj) {
		//registering for send to all NBRS
		/*for (int i = 0; i < vertexObj->getInEdgeCount(); i++) {
		 K vertexID = vertexObj->getInEdgeID(i);
		 if (!vertexExists(vertexID)) {
		 if (!vertexMiniExists(vertexID)) {
		 int location = nbrData.size();
		 nbrDataLocation[vertexID] = location;
		 mObjectMini<K, V1> * miniVertexObj = new mObjectMini<K, V1>(
		 vertexID);
		 nbrData.push_back(miniVertexObj);
		 miniVertexObj->addOutEdge(vertexObj->getVertexID());
		 } else {
		 int location = nbrDataLocation[vertexID];
		 mObjectMini<K, V1> * miniVertex = nbrData[location];
		 miniVertex->addOutEdge(vertexObj->getVertexID());
		 }
		 }
		 }
		 for (int i = 0; i < vertexObj->getOutEdgeCount(); i++) {
		 K vertexID = vertexObj->getOutEdgeID(i);
		 if (!vertexExists(vertexID)) {
		 if (!vertexMiniExists(vertexID)) {
		 int location = nbrData.size();
		 nbrDataLocation[vertexID] = location;
		 mObjectMini<K, V1> * miniVertexObj = new mObjectMini<K, V1>(
		 vertexID);
		 nbrData.push_back(miniVertexObj);
		 miniVertexObj->addInEdge(vertexObj->getVertexID());
		 } else {
		 int location = nbrDataLocation[vertexID];
		 mObjectMini<K, V1> * miniVertex = nbrData[location];
		 miniVertex->addInEdge(vertexObj->getVertexID());
		 }
		 }
		 }*/
	}
	mObject<K, V1, M> * getSoftDynamicVertexSendObjByPos(int pos) {
		return softDynamicVertexSend[pos];
	}
	int getSoftDynamicVertexSendObjectByKey(const K &vertex) {
		return softDynamicVertexSend[vertex];
	}

	int getSoftDynamicVertexSendSize() {
		return softDynamicVertexSend.size();
	}
	int getSoftDynamicVertexSendDst(const K &vertex) {
		return softDynamicVertexSendDst[vertex];
	}
	void derigsterAllNBRSoftDynamic(std::set<K> * migrateFail) {
		for (int i = 0; i < softDynamicVertexSend.size(); i++) {
			mObject<K, V1, M> * vertexObj = softDynamicVertexSend[i];
			if (migrateFail->find(vertexObj->getVertexID())
					== migrateFail->end()) {
				derigsterNBR(vertexObj);
			}
		}
	}
	void derigsterNBR(K &myVertex) {
		/*mObject<K, V1, M> * vertexObj = getVertexObjByKey(myVertex);
		 derigsterNBR(vertexObj);*/
	}
	void derigsterOutEdgeNBR(K &src, K &dst) {
		/*if (vertexMiniExists(src)) {
		 mObjectMini<K, V1> * miniPtr = nbrData[nbrDataLocation[src]];
		 miniPtr->deleteOutEdge(dst);
		 } else {
		 std::cout << " src " << src.toString()
		 << " does not exists in vertexMiniExists to delete "
		 << dst.toString() << std::endl;
		 }
		 */
	}
	void derigsterInEdgeNBR(K &src, K &dst) {
		/*if (vertexMiniExists(src)) {
		 int location = nbrDataLocation[src];
		 mObjectMini<K, V1> * miniPtr = nbrData[location];
		 miniPtr->deleteInEdge(dst);

		 } else {
		 std::cout << " src " << src.toString()
		 << " does not exists in vertexMiniExists to delete "
		 << dst.toString() << std::endl;
		 }*/
	}
	void rigsterOutEdgeNBR(K &src, K &dst) {
		/*if (vertexMiniExists(src)) {
		 mObjectMini<K, V1> * miniPtr = nbrData[nbrDataLocation[src]];
		 miniPtr->addOutEdge(dst);
		 } else {
		 mObjectMini<K, V1> * miniPtr = new mObjectMini<K, V1>(src);
		 nbrDataLocation[src] = nbrData.size();
		 nbrData.push_back(miniPtr);
		 miniPtr->addOutEdge(dst);
		 }*/
	}
	void rigsterInEdgeNBR(K &src, K &dst) {
		/*if (vertexMiniExists(src)) {
		 mObjectMini<K, V1> * miniPtr = nbrData[nbrDataLocation[src]];
		 miniPtr->addInEdge(dst);
		 } else {
		 mObjectMini<K, V1> * miniPtr = new mObjectMini<K, V1>(src);
		 nbrDataLocation[src] = nbrData.size();
		 nbrData.push_back(miniPtr);
		 miniPtr->addInEdge(dst);
		 }*/
	}
	void derigsterNBR(mObject<K, V1, M> * vertexObj) {
		/*K myID = vertexObj->getVertexID();
		 mObjectMini<K, V1> * miniPtr;
		 for (int j = 0; j < vertexObj->getInEdgeCount(); j++) {
		 K vertex = vertexObj->getInEdgeID(j);
		 miniPtr = nbrData[nbrDataLocation[vertex]];
		 miniPtr->deleteOutEdge(myID);

		 }
		 for (int j = 0; j < vertexObj->getOutEdgeCount(); j++) {
		 K vertex = vertexObj->getOutEdgeID(j);
		 miniPtr = nbrData[nbrDataLocation[vertex]];
		 miniPtr->deleteInEdge(myID);

		 }*/
	}
	void clearSoftDynamicVertexSend() {
		softDynamicVertexSend.clear();
		softDynamicVertexSendDst.clear();
	}
	void resetIterator() {
		stolenIndex = 0;
		itIndex = 0;
	}
	mObject<K, V1, M> * iterateData(bool remember) {
		//std::cout << "iterateData lock" << std::endl;

//		printf("start dataAccessLock : %d, %d\n", itIndex, vertexSetSize());

		boost::mutex::scoped_lock dataLock = boost::mutex::scoped_lock(
				this->dataAccessLock);

//		printf("dataAccessLock : %d, %d\n", itIndex, vertexSetSize());

		mObject<K, V1, M> * vertexObj = 0;
		if (itIndex == vertexSetSize()) {
			vertexObj = 0;
		} else {
			vertexObj = getVertexObjByPos(itIndex);
			if (remember) {
				insertStolen(vertexObj->getVertexID());
			}

			itIndex++;
		}
//		std::cout << "iterateData unlock" << std::endl;
		dataLock.unlock();
//		std::cout << "iterateData unlock finish" << std::endl;
		return vertexObj;

	}
	int stoleVertexCnt;
	void waitForSteel() {
		this->stolenVertexContinueLock.lock();
		this->stolenVertexContinueLock.unlock();
	}
	void insertStolen(const K &vertex) {
		//std::cout << "insertStolen lock" << std::endl;
		boost::mutex::scoped_lock stoleLock = boost::mutex::scoped_lock(
				this->stolenVertexLock);
		if (stoleVertexCnt == 0) {
			this->stolenVertexContinueLock.lock();
		}
		stoleVertexCnt++;
		//stolen.push_back(vertex);
		//std::cout << "insertStolen unlock" << std::endl;
		stoleLock.unlock();

	}
	K * getStolen() {
		//std::cout << "getStolen lock" << std::endl;
		boost::mutex::scoped_lock stoleLock = boost::mutex::scoped_lock(
				this->stolenVertexLock);
		K * vertex = (K*) calloc(1, sizeof(K));
		if (stolen.size() != 0) {
			(*vertex) = stolen.front();
			stolen.pop_front();
		} else {
			vertex = 0;
		}
		//std::cout << "getStolen unlock" << std::endl;
		stoleLock.unlock();
		return vertex;
	}
	bool deleteFromStolen(const K &vertex) {
		//std::cout << "deleteFromStolen lock" << std::endl;
		boost::mutex::scoped_lock stoleLock = boost::mutex::scoped_lock(
				this->stolenVertexLock);

		/*bool found = false;
		 typename std::list<K>::iterator itt;
		 for (itt = stolen.begin(); itt != stolen.end(); itt++) {
		 if (*itt == vertex) {
		 stolen.remove(vertex);
		 found = true;
		 break;
		 }
		 }*/
		//std::cout << "deleteFromStolen unlock" << std::endl;
		stoleVertexCnt--;
		if (stoleVertexCnt == 0) {
			this->stolenVertexContinueLock.unlock();
		}
		stoleLock.unlock();
		return true;
		//return found;
	}
	void applyUpdate(mObject<K, V1, M> * verObj) {
		this->lockDataManager();
		int location = getVertexIndex(verObj->getVertexID());
		mObject<K, V1, M> * origVerObj = getVertexObjByPos(location);

		verObj->swapMessageQueue(origVerObj);
		verObj->copyNbrs(origVerObj);

		data[location] = verObj;

		//origVerObj->setVertexValue(verObj->getVertexValue());
		//origVerObj->setSSResTime(verObj->getSSResTime());
		//origVerObj->startNewSS();
		//if (verObj->isHalted()) {
		//	origVerObj->voteToHalt();
		//}

		delete origVerObj;
		this->unlockDataManager();
	}
	void appendIncomeQueueNbr(K &src, M &message, DATA_CMDS inOut, int ssCnt) {
		if (vertexMiniExists(src)) {
			int location = nbrDataLocation[src];
			mObjectMini<K, V1> * miniVertex = nbrData[location];
			if (inOut == InNbrs) {
				for (int i = 0; i < miniVertex->getInEdgeCount(); i++) {
					this->appendLocalMessage(miniVertex->getInEdgeID(i),
							message, ssCnt);
				}
			} else if (inOut == OutNbrs) {
				for (int i = 0; i < miniVertex->getOutEdgeCount(); i++) {
					this->appendLocalMessage(miniVertex->getOutEdgeID(i),
							message, ssCnt);
				}
			}
		} else if (vertexExists(src)) {
			int location = dataLocation[src];
			mObject<K, V1, M> * vertex = data[location];
			if (inOut == InNbrs) {
				for (int i = 0; i < vertex->getInEdgeCount(); i++) {
					if (vertexExists(vertex->getInEdgeID(i))) {
						this->appendLocalMessage(vertex->getInEdgeID(i),
								message, ssCnt);
					}
				}
			} else if (inOut == OutNbrs) {
				for (int i = 0; i < vertex->getOutEdgeCount(); i++) {
					if (vertexExists(vertex->getOutEdgeID(i))) {
						this->appendLocalMessage(vertex->getOutEdgeID(i),
								message, ssCnt);
					}
				}
			}
		}
	}
	std::vector<edge<K, V1> *> * getInEdges(K &vertex) {
		return getVertexObjByKey(vertex)->getInEdges();
	}
	std::vector<edge<K, V1> *> * getOutEdges(K &vertex) {
		return getVertexObjByKey(vertex)->getOutEdges();
	}
	void writeToDisk(char * fileName) {
		std::cout << "writing to disk! " << fileName << std::endl;
		hdfsGraphWriter writer(fileName, -1);
		mObject<K, V1, M> * tmp;
		int blockSize = writer.getConfBlockSize();
		char * blockData = (char*) calloc(blockSize, sizeof(char));
		int blockDataPtr = 0;
		for (int i = 0; i < data.size(); i++) {
			tmp = data[i];
			string outData = tmp->toString();
			int strLen = outData.length() + 1;
			if (blockSize > (blockDataPtr + strLen)) {
				strcat(blockData, outData.c_str());
				strcat(blockData, "\n");
				blockDataPtr = blockDataPtr + strLen;
			} else {
				writer.writeBlock(blockDataPtr, blockData);
				blockData[0] = 0;
				blockDataPtr = 0;
			}
		}
		writer.closeTheFile();
		free(blockData);
	}

//Methods

}
;

#endif /* DATAMANAGER_H_ */
