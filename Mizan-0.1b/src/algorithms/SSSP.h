/*
 * SSSP.h
 *
 *  Created on: Nov 13, 2015
 *      Author: Chao Chen
 */

#ifndef SSSP_H_
#define SSSP_H_

#include "../IsuperStep.h"
#include "../Icombiner.h"
#include "../dataManager/dataStructures/data/mLong.h"
#include "../dataManager/dataStructures/data/mGPMType.h"
#include "../dataManager/dataStructures/data/mLongArray.h"
#include "../dataManager/dataStructures/data/mDoubleArray.h"
#include <iostream>
#include <string>
#include <stdio.h>
#include <vector>
#include <list>
#include <time.h>
#include <stdlib.h>
#include "../dataManager/dataManager.h"

#define MAX_DISTANCE 10000000

class SSSPCombiner: public Icombiner<mLong, mLong, mLong, mLong> {

	void combineMessages(mLong dst, messageIterator<mLong> * messages,
			messageManager<mLong, mLong, mLong, mLong> * mManager) {

		long long maxValue = MAX_DISTANCE;
		while (messages->hasNext()) {
			long long value = messages->getNext().getValue();
			if(maxValue>=value){
				maxValue = value;
			}
		}
		mLong newVal(maxValue);
		mManager->sendMessage(dst, newVal);
	}
};


class SSSP: public IsuperStep<mLong, mLong, mLong, mLong> {
private:
	long long distance;
	int maxSuperStep;

	std::map<long long, long long > ecc;

	std::vector<STriplets> queryTriplets;
	std::vector<SVertexArray> queryVertex;

	long long verId;
public:

	SSSP(int id) {
		distance = MAX_DISTANCE;
		maxSuperStep = MAX_DISTANCE;
		verId = id;
//		queryTriplets = tri;
//		queryVertex = vertices;
//		onlyForTest();
	}

	void Reset(long long id){
		verId = id;
		ecc.clear();
	}

	void initialize(userVertexObject<mLong, mLong, mLong, mLong> * data) {
		long long id = data->getVertexID().getValue();

//		queryTriplets = g_queryTriplets;
//		queryVertex = g_queryVertex;
//		printf("SSSp initialize %d\n", verId);

//		onlyForTest();
		queryTriplets = g_queryTriplets;
		queryVertex = g_queryVertex;

		mLong newVal;
		if(id != verId){
			newVal.setValue(MAX_DISTANCE);
		}
		else{
			newVal.setValue(0);
		}
		data->setVertexValue(newVal);
	}

	void compute(messageIterator<mLong> * messages,
			userVertexObject<mLong, mLong, mLong, mLong> * data,
			messageManager<mLong, mLong, mLong, mLong> * comm) {

		if(messages== NULL || data== NULL || comm== NULL){
			std::cout<< "NULL error!" << std::endl;
			return;
		}

		long long vertexId = data->getVertexID().getValue();

		bool sendMessage = false;

//		printf("%d, %d\n", queryVertex.size(), queryTriplets.size());

//		std::cout<< "compute vertexId" << vertexId << " value :" <<data->getVertexValue().getValue() << std::endl;

		if(data->getCurrentSS() <= 1){

//			printf("id : %d\n", vertexId);
			bool valid = false;
			std::vector<SVertexArray>::iterator it;
			for(it = queryVertex.begin(); it!=queryVertex.end(); ++it){
				if(vertexId == (*it).vertexId){
					valid = true;
					break;
				}
			}

			if(valid){
				sendMessage = true;
			}
			else{
				sendMessage = false;
			}
			data->setVertexValue(data->getVertexValue());
		}

		if (data->getCurrentSS() > 1) {

			long long curVal=data->getVertexValue().getValue();
			while (messages->hasNext()) {
				long long value = messages->getNext().getValue();
				if(curVal>value){
					curVal = value;
					sendMessage = true;
					data->setVertexValue(mLong(curVal));
				}
			}
		}


		if (data->getCurrentSS() <= MAX_DISTANCE && sendMessage) {

//			std::cout<< "compute sendMessage" << std::endl;

			std::vector<STriplets>::iterator itTri;
			std::vector<STriplets> tempTri = queryTriplets;
			std::vector<long long> target;
			std::vector<long long>::iterator targetIt;

			for(itTri = tempTri.begin(); itTri !=tempTri.end(); itTri++){

				if( ((*itTri).src.Id== vertexId) ){
					target.push_back((*itTri).dst.Id);
				}

				if( ((*itTri).dst.Id == vertexId)){
					target.push_back((*itTri).src.Id);
				}
			}

			for (targetIt = target.begin(); targetIt !=target.end(); targetIt++) {
				mLong target(*targetIt);
//				printf("id : %d, send %d, %d\n", vertexId, *targetIt,  (data->getVertexValue().getValue()+1));
				comm->sendMessage(target, (data->getVertexValue().getValue()+1) );
			}
			ecc[vertexId] = data->getVertexValue().getValue();

		}else data->voteToHalt();

	}



	void ShowEcc(){

		std::map<long long, long long >::iterator itMap;

		for(itMap = ecc.begin(); itMap!=ecc.end(); itMap++){
			printf("key: %d , value : %d\n", itMap->first, itMap->second);
		}
	}

	long long GetMinEcc(){
		long long value = 0;
		std::map<long long, long long >::iterator itMap;
		for(itMap = ecc.begin(); itMap!=ecc.end(); itMap++){
			if(itMap->second == MAX_DISTANCE)
				continue;

			if(itMap->second > value){
				value = itMap->second;
			}
		}

		return value;
	}

};


#endif /* SSSP_H_ */
