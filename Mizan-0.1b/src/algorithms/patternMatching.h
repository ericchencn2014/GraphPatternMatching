/*
 * patternMatching.h
 *
 *  Created on: Oct 11, 2015
 *      Author: Chao Chen
 */

#ifndef PATTERN_MATCHING_H_
#define PATTERN_MATCHING_H_

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

class PatternMatchingCombiner: public Icombiner<mLong, mGPMType, mGPMType, mLong> {

	void combineMessages(mLong dst, messageIterator<mGPMType> * messages,
			messageManager<mLong, mGPMType, mGPMType, mLong> * mManager) {



		std::vector<SVertexProperties> result;

		while (messages->hasNext()) {
			std::vector<SVertexProperties> pTemp= messages->getNext().GetProperties();
			if(pTemp.size()>0){
				result.push_back(pTemp[0]);
			}
		}


		mGPMType newVal(result);

//		printf( " dst : %d , len %d\n", dst.getValue(), result.size());

		mManager->sendMessage(dst, newVal);
	}
};

long long check[100]={11,6,8,10,15,23,29,35,36,56,86,171,214,230,255,259,282,285,298,
		299,306,308,310,317,319,372,407,432,506,559,575,579,587,600,637,667,706,707,
		710,722,733,761,763,765,805,810,817,826,836,838,844,849,853,857,893,904,918,
		922,932,941,946,959,960,971,982,993,999,1000,1006,1031,1034,1049,1055,1062,
		1075,1097,1103,1127,1128,1137,1151,1154,1160,1164,1165,1167,1185,1186,1199,1201,
		1211,1222,1230,1243,1248,1253,1261,1279,1285,1296 };



class PatternMatching: public IsuperStep<mLong, mGPMType, mGPMType, mLong> {
private:

	std::vector<STriplets> queryTriplets;
	std::vector<SVertexArray> queryVertex;
	std::vector<SVertexArray> dataVertex;

	std::map<long long, std::vector<long long> > queryCollect;


	PatternMatching(int maxSS) {

	}



	void initialize(userVertexObject<mLong, mGPMType, mGPMType, mLong> * data) {
		long long id = data->getVertexID().getValue();


		SVertexProperties newValue={};
		newValue.m_flag = false;

		queryTriplets = g_queryTriplets;
		queryVertex = g_queryVertex;
		dataVertex = g_dataVertex;
		queryCollect = g_queryCollect;

//		std::cout<< "dataVertex size: " << dataVertex.size() << std::endl;

		std::vector<SVertexArray>::iterator it;
		for(it = dataVertex.begin(); it!=dataVertex.end(); it++){

			if((*it).vertexId == id){
				newValue.m_label = (*it).label;
			}
		}

		newValue.vertexId = id;

		mGPMType valided(newValue);
		data->setVertexValue(valided);

	}

	void compute(messageIterator<mGPMType> * messages,
			userVertexObject<mLong, mGPMType, mGPMType, mLong> * data,
			messageManager<mLong, mGPMType, mGPMType, mLong> * comm) {

		if(messages== NULL || data== NULL || comm== NULL){
			std::cout<< "NULL error!" << std::endl;
			return;
		}

		long long vertexId = data->getVertexValue().GetId();

		long long curLabel = data->getVertexValue().GetLabel();

		bool sendMessage = false;

		mGPMType outVal;

		if(data->getCurrentSS() == 1){


//			std::cout<< "compute vertex id: " << vertexId << "  curLabel: " << curLabel << std::endl;

//			if(queryVertex.size()==0){
//				onlyForTest();
//				ExtractMap();
//				if(ExtractQuery() == false){
//					printf("extract_query error\n");
//					return;
////				}
//			}

			outVal = InitVertex(curLabel, vertexId);

			data->setVertexValue(outVal);
			sendMessage = true;
		}

		if(data->getCurrentSS() > 1){

//			printf("SS : %d, vertex id: %d,  flag : %s, match size : %d \n", data->getCurrentSS(),
//					vertexId, data->getVertexValue().GetFlag()?"true":"false", data->getVertexValue().GetMatchSet().size());

			if(data->getVertexValue().GetFlag() == true &&  data->getVertexValue().GetMatchSet().size()>0){

				std::vector<long long> recFromChi;
				std::vector<long long> recFromPar;

				if( GetVector(recFromChi, recFromPar, data, messages) == true ){

//					printf("vertex id: %d,  recFromChi len: %d,  recFromPar len: %d\n", vertexId, recFromChi.size(), recFromPar.size());


//					std::cout<< " recFromChi set : ";
//					std::vector<long long >::iterator itV;
//
//					for(itV = recFromChi.begin(); itV!=recFromChi.end();itV++){
//						std :: cout << (*itV)<< "   ";
//					}
//
//					std::cout<< std::endl;
//
//					std::cout<< " recFromPar set : ";
//
//					for(itV = recFromPar.begin(); itV!=recFromPar.end();itV++){
//						std :: cout << (*itV) << "   ";
//					}
//
//					std::cout<< std::endl;


					std::vector<long long> matchArray = data->getVertexValue().GetMatchSet();
					std::vector<STriplets> tri = data->getVertexValue().GetTriplets();

					std::vector<long long>::iterator itMatch;
					std::vector<long long> newArray;
					int i=0;
//					for(int i=0; i<matchArray.size(); i++){

					for(itMatch = matchArray.begin(); itMatch!=matchArray.end();++itMatch){
						bool bChange = false;

						bool result[3]={true,false,false};
						check_children_and_parents(result, vertexId, *itMatch, tri);

//						for(i=0; i<100; i++){
//							if(check[i] == vertexId){
//
//								printf("id : %d, match : %d, %s , %s , %s \n", vertexId, *itMatch, result[0]?"true":"false",  result[1]?"true":"false",result[2]?"true":"false");
//
//							}
//
//						}

						if(!result[0]){
							sendMessage = true;
							bChange = true;
//							matchArray.erase(matchArray.begin()+i);
						}
						else{
							bool bRChi = true;
							bool bRPar = true;

							if(result[1]){
								bRChi = QueryContainsAll(*itMatch, recFromChi, true);
							}

							if(result[2]){
								bRPar = QueryContainsAll(*itMatch, recFromPar, false);
							}


							if( !(bRChi && bRPar) ){
								sendMessage = true;
								bChange = true;
//								matchArray.erase(matchArray.begin()+i);
	//							printf("remove %d\n", matchArray[i]);
							}

						}

						if(!bChange){
							newArray.push_back((*itMatch));
						}

					}

					matchArray = newArray;

					if(matchArray.size()<=0){
//						printf("matchArray.size()<=0\n");

						outVal = data->getVertexValue();
						outVal.SetFlag(false);
						data->setVertexValue(outVal);
						sendMessage = true;
					}


					if(sendMessage){
						outVal = data->getVertexValue();
						outVal.SetMatchSet(matchArray);
						data->setVertexValue(outVal);
					}

				}

			}else{
				outVal = SendInvalidedMsg(data->getVertexValue().GetId());
				data->setVertexValue(outVal);
			}
		}


		if (data->getCurrentSS() <= maxSuperStep && sendMessage) {

			std::vector<STriplets>::iterator itTri;
			std::vector<STriplets> tempTri = data->getVertexValue().GetTriplets();
			std::vector<long long> target;
			std::vector<long long>::iterator targetIt;

			for(itTri = tempTri.begin(); itTri !=tempTri.end(); itTri++){

				if( ((*itTri).src.Id== vertexId)  && ((*itTri).dst.flag == true)){
					target.push_back((*itTri).dst.Id);
				}

				if( ((*itTri).dst.Id == vertexId) && ((*itTri).src.flag == true)){
					target.push_back((*itTri).src.Id);
				}
			}

			for (targetIt = target.begin(); targetIt !=target.end(); targetIt++) {

//				printf("vertexId : %d, target : %d\n", vertexId, *targetIt);
				mLong target(*targetIt);
				comm->sendMessage(target, outVal);
			}

//			for (int i = 0; i < data->getOutEdgeCount(); i++) {
//				comm->sendMessage(data->getOutEdgeID(i), outVal);
//			}


			bool flag = data->getVertexValue().GetFlag();
			if(!flag){

//				printf("flag false vertexId : %d\n", vertexId);

//				for(int i=0; i<100; i++){
//					if(candidate[i] == vertexId){
//						printf("%d remove\n", vertexId);
//					}
//				}

				std::vector<SVertexArray>::iterator it;
				for(it = check_dataVertex.begin(); it!=check_dataVertex.end(); it++){
					if((*it).vertexId == vertexId){
						(*it).flag = false;
					}
				}
			}


		}else data->voteToHalt();

	}

	void check_children_and_parents(bool result[3], long long vertexId, long long matchedId, std::vector<STriplets> dataTri){

		bool bChild = false;
		bool bParent = false;
		bool bResult = true;

		std::vector<STriplets>::iterator tri;

		for(tri = queryTriplets.begin(); tri!= queryTriplets.end() ; tri++){

			if( (*tri).src.Id == matchedId ){
				bChild = true;
			}

			if( (*tri).dst.Id == matchedId ){
				bParent = true;
			}

			if(bChild && bParent){
				break;
			}
		}

		bool bDataChild = false;
		bool bDataParent = false;
		for(tri = dataTri.begin(); tri!= dataTri.end() ; tri++){

			if( ((*tri).src.Id == vertexId)  && ((*tri).dst.flag)){
				bDataChild = true;
			}

			if( ((*tri).dst.Id == vertexId) && ((*tri).src.flag)){
				bDataParent = true;
			}

			if(bDataChild && bDataParent){
				break;
			}
		}

		if( (!bDataChild) && (!bDataParent)){
			bResult = false;
		}

		if( bChild &&  !bDataChild){
			bResult = false;
		}

		if( bParent &&  !bDataParent){
			bResult = false;
		}

		result[0] = bResult;
		result[1] = bChild;
		result[2] = bParent;
	}

	bool QueryContainsAll(long long id, std::vector<long long> array, bool relative){

		std::vector<long long>  temp;

		std::vector<STriplets>::iterator tri;

		if(relative){
			for(tri = queryTriplets.begin(); tri!= queryTriplets.end() ; tri++){
				if( (*tri).src.Id == id ){
					temp.push_back((*tri).dst.Id);
				}
			}
		}
		else{
			for(tri = queryTriplets.begin(); tri!= queryTriplets.end() ; tri++){
				if( (*tri).dst.Id == id ){
					temp.push_back((*tri).src.Id);
				}
			}
		}

		if(temp.size()>0 && array.size()<=0){
			return false;
		}

	    if(temp.size()<=0 || array.size()<=0){
	      return true;
	    }

	    for(int i=0; i< temp.size(); i++){

	    	if(std::find(array.begin(), array.end(), temp[i]) == array.end()){
	    		return false;
	    	}
	    }

	    return true;
	}

	bool GetVector(std::vector<long long> &chi, std::vector<long long> &par, userVertexObject<mLong, mGPMType, mGPMType, mLong> * data,
			messageIterator<mGPMType> * messages) {

		std::vector<SVertexProperties> collectMsg;

		std::vector<SVertexProperties>::iterator pIt;

		while (messages->hasNext() ) {
			std::vector<SVertexProperties> pTemp= messages->getNext().GetProperties();

			for(pIt = pTemp.begin(); pIt != pTemp.end(); pIt++){
				collectMsg.push_back(*pIt);
//				std::cout << "---recv message id:" << (*pIt).vertexId << "	match set len:"  << (*pIt).m_matchSet.size();
			}
//			std::cout << std::endl;
		}


		if(collectMsg.size()>0 ){

//			std::cout << "---recv collectMsg len : " <<  collectMsg.size() << std::endl;
			std::map<long long, std::vector<long long> > childrenSet = data->getVertexValue().GetChildrenMatch();
			std::map<long long, std::vector<long long> > parentsSet = data->getVertexValue().GetParentMatch();

			std::vector<SVertexProperties>::iterator msgIt;
			for(msgIt=collectMsg.begin(); msgIt!=collectMsg.end(); msgIt++){

				bool relative[2] = {false,false};

				GetRelatives( (*msgIt).vertexId , data->getVertexValue().GetTriplets(), relative);

//				printf("Id: %d,  %s,  %s\n", (*msgIt).vertexId, relative[0]?"true":"false", relative[1]?"true":"false");
//
//
//				std::cout<< " match set : ";
//				std::vector<long long >::iterator itV;
//
//				for(itV = (*msgIt).m_matchSet.begin(); itV!=(*msgIt).m_matchSet.end();itV++){
//					std :: cout << (*itV)<< "   ";
//				}

//				std::cout<< std::endl;


				if((*msgIt).m_flag == false ){
					UpdateStruct((*msgIt).vertexId, false, data );

					if( childrenSet.find( (*msgIt).vertexId ) != childrenSet.end()	 &&  relative[1] == true){
						childrenSet.erase( (*msgIt).vertexId );
					}

					if( parentsSet.find( (*msgIt).vertexId ) != parentsSet.end()	 &&  relative[0] == true){
						parentsSet.erase( (*msgIt).vertexId );
					}

				}
				else{

					if( relative[1] == true){
						if(childrenSet.find( (*msgIt).vertexId ) != childrenSet.end()){
							childrenSet.erase( (*msgIt).vertexId );
							childrenSet.insert( std::pair<long long, std::vector<long long> >((*msgIt).vertexId, (*msgIt).m_matchSet) );
						}
						else{
							childrenSet.insert( std::pair<long long, std::vector<long long> >((*msgIt).vertexId, (*msgIt).m_matchSet) );

						}
					}

					if( relative[0] == true){
						if(parentsSet.find( (*msgIt).vertexId ) != parentsSet.end()){
							parentsSet.erase( (*msgIt).vertexId );
							parentsSet.insert( std::pair<long long, std::vector<long long> >((*msgIt).vertexId, (*msgIt).m_matchSet) );
						}
						else{
							parentsSet.insert( std::pair<long long, std::vector<long long> >((*msgIt).vertexId, (*msgIt).m_matchSet) );

						}
					}

				}

			}


			mGPMType tem = data->getVertexValue();
			tem.SetChildrenMatch(childrenSet);
			tem.SetParentMatch(parentsSet);
			data->setVertexValue(tem);

			std::list<long long> listChild;
			std::list<long long> listParent;
			std::map<long long, std::vector<long long> >::iterator mapIt;
			std::vector<long long>::iterator vecIt;

			for(mapIt = childrenSet.begin(); mapIt != childrenSet.end(); mapIt++){
				for(vecIt = mapIt->second.begin(); vecIt != mapIt->second.end(); vecIt++){
					listChild.push_back( (*vecIt));
				}
			}
			listChild.unique();



			for(mapIt = parentsSet.begin(); mapIt != parentsSet.end(); mapIt++){
				for(vecIt = mapIt->second.begin(); vecIt != mapIt->second.end(); vecIt++){
					listParent.push_back( (*vecIt));
				}
			}
			listParent.unique();



			std::list<long long>::iterator itList;
			for(itList=listParent.begin(); itList!=listParent.end(); itList++){
				par.push_back( (*itList));
			}

			for(itList=listChild.begin(); itList!=listChild.end(); itList++){
				chi.push_back((*itList));
			}

		}

		if( par.size() > 0 || chi.size()>0){
			return true;
		}
		else{
			return false;
		}
	}

	void UpdateStruct( long long id, bool status, userVertexObject<mLong, mGPMType, mGPMType, mLong> * data ){
		std::vector<STriplets> tri = data->getVertexValue().GetTriplets();
		std::vector<STriplets>::iterator it;

		for(it = tri.begin(); it!=tri.end(); it++){

			if( (*it).src.Id == id  ){
				(*it).src.flag = status;
			}

			if( (*it).dst.Id == id){
				(*it).dst.flag = status;
			}
		}

		mGPMType tem = data->getVertexValue();
		tem.SetTriplets(tri);
		data->setVertexValue(tem);
	}

	//relative[0] represent parent, relative[1] represent children
	void GetRelatives(long long id, std::vector<STriplets> tri, bool relative[2]){
		std::vector<STriplets>::iterator it;
		for(it = tri.begin(); it!=tri.end(); it++){

			if( ((*it).src.Id) == id  && ((*it).src.flag)==true){
				relative[0] = true;
			}

			if( ((*it).dst.Id) == id  && ((*it).dst.flag)==true){
				relative[1] = true;
			}
		}

	}

	void onlyForTest(){

		STriplets tempTri = {};
		tempTri.src.Id = 1;
		tempTri.src.flag = true;
		tempTri.src.label = 1;
		tempTri.dst.Id = 2;
		tempTri.dst.flag = true;
		tempTri.dst.label = 2;
		queryTriplets.push_back(tempTri);

		tempTri.src.Id = 2;
		tempTri.src.flag = true;
		tempTri.src.label = 2;
		tempTri.dst.Id = 1;
		tempTri.dst.flag = true;
		tempTri.dst.label = 1;
		queryTriplets.push_back(tempTri);

		SVertexArray tempVer = {};
		tempVer.label = 1;
		tempVer.vertexId = 1;
		queryVertex.push_back(tempVer);

		tempVer.label = 2;
		tempVer.vertexId = 2;
		queryVertex.push_back(tempVer);

	}

	mGPMType SendInvalidedMsg(long long id){
		mGPMType invalided;
		invalided.SetInvalided(id);
		return invalided;
	}


	bool ExtractQuery(){

//		if(dataVertex.size()< queryVertices){
//			return false;
//		}
		return true;

//		int range = dataTriplets.size()*(4/5);
//		srand (time(NULL));
//		int num = rand() % range;
//		SVertexArray temp = {};
//		int i=0;
//
//		std::vector<STriplets>::iterator it = dataTriplets.begin();
//		for(i=0; i<num; i++){
//			it++;
//		}
//
//		temp.vertexId = (*it).src.Id;
//		temp.label = (*it).src.label;
//		queryVertex.push_back(temp);
//
//		std::vector<SVertexArray>::iterator itQuery = queryVertex.begin();
//		int length=0;
//
//		do{
//
//			 int currentId = (*itQuery).vertexId;
//			 itQuery++;
//
//			 for(it = dataTriplets.begin(); it!= dataTriplets.end(); it++){
//
//				 if(currentId == (*it).src.Id){
//
//					 if( length < queryVertices-1){
//						temp.vertexId = (*it).dst.Id;
//						temp.label = (*it).dst.label;
//						queryVertex.push_back(temp);
//
//						length = queryVertex.size()-1;
//
//						queryTriplets.push_back(*it);
//					 }
//					 else{
//					   break;
//					 }
//				 }
//			 }
//
////			 queryVertex.unique;
//			 length = queryVertex.size()-1;
//
//		}while(length < queryVertices-1);

	}

	void ExtractMap(){

		std::vector<SVertexArray>::iterator itVertex = queryVertex.begin();
		std::vector<long long> labels;

		for(itVertex = queryVertex.begin(); itVertex!= queryVertex.end(); itVertex++){
			long long verLabel = (*itVertex).label;
			long long verId = (*itVertex).vertexId;

			if( queryCollect.find(verLabel) != queryCollect.end()	){
				queryCollect[verLabel].push_back(verId);

			}else{
				std::vector<long long> tempVer;
				tempVer.push_back(verId);
				queryCollect.insert( std::pair<long long, std::vector<long long> >(verLabel, tempVer) );
			}
		}

//		std::map<int, std::vector<int> >::iterator itMap;
//
//		std::vector<int>::iterator itA;
//		for(itMap = queryCollect.begin(); itMap!=queryCollect.end(); itMap++){
//			printf("key: %d  ;", itMap->first);
//			printf( " value : ");
//			for(itA = itMap->second.begin(); itA != itMap->second.end(); itA++){
//				printf( " %d ,", *itA);
//			}
//			printf( "\n");
//		}
//
//		std::vector<STriplets>::iterator itTri;
//		for(itTri = dataTriplets.begin(); itTri !=dataTriplets.end(); itTri++){
//			printf("triplet : %d : %d\n", (*itTri).src.Id, (*itTri).dst.Id);
//		}
//
//		for(itVertex = dataVertex.begin(); itVertex!= dataVertex.end(); itVertex++){
//			printf("Vertex id : %d : %d\n", (*itVertex).vertexId, (*itVertex).label);
//		}
	}


	mGPMType InitVertex(long long curLabel, long long vertexId){

		mGPMType outVal;

		std::vector<STriplets> tmpTriplets;

		std::vector<SInputVertices>::iterator itIn;
		for(itIn = g_validVertices.begin(); itIn!= g_validVertices.end(); itIn++){
			if( ((*itIn).srcId== vertexId) || ((*itIn).dstId == vertexId)  ){
				STriplets temp;
				temp.src.Id = (*itIn).srcId;
				temp.src.flag = true;
				temp.dst.Id = (*itIn).dstId;
				temp.dst.flag = true;
				tmpTriplets.push_back(temp);

//				printf("vertexid : %d ; src:  %d,  dst:  %d\n", vertexId, temp.src.Id, temp.dst.Id);

			}
		}


		std::map<long long, std::vector<long long> >::iterator itM;
		bool flag = false;

		for(itM = queryCollect.begin(); itM!=queryCollect.end(); ++itM){

			if( itM->first == curLabel){
				flag = true;
				break;
			}

		}

		if(flag){


			std::vector<long long> matchArray = queryCollect[curLabel];


			SVertexProperties temp={};
			temp.m_flag = true;
			temp.m_matchSet = matchArray;
			temp.m_label = curLabel;
			temp.m_dataTriplets = tmpTriplets;
			temp.vertexId = vertexId;

			mGPMType valided(temp);
			outVal = valided;

		}else{

			SVertexProperties temp={};
			temp.m_flag = false;
			temp.m_dataTriplets = tmpTriplets;
			temp.vertexId = vertexId;
			mGPMType invalided(temp);
			outVal = invalided;
		}

		return outVal;
	}

};


#endif /* PATTERN_MATCHING_H_ */
