

#ifndef ADDITIONAL_H
#define ADDITIONAL_H

#include "dataManager/dataStructures/data/mGPMType.h"
#include "dataManager/dataManager.h"
#include "algorithms/SSSP.h"
#include "algorithms/patternMatching.h"

std::vector<SVertexArray> validData;

void Check(std::vector<SVertexArray> query, std::vector<SVertexArray> data){

	std::vector<SVertexArray>::iterator it;
	std::vector<SVertexArray>::iterator itData;
	bool flag = true;
	int num = 0;

	for(it = query.begin(); it!=query.end(); it++){

		for(itData = data.begin(); itData!=data.end(); itData++){

			if((*itData).flag ==true && flag){

				validData.push_back(*itData);

			}
			else{
				if( ((*itData).vertexId == (*it).vertexId)){
					printf("result miss %d \n", (*itData).vertexId);
				}
			}
		}
		flag = false;
	}
	printf("the total num : %d\n", validData.size());

}


void onlyForTest(std::vector<SVertexArray> &queryVertex,std::vector<STriplets> &queryTriplets){

	STriplets tempTri = {};
	tempTri.src.Id = 1;
	tempTri.dst.Id = 2;
	queryTriplets.push_back(tempTri);

	tempTri.src.Id = 2;
	tempTri.dst.Id = 1;
	queryTriplets.push_back(tempTri);

	tempTri.src.Id = 2;
	tempTri.dst.Id = 3;
	queryTriplets.push_back(tempTri);


	tempTri.src.Id = 3;
	tempTri.dst.Id = 5;
	queryTriplets.push_back(tempTri);

	tempTri.src.Id = 2;
	tempTri.dst.Id = 4;
	queryTriplets.push_back(tempTri);

	tempTri.src.Id = 4;
	tempTri.dst.Id = 5;
	queryTriplets.push_back(tempTri);

	tempTri.src.Id = 4;
	tempTri.dst.Id = 5;
	queryTriplets.push_back(tempTri);

	tempTri.src.Id = 5;
	tempTri.dst.Id = 4;
	queryTriplets.push_back(tempTri);

	tempTri.src.Id = 4;
	tempTri.dst.Id = 1;
	queryTriplets.push_back(tempTri);

	tempTri.src.Id = 6;
	tempTri.dst.Id = 1;
	queryTriplets.push_back(tempTri);

	tempTri.src.Id = 7;
	tempTri.dst.Id = 3;
	queryTriplets.push_back(tempTri);

	tempTri.src.Id = 6;
	tempTri.dst.Id = 8;
	queryTriplets.push_back(tempTri);

	tempTri.src.Id = 8;
	tempTri.dst.Id = 9;
	queryTriplets.push_back(tempTri);

	tempTri.src.Id = 9;
	tempTri.dst.Id = 8;
	queryTriplets.push_back(tempTri);



	SVertexArray tempVer = {};
	tempVer.label = 1;
	tempVer.vertexId = 1;
	queryVertex.push_back(tempVer);

	tempVer.label = 2;
	tempVer.vertexId = 2;
	queryVertex.push_back(tempVer);

	tempVer.label = 3;
	tempVer.vertexId = 3;
	queryVertex.push_back(tempVer);

	tempVer.label = 4;
	tempVer.vertexId = 4;
	queryVertex.push_back(tempVer);

	tempVer.label = 5;
	tempVer.vertexId = 5;
	queryVertex.push_back(tempVer);

	tempVer.label = 6;
	tempVer.vertexId = 6;
	queryVertex.push_back(tempVer);
	tempVer.label = 7;
	tempVer.vertexId = 7;
	queryVertex.push_back(tempVer);

	tempVer.label = 8;
	tempVer.vertexId = 8;
	queryVertex.push_back(tempVer);

	tempVer.label = 9;
	tempVer.vertexId = 9;
	queryVertex.push_back(tempVer);
}


std::map<long long, long long > GetEccEach(char ** inputBaseFile,MizanArgs myArgs,  int argc, char** argv){



	std::vector<STriplets> queryTriplets = g_queryTriplets;
	std::vector<SVertexArray> queryVertex = g_queryVertex;
	std::vector<SVertexArray>::iterator ssspIt;

	std::map<long long, long long > itEcc;

	printf("query : %d, %d\n", queryTriplets.size(), queryVertex.size());
	for(ssspIt = queryVertex.begin(); ssspIt!=queryVertex.end(); ssspIt++)
	{

		bool groupVoteToHalt;
		edgeStorage storageType;

		groupVoteToHalt = true;
		storageType = OutNbrStore;

		SSSP sp((*ssspIt).vertexId);

		SSSPCombiner sprc;

		Mizan<mLong, mLong, mLong, mLong> * mmkS = new Mizan<mLong, mLong,
				mLong, mLong>(myArgs.communication, &sp, storageType,
				inputBaseFile, myArgs.clusterSize, myArgs.fs, myArgs.migration);

		mmkS->registerMessageCombiner(&sprc);

		mmkS->setVoteToHalt(groupVoteToHalt);

		//User Defined aggregator
		char * maxAggA = "maxAggregatorA";
		maxAggregator maxi;
		mmkS->registerAggregator(maxAggA, &maxi);


		sp.Reset((*ssspIt).vertexId);
		mmkS->run(argc, argv);
		long long ecc =  sp.GetMinEcc();
		printf("id : %d, ecc : %d \n", (*ssspIt).vertexId, ecc);
		itEcc.insert( std::pair<long long, long long >((*ssspIt).vertexId, ecc));
		delete mmkS;
	}


	return itEcc;
}

void ShowEcc(std::map<long long, long long > ecc){

	std::map<long long, long long >::iterator itMap;

	for(itMap = ecc.begin(); itMap!=ecc.end(); itMap++){
		printf("key: %d , value : %d\n", itMap->first, itMap->second);
	}
}

void GetEcc(char ** inputBaseFile,MizanArgs myArgs, int argc, char** argv, long long &vId, long long &label,
		long long &radius){


		std::map<long long, long long >::iterator itMap;
		std::map<long long, long long > itEcc;

		printf("GetEccEach\n");
		itEcc = GetEccEach( inputBaseFile, myArgs, argc, argv);

		radius = MAX_DISTANCE;
		for(itMap = itEcc.begin(); itMap!=itEcc.end(); itMap++){

			if(itMap->second < radius){
				radius = itMap->second;
				vId = itMap->first;
			}
		}

		ShowEcc(itEcc);

		std::vector<SVertexArray> queryVertex = g_queryVertex;
		std::vector<SVertexArray>::iterator ssspIt;

		for(ssspIt = queryVertex.begin(); ssspIt!=queryVertex.end(); ssspIt++)
		{
			if( (*ssspIt).vertexId == vId){
				label = (*ssspIt).label;
			}
		}
}

std::vector<long long> GetCandidates(std::vector<SVertexArray> dataVertex, long long label){
	std::vector<long long> result;
	std::vector<SVertexArray>::iterator it;
	for(it = dataVertex.begin(); it!=dataVertex.end(); it++)
	{
		if( (*it).label == label  &&  (*it).flag ){
			result.push_back((*it).vertexId);
		}
	}
	return result;
}



template<class InputIterator, class T>
  InputIterator find_vector (InputIterator first, InputIterator last, const T& val)
{
  while (first!=last) {
    if (*first==val) return first;
    ++first;
  }
  return last;
}

std::vector<SVertexArray> CreateBall(long long id, long long radius, std::vector<SVertexArray> dataVertex){
	std::vector<long long> result;
	std::vector<long long> target;

	long long round = radius;

	result.push_back(id);

	std::vector<SInputVertices>::iterator itTri;

	target = result;

	while(round > 0){

		std::vector<long long> findVer;
		for(itTri=g_validVertices.begin(); itTri!=g_validVertices.end(); ++itTri){

			if(find_vector(target.begin(), target.end(), ((*itTri).srcId)) !=target.end()
					&&  find_vector(findVer.begin(), findVer.end(), ((*itTri).dstId)) ==findVer.end()){
				findVer.push_back((*itTri).dstId);
			}

			if(find_vector(target.begin(), target.end(), ((*itTri).dstId)) !=target.end()
					&&  find_vector(findVer.begin(), findVer.end(), ((*itTri).srcId)) ==findVer.end()){
				findVer.push_back((*itTri).srcId);
			}

		}

		target = findVer;
		result.insert(result.end(), findVer.begin(), findVer.end());
		round--;
	}

	std::vector<SVertexArray> final;
	std::vector<long long>::iterator it;
	SVertexArray temp={};
	for(it=result.begin();it!=result.end();++it){
		temp.vertexId = *it;
	}

	return final;
}


int DualMatching(char ** inputBaseFile,MizanArgs myArgs, int argc, char** argv){

	bool groupVoteToHalt;
	edgeStorage storageType;

	groupVoteToHalt = true;
	storageType = OutNbrStore;

	PatternMatching us(myArgs.superSteps);
	PatternMatchingCombiner prc;

	Mizan<mLong, mGPMType, mGPMType, mLong> * mmkP = new Mizan<mLong, mGPMType,
			mGPMType, mLong>(myArgs.communication, &us, storageType,
			inputBaseFile, myArgs.clusterSize, myArgs.fs, myArgs.migration);

	mmkP->registerMessageCombiner(&prc);

	mmkP->setVoteToHalt(groupVoteToHalt);


	//User Defined aggregator
	char * maxAgg = "maxAggregator";
	maxAggregator maxi;
	mmkP->registerAggregator(maxAgg, &maxi);

//	ResetDataTri();

	mmkP->run(argc, argv);

	Check(g_queryVertex, check_dataVertex);


	delete mmkP;
}



std::vector<long long> TightCal(char ** inputBaseFile,MizanArgs myArgs, int argc, char** argv,
		std::vector<long long> candidates, long long radius,std::vector<SVertexArray> dataVertex){

	std::vector<long long> result;
	std::vector<long long> temp;
	std::vector<long long>::iterator it;

	std::vector<SVertexArray> tempDataVer;
	std::vector<SVertexArray>::iterator itSV;


	for(it = candidates.begin(); it!= candidates.end(); ++it){

		std::vector<SVertexArray> ballVer = CreateBall(*it, radius, dataVertex);

		tempDataVer = g_dataVertex;
		g_dataVertex = ballVer;

		//Apply dual in ballVer
		DualMatching(inputBaseFile, myArgs,argc, argv);

		for(itSV=check_dataVertex.begin();itSV!=check_dataVertex.end();++itSV){
			if((*itSV).flag){
				result.push_back((*itSV).vertexId);
			}
		}

		g_dataVertex = tempDataVer;
	}
	return result;
}



int MainPro(char ** inputBaseFile,MizanArgs myArgs, int argc, char** argv){

	long long centerId = 0;
	long long label = 0;
	long long radius = 0;


	std::vector<SVertexArray> ssspVertex;
	std::vector<STriplets> ssspTriplets ;
//	onlyForTest(ssspVertex, ssspTriplets);
	ssspTriplets = g_queryTriplets;
	ssspVertex = g_queryVertex;


	DualMatching(inputBaseFile, myArgs,argc, argv);


	GetEcc(inputBaseFile, myArgs, argc, argv, centerId, label, radius);
	printf("id : %d, radius : %d, label : %d\n", centerId, radius, label);

//	std::vector<long long> candidates = GetCandidates(check_dataVertex, label);
//
//	std::vector<long long> result =TightCal(inputBaseFile, myArgs,argc, argv,
//			candidates, radius, check_dataVertex);


}




#endif /*ADDITIONAL_H*/
