/*
 * mGPMType.h
 *
 *  Created on: Oct 15, 2015
 *      Author: Chao Chen
 */

#ifndef M_GPM_TYPE_H_
#define M_GPM_TYPE_H_

#include "IdataType.h"
#include <vector>
#include <list>


typedef struct{
	long long vertexId;
	long long label;
	bool flag;
}SVertexArray;


typedef struct{
	long long Id;
	bool flag;
	long long label;
}SVertex;

typedef struct{
	SVertex src;
	SVertex dst;
}STriplets;


typedef struct{

	long long vertexId;
	std::vector<long long> m_matchSet;
	long long m_label;
	bool m_flag;
	std::vector<STriplets> m_dataTriplets;
	std::map<long long, std::vector<long long> > m_childrenSet;
	std::map<long long, std::vector<long long> > m_parentsSet;

}SVertexProperties;


class mGPMType: public IdataType {
private:
//	SVertexProperties m_properties;
	std::vector<SVertexProperties> m_vertexMessage;

public:


	mGPMType();
	mGPMType(const mGPMType& obj);
	mGPMType(SVertexProperties temp);
	mGPMType(std::vector<SVertexProperties> temp);
	virtual ~mGPMType();


	//Class specific methods
	void cleanUp();
//	std::vector<int> mergeVector(std::vector<int> str);
	std::vector<SVertexProperties> mergeVector(SVertexProperties input);
//	int  getValue();


	int byteSize();
	std::string toString();
	void readFromCharArray(char * input);
	char * byteEncode(int &size);
	void byteDecode(int size, char * input);

	int byteEncode2(char * buffer);

	std::size_t local_hash_value() const;
	mGPMType & operator=(const mGPMType& rhs);
	bool operator==(const IdataType& rhs) const;
	bool operator<(const IdataType& rhs) const;
	bool operator>(const IdataType &rhs) const;
	bool operator<=(const IdataType &rhs) const;
	bool operator>=(const IdataType &rhs) const;

	//Class specific methods
	std::vector<int>  getArray() const;
	int getSize() const;
	void setArray(std::vector<int> str);
	void setDelimiter(char inDelimiter);

	long long GetLabel();
	std::vector<long long> GetMatchSet();
	bool GetFlag();
	std::vector<STriplets> GetTriplets();
	std::map<long long, std::vector<long long> > GetParentMatch();
	std::map<long long, std::vector<long long> > GetChildrenMatch();

	void SetLabel(long long label);
	void SetMatchSet(std::vector<long long> matchSet);
	void SetFlag(bool flag);
	void SetTriplets(std::vector<STriplets> triplet);
	void SetParentMatch(std::map<long long, std::vector<long long> > vec);
	void SetChildrenMatch(std::map<long long, std::vector<long long> > vec);

	void SetInvalided(long long id);
	void SetId(long long id);
	long long GetId();

//	void SetValue(SVertexProperties temp);

	std::vector<SVertexProperties> GetProperties();



};
#endif /* M_GPM_TYPE_H_ */
