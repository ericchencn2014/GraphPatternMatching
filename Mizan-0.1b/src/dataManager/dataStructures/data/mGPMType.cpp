

#include "mGPMType.h"




mGPMType::mGPMType(){
	// TODO Auto-generated constructor stub
}



mGPMType::mGPMType(const mGPMType& obj) {
	this->m_vertexMessage = obj.m_vertexMessage;
}

mGPMType::mGPMType(SVertexProperties temp){
	m_vertexMessage.clear();
	m_vertexMessage.push_back(temp);
}

mGPMType::mGPMType(std::vector<SVertexProperties> temp){
	m_vertexMessage = temp;
}

mGPMType::~mGPMType() {
	cleanUp();
}

#ifdef __cplusplus
extern "C" {
#endif


void  U32ToArray(unsigned char tempU8[4],unsigned int aData)
{
	tempU8[0]=(unsigned char)( (aData&0x00000000ff000000ULL)  >>24);
	tempU8[1]=(unsigned char)( (aData&0x0000000000ff0000ULL)  >>16);
	tempU8[2]=(unsigned char)( (aData&0x000000000000ff00ULL)  >>8) ;
	tempU8[3]=(unsigned char)  (aData&0x00000000000000ffULL)       ;
}

unsigned int ArrayToU32(unsigned char tempU8[4]){

	unsigned int data=0;

	data = tempU8[0]<<24;
	data = data | (tempU8[1]<<16);
	data = data | (tempU8[2]<<8);
	data = data | (tempU8[3]);

	return data;
}


void  L64ToArray(unsigned char tempU8[8],long long aData)
{
	tempU8[0]=(unsigned char)( (aData&0xff00000000000000ULL)  >>56);
	tempU8[1]=(unsigned char)( (aData&0x00ff000000000000ULL)  >>48);
	tempU8[2]=(unsigned char)( (aData&0x0000ff0000000000ULL)  >>40);
	tempU8[3]=(unsigned char)( (aData&0x000000ff00000000ULL)  >>32);

	tempU8[4]=(unsigned char)( (aData&0x00000000ff000000ULL)  >>24);
	tempU8[5]=(unsigned char)( (aData&0x0000000000ff0000ULL)  >>16);
	tempU8[6]=(unsigned char)( (aData&0x000000000000ff00ULL)  >>8) ;
	tempU8[7]=(unsigned char)  (aData&0x00000000000000ffULL)       ;
}

long long ArrayToL64(unsigned char tempU8[8]){

	unsigned int msbData=0;
	unsigned int lsbData=0;

	long long value = 0;

	lsbData = tempU8[0]<<24;
	lsbData = lsbData | (tempU8[1]<<16);
	lsbData = lsbData | (tempU8[2]<<8);
	lsbData = lsbData | (tempU8[3]);

	msbData = msbData | (tempU8[4]<<24);
	msbData = msbData | (tempU8[5]<<16);
	msbData = msbData | (tempU8[6]<<8);
	msbData = msbData | (tempU8[7]);

	value =  (((long long )msbData)) | (((long long )lsbData << 32));
	return value;
}


unsigned char * MapEncode(std::map<long long, std::vector<long long> > map, int &size, unsigned char * buf) {

	int len = 0;

	std::map<long long, std::vector<long long> >::iterator itMap;

	std::vector<long long>::iterator itA;

	for(itMap = map.begin(); itMap!=map.end(); itMap++){

		unsigned char temp64bit[8]={0};
		L64ToArray(temp64bit, (*itMap).first);
		memcpy( &buf[len], temp64bit, sizeof(temp64bit) );
		len+=sizeof(temp64bit);

		int posLen = len;		//4 byte for length of value
		len+=4;

		int lenOfValue = 0;

		for(itA = itMap->second.begin(); itA != itMap->second.end(); itA++){
			unsigned char tp64bit[8]={0};
			L64ToArray(tp64bit, (*itA));
			memcpy( &buf[len], tp64bit, sizeof(tp64bit) );
			len+=sizeof(tp64bit);
			lenOfValue+=sizeof(tp64bit);
		}

		unsigned char u32[4]={0};
		U32ToArray(u32, lenOfValue);
		memcpy(&buf[posLen], u32, sizeof(u32) );

	}
	size = len;
	return buf;
}

#define MIN_MAP_FRAME 20	// 1*key + 1*lenOfValue + 1*value


std::map<long long, std::vector<long long> >  MapDecode(unsigned char* buf, int size) {

	int len = 0;

	std::map<long long, std::vector<long long> >  result ;

	if(buf == NULL || size<MIN_MAP_FRAME){
		return result;
	}

	while( len < size){

		if( (size-len) <MIN_MAP_FRAME){
			return result;
		}

		long long key = 0;
		long long value = 0;

		std::vector<long long> vec;
		unsigned char temp64bit[8]={0};
		memcpy(temp64bit, &buf[len], 8);
		key = ArrayToL64(temp64bit);
		len+=8;

		unsigned char temp32bit[4]={0};
		memcpy(temp32bit, &buf[len], 4);
		unsigned int lenOfVaule = ArrayToU32(temp32bit);
		len+=4;


		while( lenOfVaule >=8 ){
			memset(temp64bit, 0, 8);
			memcpy(temp64bit, &buf[len], 8);
			value = ArrayToL64(temp64bit);
			vec.push_back(value);
			len+=8;
			lenOfVaule-=8;
		}
		result.insert(std::pair<long long, std::vector<long long> >(key, vec));


	}
	return result;
}


#define ADD_ITEMS(item) \
	memset(temp64bit, 0 , sizeof(temp64bit));\
	L64ToArray(temp64bit, item);\
	memcpy( &buf[len], temp64bit, sizeof(temp64bit) );\
	len+=sizeof(temp64bit);




unsigned char * TriEncode(std::vector<STriplets> tri, int &size, unsigned char * buf){
	int len=0;

	unsigned char temp64bit[8]={0};
	std::vector<STriplets>::iterator it;

	for(it = tri.begin(); it!= tri.end(); it++){
		ADD_ITEMS( (*it).src.Id );

		temp64bit[0] = (*it).src.flag? 1:0;
		memcpy( &buf[len], temp64bit, 1 );
		len++;

		ADD_ITEMS((*it).src.label);

		ADD_ITEMS( (*it).dst.Id );

		temp64bit[0] = (*it).dst.flag? 1:0;
		memcpy( &buf[len], temp64bit, 1 );
		len++;

		ADD_ITEMS((*it).dst.label);

	}
	size = len;
	return buf;
}



#define GET_ITEM(item) \
	memset(temp64bit, 0, 8);\
	memcpy(temp64bit, &buf[len], 8);\
	item = ArrayToL64(temp64bit);\
	len+=sizeof(temp64bit);

#define GET_LENGTH	\
	memset(temp32bit, 0, 4);\
	memcpy(temp32bit, &buf[len], 4);\
	lenOfVaule = ArrayToU32(temp32bit);\
	len+=4;

std::vector<long long> VecDecode(int size, unsigned char* buf){

	std::vector<long long> result;
	int len=0;
	unsigned char temp64bit[8]={0};

	if( buf==NULL || ( (size % 8 ) != 0)){
		return result;
	}

	while( len < size){
		long long value;
		GET_ITEM(value);
		result.push_back( value);
	}

	return result;
}





unsigned char* GPMEncode(SVertexProperties data, int &size, unsigned char* buf){

	if(buf==NULL){
		return NULL;
	}

	int len=0;

	unsigned char temp64bit[8]={0};

	ADD_ITEMS(data.vertexId);

	temp64bit[0] = data.m_flag? 1:0;
	memcpy( &buf[len], temp64bit, 1 );
	len++;

	ADD_ITEMS(data.m_label);

	int posLen = len;
	len+=4;
	int lenOfValue = 0;

	std::vector<long long>::iterator vIt;
	for(vIt = data.m_matchSet.begin(); vIt!=data.m_matchSet.end(); vIt++){
		ADD_ITEMS( (*vIt));
		lenOfValue+=8;
	}

	unsigned char u32[4]={0};
	U32ToArray(u32, lenOfValue);
	memcpy(&buf[posLen], u32, sizeof(u32) );


	posLen = len;
	len+=4;
	lenOfValue = 0;
	MapEncode(data.m_childrenSet, lenOfValue, &buf[len]);
	len+=lenOfValue;
	U32ToArray(u32, lenOfValue);
	memcpy(&buf[posLen], u32, sizeof(u32) );


	posLen = len;
	len+=4;
	lenOfValue = 0;
	MapEncode(data.m_parentsSet, lenOfValue, &buf[len]);
	len+=lenOfValue;
	U32ToArray(u32, lenOfValue);
	memcpy(&buf[posLen], u32, sizeof(u32) );



	posLen = len;
	len+=4;
	lenOfValue = 0;
	TriEncode(data.m_dataTriplets, lenOfValue, &buf[len]);
	len+=lenOfValue;
	U32ToArray(u32, lenOfValue);
	memcpy(&buf[posLen], u32, sizeof(u32) );

	size = len;
	return buf;
}


int MapByte(std::map<long long, std::vector<long long> > map){
	int len = 0;

	std::map<long long, std::vector<long long> >::iterator itMap;

	std::vector<long long>::iterator itA;

	for(itMap = map.begin(); itMap!=map.end(); itMap++){


		len+=8;

		len+=4;

		for(itA = itMap->second.begin(); itA != itMap->second.end(); itA++){
			len+=8;
		}

	}
	return len;
}

int TriByte(std::vector<STriplets> tri){
	int len=0;

	unsigned char temp64bit[8]={0};
	std::vector<STriplets>::iterator it;

	for(it = tri.begin(); it!= tri.end(); it++){
		len+=34;
	}
	return len;
}

int GPMByte(SVertexProperties data){

	int len=0;

	unsigned char temp64bit[8]={0};

	len+=sizeof(temp64bit);

	len++;

	len+=sizeof(temp64bit);


	len+=4;
	int lenOfValue = 0;

	std::vector<long long>::iterator vIt;
	for(vIt = data.m_matchSet.begin(); vIt!=data.m_matchSet.end(); vIt++){
		lenOfValue+=8;
	}
	len+=lenOfValue;

	len+=4;
	lenOfValue = MapByte(data.m_childrenSet);
	len+=lenOfValue;

	len+=4;
	lenOfValue = MapByte(data.m_parentsSet);
	len+=lenOfValue;

	len+=4;
	lenOfValue = TriByte(data.m_dataTriplets);
	len+=lenOfValue;

	return len;
}


std::vector<STriplets> TriDecode(unsigned char * buf , int size){
	int len=0;
	std::vector<STriplets> result;
	unsigned char temp64bit[8]={0};
	std::vector<STriplets>::iterator it;

	if(size%34 != 0){
		return result;
	}

	while(len < size){

		STriplets temp = {0};

		GET_ITEM( temp.src.Id );

		if(buf[len] == 0){
			temp.src.flag = false;
		}
		else{
			temp.src.flag = true;
		}
		len++;

		GET_ITEM(temp.src.label);



		GET_ITEM( temp.dst.Id );

		if(buf[len] == 0){
			temp.dst.flag = false;
		}
		else{
			temp.dst.flag = true;
		}
		len++;

		GET_ITEM(temp.dst.label);

		result.push_back(temp);
	}
	return result;
}




SVertexProperties GPMDecode(int size, unsigned char* buf){
	SVertexProperties result = {0};

	int len=0;
	unsigned char temp64bit[8]={0};


	GET_ITEM(result.vertexId);

	if(buf[len] == 0){
		result.m_flag = false;
	}
	else{
		result.m_flag = true;
	}
	len++;

	GET_ITEM(result.m_label);


	unsigned int lenOfVaule = 0;
	unsigned char temp32bit[4]={0};

	GET_LENGTH;

	result.m_matchSet = VecDecode( lenOfVaule, &buf[len]);
	len+=lenOfVaule;

	GET_LENGTH;

	result.m_childrenSet = MapDecode(&buf[len], lenOfVaule);
	len+=lenOfVaule;

	GET_LENGTH;

	result.m_parentsSet = MapDecode(&buf[len], lenOfVaule);


	len+=lenOfVaule;

	GET_LENGTH;

	result.m_dataTriplets = TriDecode(&buf[len], lenOfVaule);

	return result;

}

#ifdef __cplusplus
}
#endif


int mGPMType::byteSize() {
	int size = 0;

	std::vector<SVertexProperties>::iterator it;
	for(it = m_vertexMessage.begin(); it!=m_vertexMessage.end();it++){
		int num = GPMByte((*it))+4;
		size+=num;
	}
	return size;
}

std::string mGPMType::toString() {
	printf("toString\n");
	std::string output;
	return output;
}

void mGPMType::readFromCharArray(char * input) {

}

char * mGPMType::byteEncode(int &inSize) {
	char * output = (char *) calloc( (byteSize()), sizeof(char));

	std::vector<SVertexProperties>::iterator it;
	int len = 0;

	for(it = m_vertexMessage.begin(); it!=m_vertexMessage.end();it++){
		int posLen = len;
		len+=4;
		int lenOfValue = 0;

		GPMEncode(*it, lenOfValue, (unsigned char*)&output[len]);

		unsigned char u32[4]={0};
		len+=lenOfValue;
		U32ToArray(u32, lenOfValue);
		memcpy(&output[posLen], u32, sizeof(u32) );
	}
	inSize = len;
	return output;
}

int mGPMType::byteEncode2(char * buffer){
	return 1;
}

void mGPMType::byteDecode(int inSize, char * input) {
	int len=0;
	unsigned char temp64bit[8]={0};

	m_vertexMessage.clear();

	while(len < inSize){
		unsigned int lenOfVaule = 0;
		unsigned char temp32bit[4]={0};

		memset(temp32bit, 0, 4);\
		memcpy(temp32bit, &input[len], 4);\
		lenOfVaule = ArrayToU32(temp32bit);\
		len+=4;

		SVertexProperties recValue = GPMDecode(lenOfVaule ,(unsigned char*)&input[len]);
		len+=lenOfVaule;

		m_vertexMessage.push_back(recValue);
	}
}


std::size_t mGPMType::local_hash_value() const {
	printf("local_hash_value\n");
//	if (size > 0) {
//		return array[0].local_hash_value();
//	}
	return 0;
}
mGPMType & mGPMType::operator=(const mGPMType& rhs) {

	this->m_vertexMessage = rhs.m_vertexMessage;

	return *this;
}

bool mGPMType::operator==(const IdataType& rhs) const {
	printf("operator==\n");
	int size2 = 0;
	if (0 != size2) {
		return false;
	}

	return true;
}

bool mGPMType::operator<(const IdataType& rhs) const {
	printf("operator<\n");
//	int size2 = ((mLongArray&) rhs).getSize();
//	mLong * array2 = ((mLongArray&) rhs).getArray();
//	for (int i = 0; i < size && i < size2; i++) {
//		if ((array[i] >= array2[i])) {
//			return false;
//		}
//	}
	return true;
}
bool mGPMType::operator>(const IdataType& rhs) const {
	printf("operator>\n");
//	int size2 = ((mLongArray&) rhs).getSize();
//	mLong * array2 = ((mLongArray&) rhs).getArray();
//	for (int i = 0; i < size && i < size2; i++) {
//		if ((array[i] <= array2[i])) {
//			return false;
//		}
//	}
	return true;
}
bool mGPMType::operator<=(const IdataType& rhs) const {
	printf("operator<=\n");
//	int size2 = ((mLongArray&) rhs).getSize();
//	mLong * array2 = ((mLongArray&) rhs).getArray();
//	for (int i = 0; i < size && i < size2; i++) {
//		if ((array[i] > array2[i])) {
//			return false;
//		}
//	}
	return true;
}
bool mGPMType::operator>=(const IdataType& rhs) const {
	printf("operator>=\n");
//	int size2 = ((mLongArray&) rhs).getSize();
//	mLong * array2 = ((mLongArray&) rhs).getArray();
//	for (int i = 0; i < size && i < size2; i++) {
//		if ((array[i] < array2[i])) {
//			return false;
//		}
//	}
	return true;
}

//Class specific methods
std::vector<int>  mGPMType::getArray() const {
	std::vector<int> temp;
	return temp;
}
int mGPMType::getSize() const {
	return 0;
}
void mGPMType::setArray(std::vector<int> str) {
	return;
}





std::vector<SVertexProperties> mGPMType::mergeVector(SVertexProperties input){

	this->m_vertexMessage.push_back(input);

	return m_vertexMessage;
}


void mGPMType::cleanUp() {
	m_vertexMessage.clear();
	SVertexProperties temp = {};
	temp.m_flag = false;
//	temp.vertexId = 0;
	m_vertexMessage.push_back(temp);
}


//int  mGPMType::getValue(){
//
//	return m_properties.m_flag;
//}

long long mGPMType::GetLabel(){
	if(m_vertexMessage.size()>0){
		return m_vertexMessage[0].m_label;
	}
	std::cout<< " error m_vertexMessage.size()=0!\n" << std::endl;
	return -1;
}

std::vector<long long> mGPMType::GetMatchSet(){

	if(m_vertexMessage.size()>0){
		return m_vertexMessage[0].m_matchSet;
	}
	std::cout<< " error m_vertexMessage.size()=0!\n" << std::endl;
	std::vector<long long> tmp;
	return tmp;
}

bool mGPMType::GetFlag(){
	if(m_vertexMessage.size()>0){
		return m_vertexMessage[0].m_flag;
	}
	std::cout<< " error m_vertexMessage.size()=0!\n" << std::endl;
	return false;
}

std::vector<STriplets> mGPMType::GetTriplets(){
	if(m_vertexMessage.size()>0){
		return m_vertexMessage[0].m_dataTriplets;
	}
	std::cout<< " error m_vertexMessage.size()=0!\n" << std::endl;
	std::vector<STriplets> tmp;
	return tmp;
}


std::map<long long, std::vector<long long> > mGPMType::GetParentMatch(){
	if(m_vertexMessage.size()>0){
		return m_vertexMessage[0].m_parentsSet;
	}
	std::cout<< " error m_vertexMessage.size()=0!\n" << std::endl;
	std::map<long long, std::vector<long long> > tmp;
	return tmp;
}
std::map<long long, std::vector<long long> > mGPMType::GetChildrenMatch(){
	if(m_vertexMessage.size()>0){
		return m_vertexMessage[0].m_childrenSet;
	}
	std::cout<< " error m_vertexMessage.size()=0!\n" << std::endl;
	std::map<long long, std::vector<long long> > tmp;
	return tmp;
}



void mGPMType::SetLabel(long long label){
	if(this->m_vertexMessage.size()>0){
		this->m_vertexMessage[0].m_label = label;
		return;
	}
	std::cout<< " error m_vertexMessage.size()=0!\n" << std::endl;
}

void mGPMType::SetMatchSet(std::vector<long long> matchSet){
	if(m_vertexMessage.size()>0){
		m_vertexMessage[0].m_matchSet = matchSet;
		return;
	}
	std::cout<< " error m_vertexMessage.size()=0!\n" << std::endl;
}

void mGPMType::SetFlag(bool flag){
	if(m_vertexMessage.size()>0){
		m_vertexMessage[0].m_flag = flag;
		return;

	}
	std::cout<< " error m_vertexMessage.size()=0!\n" << std::endl;
}

void mGPMType::SetTriplets(std::vector<STriplets> triplet){
	if(m_vertexMessage.size()>0){
		m_vertexMessage[0].m_dataTriplets = triplet;
		return;
	}
	std::cout<< " error m_vertexMessage.size()=0!\n" << std::endl;
}

void mGPMType::SetInvalided(long long id){
	cleanUp();
	m_vertexMessage[0].vertexId = id;
}

void mGPMType::SetParentMatch(std::map<long long, std::vector<long long> > vec){
	if(m_vertexMessage.size()>0){
		m_vertexMessage[0].m_parentsSet = vec;
		return;
	}
	std::cout<< " error m_vertexMessage.size()=0!\n" << std::endl;
}
void mGPMType::SetChildrenMatch(std::map<long long, std::vector<long long> > vec){
	if(m_vertexMessage.size()>0){
		m_vertexMessage[0].m_childrenSet = vec;
		return ;
	}
	std::cout<< " error m_vertexMessage.size()=0!\n" << std::endl;
}


void mGPMType::SetId(long long id){
	printf(" input id:%d\n", id);
	if(this->m_vertexMessage.size()>0){
		this->m_vertexMessage[0].vertexId = id;
		printf(" input id:%d\n", this->m_vertexMessage[0].vertexId);
		return;
	}
	std::cout<< " error m_vertexMessage.size()=0!\n" << std::endl;
}

long long mGPMType::GetId(){
	if(m_vertexMessage.size()>0){
		return m_vertexMessage[0].vertexId;
	}
	std::cout<< " error m_vertexMessage.size()=0!\n" << std::endl;
	return -1;
}

std::vector<SVertexProperties> mGPMType::GetProperties(){
	return this->m_vertexMessage;
}
