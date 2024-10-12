#ifndef OPERATOR_SliceAggregator_HPP_
#define OPERATOR_SliceAggregator_HPP_

#include <unordered_map>
#include <utility>

#include "../dataflow/Vertex.hpp"

using namespace std;

typedef std::pair<int, long int> count_maxeventtime;
typedef unordered_map<long int, count_maxeventtime> InnerHMap;
typedef unordered_map<long int, unordered_map<long int, count_maxeventtime>> OuterHMap;
typedef unordered_map<long int, std::pair<int, int>> WIDtoWrapperUnitHMap;

class SliceAggregator : public Vertex
{

public:
	OuterHMap WIDtoIHM;
	pthread_mutex_t WIDtoIHM_mutex;

	WIDtoWrapperUnitHMap WIDtoWrapperUnit;
	pthread_mutex_t WIDtoWrapperUnit_mutex;
	int queries;
	SliceAggregator(int tag, int rank, int worldSize, int q);

	~SliceAggregator();

	void batchProcess();

	void streamProcess(int channel);
};

#endif /* OPERATOR_SliceAggregator_HPP_ */
