#ifndef OPERATOR_QueryAggregator_HPP_
#define OPERATOR_QueryAggregator_HPP_

#include <unordered_map>
#include <utility>
#include <bits/stdc++.h>
#include "../dataflow/Vertex.hpp"

using namespace std;

typedef std::pair<int, long int> count_maxeventtime;
typedef unordered_map<long int, count_maxeventtime> InnerHMap;
typedef unordered_map<long int, unordered_map<long int, count_maxeventtime>> OuterHMap;
typedef unordered_map<long int, std::pair<int, int>> WIDtoWrapperUnitHMap;
// typedef unordered_map<long int, set<long int>> SlicePresenceMap;
// typedef unordered_map<std::pair<long int, int>, set<long int>> SlicePresenceMap;
struct pair_hash
{
    template <class T1, class T2>
    std::size_t operator()(const std::pair<T1, T2> &pair) const
    {
        return std::hash<T1>()(pair.first) ^ std::hash<T2>()(pair.second);
    }
};
typedef unordered_map<std::pair<long int, int>, set<long int>, pair_hash> SlicePresenceMap;

// typedef unordered_map<long int, InnerHMap> WIDToIHM;
class QueryAggregator : public Vertex
{

public:
    OuterHMap WIDtoIHM;
    pthread_mutex_t WIDtoIHM_mutex;
    WIDtoWrapperUnitHMap WIDtoWrapperUnit;
    pthread_mutex_t WIDtoWrapperUnit_mutex;
    SlicePresenceMap slicePresenceMap;
    WIDtoWrapperUnitHMap::iterator WIDtoWrapperUnit_it;
    OuterHMap::iterator WIDtoIHM_it;
    InnerHMap::iterator CIDtoCountAndMaxEventTime_it;
    int queries;
    // std::unordered_map<long int, InnerHMap> WIDtoIHM;

    // WIDToIHM WIDtoIHM;

    QueryAggregator(int tag, int rank, int worldSize, int q);

    ~QueryAggregator();

    void batchProcess();

    void streamProcess(int channel);
};

#endif /* OPERATOR_QueryAggregator_HPP_ */
