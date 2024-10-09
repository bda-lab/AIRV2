#ifndef OPERATOR_QueryAggregatorM_HPP_
#define OPERATOR_QueryAggregatorM_HPP_

#include <unordered_map>
#include <utility>
#include <bits/stdc++.h>
#include "../dataflow/Vertex.hpp"

using namespace std;

// Define inner and outer hashmaps with clickCount, viewCount, and latency directly
typedef unordered_map<long int, std::tuple<long int, long int, long int>> InnerHMap_Q; // Inner hashmap: c_id -> (clickCount, viewCount, latency)
typedef unordered_map<long int, InnerHMap_Q> OuterHMap_Q; // Outer hashmap: WID -> InnerHMap
typedef unordered_map<long int, std::pair<int, int>> WIDtoWrapperUnitHMap; // Wrapper unit map

// Hash function for pairs
struct pair_hash
{
    template <class T1, class T2>
    std::size_t operator()(const std::pair<T1, T2> &pair) const
    {
        return std::hash<T1>()(pair.first) ^ std::hash<T2>()(pair.second);
    }
};

// Slice presence tracking map
typedef unordered_map<std::pair<long int, int>, set<long int>, pair_hash> SlicePresenceMap;

class QueryAggregatorM : public Vertex
{
public:
    OuterHMap_Q WIDtoIHM;                // Outer hashmap to store WID and its inner counts and latency
    pthread_mutex_t WIDtoIHM_mutex;    // Mutex for WIDtoIHM

    WIDtoWrapperUnitHMap WIDtoWrapperUnit; // Wrapper unit map
    pthread_mutex_t WIDtoWrapperUnit_mutex; // Mutex for WIDtoWrapperUnit

    SlicePresenceMap slicePresenceMap;      // Slice presence tracking
    WIDtoWrapperUnitHMap::iterator WIDtoWrapperUnit_it; // Iterator for WIDtoWrapperUnit
    OuterHMap_Q::iterator WIDtoIHM_it;        // Iterator for WIDtoIHM
    InnerHMap_Q::iterator CIDtoCountAndMaxEventTime_it; // Iterator for InnerHMap

    QueryAggregatorM(int tag, int rank, int worldSize);

    ~QueryAggregatorM();

    void batchProcess();

    void streamProcess(int channel);
};

#endif /* OPERATOR_QueryAggregatorM_HPP_ */
