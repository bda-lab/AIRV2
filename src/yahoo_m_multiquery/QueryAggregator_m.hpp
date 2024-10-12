#ifndef OPERATOR_QueryAggregatorM_HPP_
#define OPERATOR_QueryAggregatorM_HPP_

#include <unordered_map>
#include <utility>
#include <set>
#include <pthread.h>
#include "../dataflow/Vertex.hpp"

using namespace std;

// Define types for click, view, and max event time
typedef std::pair<std::pair<long int, long int>, long int> countViewMaxEventTime;  // <ClickCount, ViewCount>, MaxLatency
typedef unordered_map<long int, countViewMaxEventTime> InnerHMapQ;  // Maps c_id to counts and max latency
typedef unordered_map<long int, InnerHMapQ> OuterHMapQ;  // Maps WID to c_id and its corresponding counts and latency
typedef unordered_map<long int, std::pair<int, int>> WIDtoWrapperUnitHMap;  // Maintains additional WID to unit info

// Hash function for pair to be used in unordered_map
struct pair_hash {
    template <class T1, class T2>
    std::size_t operator()(const std::pair<T1, T2>& pair) const {
        return std::hash<T1>()(pair.first) ^ std::hash<T2>()(pair.second);
    }
};

// SlicePresenceMap: Tracks the presence of slice_ids per WID and rank
typedef unordered_map<std::pair<long int, int>, set<long int>, pair_hash> SlicePresenceMap;

class QueryAggregatorM : public Vertex {
public:
    // Maps WID to inner map that contains c_id and <ClickCount, ViewCount, MaxLatency>
    OuterHMapQ WIDtoIHM;
    pthread_mutex_t WIDtoIHM_mutex;  // Mutex to protect WIDtoIHM map
    
    // Tracks WID to Wrapper Unit mappings
    WIDtoWrapperUnitHMap WIDtoWrapperUnit;
    pthread_mutex_t WIDtoWrapperUnit_mutex;  // Mutex to protect WIDtoWrapperUnit map
    
    // Tracks slice presence (slice_id) for each WID and rank
    SlicePresenceMap slicePresenceMap;

    int queries;  // Number of queries to be handled
    
    // Constructor and Destructor
    QueryAggregatorM(int tag, int rank, int worldSize, int q);
    ~QueryAggregatorM();

    // Methods for processing messages
    void batchProcess();
    void streamProcess(int channel);
};

#endif /* OPERATOR_QueryAggregatorM_HPP_ */