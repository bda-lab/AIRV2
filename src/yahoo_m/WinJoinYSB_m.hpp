
/*


#ifndef OPERATOR_WINJOINYSBM_HPP_
#define OPERATOR_WINJOINYSBM_HPP_

#include <unordered_map>
#include <utility>
#include "../dataflow/Vertex.hpp"

using namespace std;

// Typedefs for handling event and window data
typedef std::pair<long int, long int> counts_click_view_pair;  // Pair of click and view counts
typedef unordered_map<long int, counts_click_view_pair> InnerHMapM;  // Inner hashmap for c_id -> (click_count, view_count)
typedef unordered_map<long int, InnerHMapM> OuterHMapM;  // Outer hashmap for WID -> c_id -> (click_count, view_count)
typedef unordered_map<long int, std::pair<int, int>> WIDtoWrapperUnitHMap;  // WID -> (completeness numerator, denominator) for window completeness
// In the WinJoinYSBM class (WinJoinYSBM.hpp or appropriate header file)

typedef std::pair<long int, long int> EventTimePair;  // (clickEventTime, viewEventTime)
std::unordered_map<long int, std::unordered_map<long int, EventTimePair>> CIDtoEventTime;  // WID -> c_id -> (clickEventTime, viewEventTime)


class WinJoinYSBM : public Vertex {
public:
    OuterHMapM WIDtoCIDtoCounts;  // Hashmap for WID -> c_id -> click/view counts
    WIDtoWrapperUnitHMap WIDtoWrapperUnit;  // Hashmap for window completeness

    // Mutexes for thread safety
    pthread_mutex_t WIDtoCIDtoCounts_mutex;  // Mutex for the outer hashmap
    pthread_mutex_t WIDtoWrapperUnit_mutex;  // Mutex for window completeness hashmap

    // Constructor and Destructor
    WinJoinYSBM(int tag, int rank, int worldSize);
    ~WinJoinYSBM();

    // Core methods for batch and stream processing
    void batchProcess();
    void streamProcess(int channel);

};

#endif   */ /* OPERATOR_WINJOINYSBM_HPP_ */

#ifndef OPERATOR_WINJOINYSBM_HPP_
#define OPERATOR_WINJOINYSBM_HPP_

#include <unordered_map>
#include <utility>
#include "../dataflow/Vertex.hpp"

using namespace std;

// Typedefs for handling event and window data
typedef std::pair<long int, long int> counts_click_view_pair;  // Pair of click and view counts
typedef unordered_map<long int, counts_click_view_pair> InnerHMapM;  // Inner hashmap for c_id -> (click_count, view_count)
typedef unordered_map<long int, InnerHMapM> OuterHMapM;  // Outer hashmap for WID -> c_id -> (click_count, view_count)
typedef unordered_map<long int, std::pair<int, int>> WIDtoWrapperUnitHMap;  // WID -> (completeness numerator, denominator) for window completeness

// Typedef for storing event times
typedef std::pair<long int, long int> EventTimePair;  // (clickEventTime, viewEventTime)

class WinJoinYSBM : public Vertex {
public:
    OuterHMapM WIDtoCIDtoCounts;  // Hashmap for WID -> c_id -> click/view counts
    WIDtoWrapperUnitHMap WIDtoWrapperUnit;  // Hashmap for window completeness

    // New hashmap to store click and view event times for each WID and c_id
    unordered_map<long int, unordered_map<long int, EventTimePair>> CIDtoEventTime;

    // Mutexes for thread safety
    pthread_mutex_t WIDtoCIDtoCounts_mutex;  // Mutex for the outer hashmap
    pthread_mutex_t WIDtoWrapperUnit_mutex;  // Mutex for window completeness hashmap

    // Constructor and Destructor
    WinJoinYSBM(int tag, int rank, int worldSize);
    ~WinJoinYSBM();

    // Core methods for batch and stream processing
    void batchProcess();
    void streamProcess(int channel);

};

#endif /* OPERATOR_WINJOINYSBM_HPP_ */
