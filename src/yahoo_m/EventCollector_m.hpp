#pragma once

#include "../dataflow/Vertex.hpp"
#include "../serialization/Serialization.hpp"
#include <unordered_map>
#include <list>

class EventCollectorM : public Vertex {
public:
    EventCollectorM(int tag, int rank, int worldSize);
    ~EventCollectorM();

    void batchProcess();    // Override the batch processing method
    void streamProcess(int channel);    // Method to process the stream of events

private:
    // Hashmap to store the sum of latencies and event counts for each WID
    std::unordered_map<long int, std::pair<long int, long int>> widLatencyCountMap;

    // Helper function to reset the latency and count map
    void resetLatencyCountMap();

};
