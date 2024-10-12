#ifndef COLLECTOR_QueryCollectorM_HPP_
#define COLLECTOR_QueryCollectorM_HPP_

#include "../dataflow/Vertex.hpp"
#include <fstream>
#include <string>

using namespace std;

class QueryCollectorM : public Vertex
{

public:
    // Global stats
    long int sum_latency;
    long int sum_counts;
    int num_messages;

    QueryCollectorM(int tag, int rank, int worldSize);

    ~QueryCollectorM();

    void batchProcess();

    void streamProcess(int channel);

private:
    std::ofstream datafile;
};

#endif /* COLLECTOR_QueryCollector_HPP_ */
