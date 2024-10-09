#ifndef COLLECTOR_EVENTCOLLECTORM_HPP_
#define COLLECTOR_EVENTCOLLECTORM_HPP_

#include "../dataflow/Vertex.hpp"
#include <fstream>
#include <string>

using namespace std;

class EventCollectorM: public Vertex {

public:

	// Global stats
	long int sum_latency;
	long int sum_counts;
	int num_messages;

	EventCollectorM(int tag, int rank, int worldSize);

	~EventCollectorM();

	void batchProcess();

	void streamProcess(int channel);

private:
	std::ofstream datafile;

};

#endif /* COLLECTOR_EVENTCOLLECTORM_HPP_ */
