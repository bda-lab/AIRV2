#include <mpi.h>
// #include <mpi_time.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include "../serialization/Serialization.hpp"
#include "QueryCollector_m.hpp"

using namespace std;

QueryCollectorM::QueryCollectorM(int tag, int rank, int worldSize) : Vertex(tag, rank, worldSize)
{

	// Global stats
	sum_latency = 0;
	sum_counts = 0;
	num_messages = 0;

	S_CHECK(if (rank == 0) {
		datafile.open("Data/results" + to_string(rank) + ".tsv");
	})

	D(std::cout << "QueryCollectorM [" << tag << "] CREATED @ " << rank << endl;)
}

QueryCollectorM::~QueryCollectorM()
{
	D(std::cout << "QueryCollector [" << tag << "] DELETED @ " << rank << endl;)
}

void QueryCollectorM::batchProcess()
{
	D(std::cout << "QueryCollector->BATCHPROCESS [" << tag << "] @ " << rank << endl;)
}

void QueryCollectorM::streamProcess(int channel)
{

	D(std::cout << "QueryCollector->STREAMPROCESS [" << tag << "] @ " << rank
				<< " IN-CHANNEL " << channel << endl;);

	Message *inMessage;
	list<Message *> *tmpMessages = new list<Message *>();
	Serialization sede;

	EventWJ eventWJ;

	int c = 0;
	while (ALIVE)
	{

		pthread_mutex_lock(&listenerMutexes[channel]);

		while (inMessages[channel].empty())
			pthread_cond_wait(&listenerCondVars[channel],
							  &listenerMutexes[channel]);

		while (!inMessages[channel].empty())
		{
			inMessage = inMessages[channel].front();
			inMessages[channel].pop_front();
			tmpMessages->push_back(inMessage);
		}

		pthread_mutex_unlock(&listenerMutexes[channel]);

		while (!tmpMessages->empty())
		{

			inMessage = tmpMessages->front();
			tmpMessages->pop_front();

			// std::cout << "QueryCollector->POP MESSAGE: TAG [" << tag << "] @ "
			// 		  << rank << " CHANNEL " << channel << " BUFFER "
			// 		  << inMessage->size << endl;

			int event_count = inMessage->size / sizeof(EventWJ);
			// std::cout << "EVENT_COUNT: " << event_count << endl;

			int i = 0, click_count = 0, view_count = 0;
			std::cout << "*********************************************QUERY " << rank + 1 << "*********************************************" << endl;
			while (i < event_count)
			{
				sede.YSBdeserializeWJ(inMessage, &eventWJ,
									  i * sizeof(EventWJ));
				long int time_now = (long int)(MPI_Wtime() * 1000.0);

				sum_latency += (time_now - eventWJ.latency);
				click_count += eventWJ.ClickCount;
                view_count += eventWJ.ViewCount;
				//					sede.YSBprintPC(&eventPC);
				S_CHECK(
					datafile
						<< eventWJ.WID << "\t"
						<< eventWJ.c_id << "\t"
						<< eventWJ.ClickCount << "\t"
                        << eventWJ.ViewCount
						<< endl;

				)
				std::cout << "WID: " << eventWJ.WID << "\tQuery " << rank + 1 << "\t" << "\tc_id: " << eventWJ.c_id << "\tClickCount: " << eventWJ.ClickCount << "\tViewCount: " << eventWJ.ViewCount << "\tmax_event_time: " << eventWJ.latency << endl;

				i++;
			}
			sum_counts += event_count; // count of distinct c_id's processed
			num_messages++;

			std::cout << "\n  #" << num_messages << "\tQuery " << (rank + 1) << " Total (Click + View) Count: " << click_count + view_count
					  << "\tAVG_LATENCY: " << ((double)sum_latency / sum_counts) / 1000.0 << "\tGlobal Sum Counts: " << sum_counts << "\tGlobal Sum Latency: " << sum_latency << "\tN=" << event_count << "\n"
					  << endl;

			delete inMessage; // delete message from incoming queue
			c++;
		}

		tmpMessages->clear();
	}

	delete tmpMessages;
}
