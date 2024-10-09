#include <mpi.h>
// #include <mpi_time.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include "../serialization/Serialization.hpp"
#include "EventCollector_m.hpp"

using namespace std;

EventCollectorM::EventCollectorM(int tag, int rank, int worldSize) : Vertex(tag, rank, worldSize)
{

	// Global stats
	sum_latency = 0;
	sum_counts = 0;
	num_messages = 0;

	S_CHECK(if (rank == 0) {
		datafile.open("Data/results" + to_string(rank) + ".tsv");
	})

	D(cout << "EVENTCOLLECTOR [" << tag << "] CREATED @ " << rank << endl;)
}

EventCollectorM::~EventCollectorM()
{
	D(cout << "EVENTCOLLECTOR [" << tag << "] DELETED @ " << rank << endl;)
}

void EventCollectorM::batchProcess()
{
	D(cout << "EVENTCOLLECTOR->BATCHPROCESS [" << tag << "] @ " << rank << endl;)
}

void EventCollectorM::streamProcess(int channel)
{

	D(cout << "EVENTCOLLECTOR->STREAMPROCESS [" << tag << "] @ " << rank
		   << " IN-CHANNEL " << channel << endl;)

	if (rank == 0)
	{

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

				D(cout << "EVENTCOLLECTOR->POP MESSAGE: TAG [" << tag << "] @ "
					   << rank << " CHANNEL " << channel << " BUFFER "
					   << inMessage->size << endl;)

				int event_count = inMessage->size / sizeof(EventWJ);

				int i = 0, click_count = 0, view_count = 0;
				while (i < event_count)
				{
					sede.YSBdeserializeWJ(inMessage, &eventWJ,
										  i * sizeof(EventWJ));
					long int time_now = (long int)(MPI_Wtime() * 1000.0);

					sum_latency += (time_now - eventWJ.latency);
					click_count += eventWJ.ClickCount;
					view_count += eventWJ.ViewCount;

					S_CHECK(
						datafile
							<< eventWJ.WID << "\t"
							<< eventWJ.c_id << "\t"
							<< eventWJ.ratio
							<< endl;

					)
					cout << "WID: " << eventWJ.WID << "\tc_id: " << eventWJ.c_id << "\tClickCount: " << eventWJ.ClickCount << "\tViewCount: " << eventWJ.ViewCount << "\tRatio: " << eventWJ.ratio<< "\tLatency: " << eventWJ.latency << endl;
					i++;
				}
				sum_counts += event_count; // count of distinct c_id's processed
				num_messages++;

				cout << "# " << num_messages << " Total (Click + View) Count: " << click_count + view_count
					 << "\tAverage Latency: " << ((double)sum_latency / sum_counts) / 1000.0 << "\tGlobal Sum Counts: " << sum_counts << "\tGlobal Sum Latency: " << sum_latency << "\tN=" << event_count << "\n"
					 << endl;

				delete inMessage; // delete message from incoming queue
				c++;
			}

			tmpMessages->clear();
		}

		delete tmpMessages;
	}
}