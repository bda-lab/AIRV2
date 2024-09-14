
#include <unistd.h>
#include "../serialization/Serialization.hpp"
#include "EventCollector_m.hpp"


using namespace std;

EventCollectorM::EventCollectorM(int tag, int rank, int worldSize) :
		Vertex(tag, rank, worldSize) {

	// Global stats
	sum_latency = 0;
	sum_counts = 0;
	num_messages = 0;

	S_CHECK(if (rank == 0) {
				datafile.open("Data/results"+to_string(rank)+".tsv");
	})

	D(cout << "EVENTCOLLECTOR [" << tag << "] CREATED @ " << rank << endl;)
}

EventCollectorM::~EventCollectorM() {
	D(cout << "EVENTCOLLECTOR [" << tag << "] DELETED @ " << rank << endl;)
}

void EventCollectorM::batchProcess() {
	D(cout << "EVENTCOLLECTOR->BATCHPROCESS [" << tag << "] @ " << rank << endl;)
}

void EventCollectorM::streamProcess(int channel) {


	D(cout << "EVENTCOLLECTOR->STREAMPROCESS [" << tag << "] @ " << rank
			<< " IN-CHANNEL " << channel << endl;)

	if (rank == 0) {

		Message* inMessage;
		list<Message*>* tmpMessages = new list<Message*>();
		Serialization sede;

		EventWJ eventWJ;

		int c = 0;
		while (ALIVE) {

			pthread_mutex_lock(&listenerMutexes[channel]);

			while (inMessages[channel].empty())
				pthread_cond_wait(&listenerCondVars[channel],
						&listenerMutexes[channel]);

			while (!inMessages[channel].empty()) {
				inMessage = inMessages[channel].front();
				inMessages[channel].pop_front();
				tmpMessages->push_back(inMessage);
			}

			pthread_mutex_unlock(&listenerMutexes[channel]);

			while (!tmpMessages->empty()) {

				inMessage = tmpMessages->front();
				tmpMessages->pop_front();

				cout << "EVENTCOLLECTOR->POP MESSAGE: TAG [" << tag << "] @ "
						<< rank << " CHANNEL " << channel << " BUFFER "
						<< inMessage->size << endl;

				int event_count = inMessage->size / sizeof(EventWJ);
			cout << "EVENT_COUNT: " << event_count << endl;

				int i = 0, count = 0;
				while (i < event_count) {
					sede.YSBdeserializeWJ(inMessage, &eventWJ,
							i * sizeof(EventWJ));
					sum_latency += eventWJ.latency;
					count += eventWJ.ClickCount + eventWJ.ViewCount;
//					sede.YSBprintPC(&eventPC);
					S_CHECK(
										datafile
												<< eventWJ.WID << "\t"
												<< eventWJ.c_id << "\t"
										    
												<<endl;

									)
					i++;
				}
				sum_counts += event_count; // count of distinct c_id's processed
				num_messages++;

				cout << "\n  #" << num_messages <<"\tWID"<<eventWJ.WID <<" COUNT: " << count
						<< "\tAVG_LATENCY: " << (sum_latency / sum_counts)
						<< "\tN=" << event_count << "\n" << endl;


				delete inMessage; // delete message from incoming queue
				c++;
			}

			tmpMessages->clear();
		}

		delete tmpMessages;
	}
}

