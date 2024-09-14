#include <iostream>
#include <unordered_map>
#include "../serialization/Serialization.hpp"
#include "EventCollector_m.hpp"
#include <list>

using namespace std;

EventCollectorM::EventCollectorM(int tag, int rank, int worldSize) : Vertex(tag, rank, worldSize) {
    D(cout << "EventCollectorM [" << tag << "] CREATED @ " << rank << endl;)
}

EventCollectorM::~EventCollectorM() {
    D(cout << "EventCollectorM [" << tag << "] DELETED @ " << rank << endl;)
}

void EventCollectorM::batchProcess() {
    D(cout << "EventCollectorM->BATCHPROCESS [" << tag << "] @ " << rank << endl;)
}

void EventCollectorM::streamProcess(int channel) {
    D(cout << "EventCollectorM->STREAMPROCESS [" << tag << "] @ " << rank
           << " IN-CHANNEL " << channel << endl;)

    if (rank == 0) {
        Message* inMessage;
        list<Message*>* tmpMessages = new list<Message*>();
        Serialization sede;
        
        // Hashmap to store the sum of latencies and counts for each WID
        unordered_map<long int, pair<long int, long int>> widLatencyCountMap;

        EventWJ eventWJ;
        int message_count = 0;  // Track number of messages

        while (ALIVE) {
            pthread_mutex_lock(&listenerMutexes[channel]);

            while (inMessages[channel].empty()) {
                pthread_cond_wait(&listenerCondVars[channel], &listenerMutexes[channel]);
            }

            while (!inMessages[channel].empty()) {
                inMessage = inMessages[channel].front();
                inMessages[channel].pop_front();
                tmpMessages->push_back(inMessage);
            }

            pthread_mutex_unlock(&listenerMutexes[channel]);

            while (!tmpMessages->empty()) {
                inMessage = tmpMessages->front();
                tmpMessages->pop_front();

                D(cout << "EventCollectorM->POP MESSAGE: TAG [" << tag << "] @ " << rank
                       << " BUFFER SIZE " << inMessage->size << endl;)

                int event_count = inMessage->size / sizeof(EventWJ);
                int i = 0;

                // Reset the hashmap for each message
                widLatencyCountMap.clear();
                long int total_clicks_views = 0;

                // First pass: accumulate latencies and counts for each WID
                while (i < event_count) {
                    sede.YSBdeserializeWJ(inMessage, &eventWJ, i * sizeof(EventWJ));

                    // Accumulate latency and count for the current WID
                    widLatencyCountMap[eventWJ.WID].first += eventWJ.latency;
                    widLatencyCountMap[eventWJ.WID].second++;  // Increment the event count for WID

                    // Accumulate clicks and views for total count
                    total_clicks_views += eventWJ.ClickCount + eventWJ.ViewCount;

                    // Print event details
                    cout << i << "\tWID: " << eventWJ.WID
                         << "\tc_id: " << eventWJ.c_id
                         << "\tClickCount: " << eventWJ.ClickCount
                         << "\tViewCount: " << eventWJ.ViewCount
                         << "\tClick/View Ratio: " << eventWJ.ratio
                         << "\tLatency: " << eventWJ.latency << " ms" << endl;

                    i++;
                }

                // Second pass: calculate and print the average latency for each WID
                for (const auto& pair : widLatencyCountMap) {
                    long int WID = pair.first;
                    long int total_latency = pair.second.first;
                    long int event_count_per_WID = pair.second.second;

                    // Calculate the average latency for the WID
                    double avg_latency_per_WID = (event_count_per_WID > 0) ? static_cast<double>(total_latency) / event_count_per_WID : 0;
                    cout << "WID: " << WID << " - Average Latency: " << avg_latency_per_WID << " ms over " << event_count_per_WID << " events." << endl;
                }

                // Print message-level summary
                message_count++;
                cout << "Message #" << message_count
                     << " - Total Clicks+Views: " << total_clicks_views << endl;

                delete inMessage;
            }

            tmpMessages->clear();
        }

        delete tmpMessages;
    }
}
