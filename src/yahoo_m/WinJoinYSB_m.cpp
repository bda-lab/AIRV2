/*
 * FullAggregator.cpp
 *
 *  Created on: 17, Dec, 2018
 *      Author: vinu.venugopal
 */

#include "WinJoinYSB_m.hpp"
#include <mpi.h>
#include <cstring>
#include <iostream>
#include <list>
#include <unistd.h>
#include "../communication/Message.hpp"
#include "../communication/Window.hpp"
#include "../serialization/Serialization.hpp"

using namespace std;

WinJoinYSBM::WinJoinYSBM(int tag, int rank, int worldSize)
    : Vertex(tag, rank, worldSize) {
    pthread_mutex_init(&WIDtoCIDtoCounts_mutex, NULL);
    D(cout << "WINJOINYSBM [" << tag << "] CREATED @ " << rank << endl;);
}

WinJoinYSBM::~WinJoinYSBM() {
    pthread_mutex_destroy(&WIDtoCIDtoCounts_mutex);
    D(cout << "WINJOINYSBM [" << tag << "] DELETED @ " << rank << endl;);
}

void WinJoinYSBM::batchProcess() {
    D(cout << "WINJOINYSBM->BATCHPROCESS [" << tag << "] @ " << rank << endl;);
}

void WinJoinYSBM::streamProcess(int channel) {
    D(cout << "WINJOINYSBM->STREAMPROCESS [" << tag << "] @ " << rank << " IN-CHANNEL " << channel << endl;);

    Message* inMessage, *outMessage;
    list<Message*>* tmpMessages = new list<Message*>();
    list<long int> completed_windows;
    Serialization sede;

    WrapperUnit wrapper_unit;
    EventPC_m eventPC;
    EventWJ eventWJ;

    int c = 0;  // Counter for number of iterations
    
    WIDtoWrapperUnitHMap::iterator WIDtoWrapperUnit_it;  // Declare the iterator here

    while (ALIVE) { 
        int flag = 0;
        int j = 0;
        // The ALIVE loop starts here
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

            D(cout << "WINJOINYSBM->POP MESSAGE: TAG [" << tag << "] @ " << rank << " BUFFER SIZE " << inMessage->size << endl;);

            // Deserialize wrapper and event
            sede.unwrap(inMessage);

            if (inMessage->wrapper_length > 0) {
                sede.unwrapFirstWU(inMessage, &wrapper_unit);
                long int WID = wrapper_unit.window_start_time;

                pthread_mutex_lock(&WIDtoWrapperUnit_mutex);

                // Check if WID already exists in the completeness map
                if ((WIDtoWrapperUnit_it = WIDtoWrapperUnit.find(WID)) != WIDtoWrapperUnit.end()) {
                    WIDtoWrapperUnit_it->second.first += wrapper_unit.completeness_tag_numerator;

                    // Check if the window is complete
                    if (WIDtoWrapperUnit_it->second.first == WIDtoWrapperUnit_it->second.second) {
                        completed_windows.push_back(WID);
                        WIDtoWrapperUnit.erase(WID);  // Erase completed window entry
                        D(cout << "WINJOINYSBM->WID COMPLETE: " << WID << endl;);
                    }
                } else {
                    // If the window ID does not exist, insert it into the map
                    WIDtoWrapperUnit.emplace(WID, make_pair(wrapper_unit.completeness_tag_numerator, wrapper_unit.completeness_tag_denominator));
                }

                pthread_mutex_unlock(&WIDtoWrapperUnit_mutex);

                D(cout << "WINJOINYSBM->PROCESSING WID: " << WID << " @ " << rank << endl;);

                int offset = sizeof(int) + inMessage->wrapper_length * sizeof(WrapperUnit);
                int event_count = (inMessage->size - offset) / sizeof(EventPC_m);

                D(cout << "WINJOINYSBM->EVENT COUNT: " << event_count << " FOR WID: " << WID << endl;);

                pthread_mutex_lock(&WIDtoCIDtoCounts_mutex); // Lock while modifying WIDtoCIDtoCounts map

                for (int i = 0; i < event_count; ++i) {
                    sede.YSBdeserializePC_m(inMessage, &eventPC, offset + i * sizeof(EventPC_m));

                    long int c_id = eventPC.c_id;
                    D(cout << "WINJOINYSBM->PROCESSING EVENT WITH c_id: " << c_id << " TYPE: " << eventPC.type << " COUNT: " << eventPC.count << endl;);

                    // Check if WID already exists in the outer hashmap
                    if (WIDtoCIDtoCounts.find(WID) == WIDtoCIDtoCounts.end()) {
                        WIDtoCIDtoCounts[WID] = InnerHMapM();
                        D(cout << "WINJOINYSBM->NEW WID ENTRY CREATED FOR WID: " << WID << endl;);
                    }

                    // Check if c_id already exists in the inner hashmap
                    if (WIDtoCIDtoCounts[WID].find(c_id) == WIDtoCIDtoCounts[WID].end()) {
                        WIDtoCIDtoCounts[WID][c_id] = make_pair(0, 0); // Initialize both counts as 0
                        D(cout << "WINJOINYSBM->NEW c_id ENTRY CREATED FOR WID: " << WID << " c_id: " << c_id << endl;);
                    }

                    // Update click or view count based on event type
                    if (eventPC.type == 1) {
                        WIDtoCIDtoCounts[WID][c_id].first += eventPC.count; // Update click count

                        // Update click event time
                        CIDtoEventTime[WID][c_id].first = eventPC.event_time;
                        D(cout << "WINJOINYSBM->UPDATED CLICK COUNT AND EVENT TIME FOR c_id: " << c_id << endl;);
                    } else if (eventPC.type == 2) {
                        WIDtoCIDtoCounts[WID][c_id].second += eventPC.count; // Update view count

                        // Update view event time
                        CIDtoEventTime[WID][c_id].second = eventPC.event_time;
                        D(cout << "WINJOINYSBM->UPDATED VIEW COUNT AND EVENT TIME FOR c_id: " << c_id << endl;);
                    }
                }

                // Don't unlock yet, move unlock to after processing completed windows

            }
        }

        // Process all completed windows while the WIDtoCIDtoCounts_mutex is locked
        long int time_now = (long int)(MPI_Wtime() * 1000.0);
        while (!completed_windows.empty()) {
            long int WID = completed_windows.front();
            completed_windows.pop_front();

            D(cout << "WINJOINYSBM->PROCESSING COMPLETED WINDOW FOR WID: " << WID << endl;);

            InnerHMapM& CIDtoCounts = WIDtoCIDtoCounts[WID];

            // Create a single outMessage for this WID
            outMessage = new Message(sizeof(EventWJ) * CIDtoCounts.size()); // Create one message to store all events for this WID

            // Iterate through all c_ids in the window and serialize them into the outMessage
            for (auto& cidEntry : CIDtoCounts) {
                long int c_id = cidEntry.first;
                long int clickCount = cidEntry.second.first;
                long int viewCount = cidEntry.second.second;

                // If view count is zero, set it to 1 to avoid division by zero
                if (viewCount == 0) {
                    D(cout << "WINJOINYSBM->MISSING VIEW COUNT FOR WID: " << WID << " c_id: " << c_id << ". SETTING VIEW COUNT TO 1." << endl;);
                    viewCount = 1;
                }

                // Get the event time from both click and view messages
                long int clickEventTime = CIDtoEventTime[WID][c_id].first;
                long int viewEventTime = CIDtoEventTime[WID][c_id].second;

                // Find the maximum event time between click and view
                long int maxEventTime = std::max(clickEventTime, viewEventTime);

                double ratio = static_cast<double>(clickCount) / viewCount;
                D(cout << "WINJOINYSBM->CALCULATED RATIO FOR WID: " << WID << " c_id: " << c_id << " RATIO: " << ratio << endl;);

                // Prepare the EventWJ struct for the current event
                eventWJ.WID = WID;
                eventWJ.c_id = c_id;
                eventWJ.ClickCount = clickCount;
                eventWJ.ViewCount = viewCount;
                eventWJ.ratio = ratio;
                eventWJ.latency = (time_now - maxEventTime);

              cout << "WID: " << eventWJ.WID << "@rank" << rank
                     << "\tc_id: " << eventWJ.c_id
                     << "\tClickCount: " << eventWJ.ClickCount
                     << "\tViewCount: " << eventWJ.ViewCount
                     << "\tClick/View Ratio: " << eventWJ.ratio
                     << "\tLatency: " << eventWJ.latency << " ms" << endl;

                // Serialize the event into the single outMessage
                sede.YSBserializeWJ(&eventWJ, outMessage);
            }

            // After all events are serialized into outMessage, send the message
            int n = 0;
            for (vector<Vertex*>::iterator v = next.begin(); v != next.end(); ++v) {
                if (outMessage->size > 0) {
                    int idx = n * worldSize + 0;  // Send to rank 0 only
                    pthread_mutex_lock(&senderMutexes[idx]);
                    outMessages[idx].push_back(outMessage);
                    pthread_cond_signal(&senderCondVars[idx]);
                    pthread_mutex_unlock(&senderMutexes[idx]);
                } else {
                    delete outMessage;
                }
                n++;
                break;  // Only one successor node allowed
            }

            // Clean up the window data after processing
            WIDtoCIDtoCounts.erase(WID);
            D(cout << "WINJOINYSBM->WID: " << WID << " IS COMPLETED AND REMOVED" << endl;);
        }

        pthread_mutex_unlock(&WIDtoCIDtoCounts_mutex); // Unlock after completed windows are processed

        tmpMessages->clear();

        delete inMessage; // Move delete inMessage here, after processing the message
    }

    delete tmpMessages; // Delete temporary message list at the end
}
