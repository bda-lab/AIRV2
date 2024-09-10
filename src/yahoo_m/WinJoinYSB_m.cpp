

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



             
/*

void WinJoinYSBM::streamProcess(int channel) {
    D(cout << "WINJOINYSBM->STREAMPROCESS [" << tag << "] @ " << rank << " IN-CHANNEL " << channel << endl;);

    Message* inMessage, *outMessage;
    list<Message*>* tmpMessages = new list<Message*>();
    list<long int> completed_windows;
    Serialization sede;

    WrapperUnit wrapper_unit;
    EventPC_m eventPC;
    EventPC eventWJ;

    int c = 0;  // Counter for number of iterations

    WIDtoWrapperUnitHMap::iterator WIDtoWrapperUnit_it;  // Declare the iterator here

    while (ALIVE) {  // The ALIVE loop starts here
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

                pthread_mutex_lock(&WIDtoCIDtoCounts_mutex);

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
                        D(cout << "WINJOINYSBM->UPDATED CLICK COUNT FOR c_id: " << c_id << " NEW CLICK COUNT: " << WIDtoCIDtoCounts[WID][c_id].first << endl;);
                    } else if (eventPC.type == 2) {
                        WIDtoCIDtoCounts[WID][c_id].second += eventPC.count; // Update view count
                        D(cout << "WINJOINYSBM->UPDATED VIEW COUNT FOR c_id: " << c_id << " NEW VIEW COUNT: " << WIDtoCIDtoCounts[WID][c_id].second << endl;);
                    }
                }

                pthread_mutex_unlock(&WIDtoCIDtoCounts_mutex);
                delete inMessage;
            }
        }

        // Process all completed windows
        pthread_mutex_lock(&WIDtoCIDtoCounts_mutex);
        while (!completed_windows.empty()) {
           long int time_now = (long int)(MPI_Wtime() * 1000.0); long int WID = completed_windows.front();
            completed_windows.pop_front();

            D(cout << "WINJOINYSBM->PROCESSING COMPLETED WINDOW FOR WID: " << WID << endl;);

            InnerHMapM& CIDtoCounts = WIDtoCIDtoCounts[WID];

            // Iterate through all c_ids in the window
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
             // long int clickEventTime = CIDtoEventTime[WID][c_id].first;  // Assuming CIDtoEventTime stores the event time for click
             // long int viewEventTime = CIDtoEventTime[WID][c_id].second; // Assuming CIDtoEventTime stores the event time for view

              // Find the maximum event time between click and view
             // long int maxEventTime = std::max(clickEventTime, viewEventTime);


                double ratio = static_cast<double>(clickCount) / viewCount;
                D(cout << "WINJOINYSBM->CALCULATED RATIO FOR WID: " << WID << " c_id: " << c_id << " RATIO: " << ratio << endl;);

                // Prepare the output message
                outMessage = new Message(sizeof(EventPC) * 100, 1);
                memcpy(outMessage->buffer + sizeof(int), &wrapper_unit, sizeof(WrapperUnit));
                outMessage->size += sizeof(int) + outMessage->wrapper_length * sizeof(WrapperUnit);

                long int time_now = (long int)(MPI_Wtime() * 1000.0);
                eventWJ.WID = WID;
                eventWJ.c_id = c_id;
                eventWJ.count = clickCount + viewCount;
                eventWJ.latency = (time_now - eventPC.event_time);

                cout << "\tWID: " << eventWJ.WID
                     << "\tc_id: " << eventWJ.c_id
                     << "\tClickCount: " << clickCount
                     << "\tViewCount: " << viewCount
                     << "\tClick/View Ratio: " << ratio
                     << "\tlatency: " << eventWJ.latency << endl;

                // Serialize and send the event
                sede.YSBserializePC(&eventWJ, outMessage);

                // Send message to the collector
                int n = 0;
                for (vector<Vertex*>::iterator v = next.begin(); v != next.end(); ++v) {
                    if (outMessage->size > 0) {
                        int idx = n * worldSize + 0; // Send to rank 0 only
                        pthread_mutex_lock(&senderMutexes[idx]);
                        outMessages[idx].push_back(outMessage);
                        pthread_cond_signal(&senderCondVars[idx]);
                        pthread_mutex_unlock(&senderMutexes[idx]);
                    } else {
                        delete outMessage;
                    }
                    n++;
                    break; // Only one successor node allowed
                }
            }

            // Clean up the window data
            WIDtoCIDtoCounts.erase(WID);
            D(cout << "WINJOINYSBM->WID: " << WID << " IS COMPLETED AND REMOVED" << endl;);
        }

        pthread_mutex_unlock(&WIDtoCIDtoCounts_mutex);
        tmpMessages->clear();
    }  // The ALIVE loop ends here

    delete tmpMessages;
}

*/

void WinJoinYSBM::streamProcess(int channel) {
    D(cout << "WINJOINYSBM->STREAMPROCESS [" << tag << "] @ " << rank << " IN-CHANNEL " << channel << endl;);

    Message* inMessage, *outMessage;
    list<Message*>* tmpMessages = new list<Message*>();
    list<long int> completed_windows;
    Serialization sede;

    WrapperUnit wrapper_unit;
    EventPC_m eventPC;
    EventPC eventWJ;

    int c = 0;  // Counter for number of iterations
    
    WIDtoWrapperUnitHMap::iterator WIDtoWrapperUnit_it;  // Declare the iterator here

    while (ALIVE) { 
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

                pthread_mutex_lock(&WIDtoCIDtoCounts_mutex);
				

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

                pthread_mutex_unlock(&WIDtoCIDtoCounts_mutex);
                delete inMessage;
            }
        }

        // Process all completed windows
        pthread_mutex_lock(&WIDtoCIDtoCounts_mutex);
        while (!completed_windows.empty()) {
            long int time_now = (long int)(MPI_Wtime() * 1000.0);
            long int WID = completed_windows.front();
            completed_windows.pop_front();

            D(cout << "WINJOINYSBM->PROCESSING COMPLETED WINDOW FOR WID: " << WID << endl;);

            InnerHMapM& CIDtoCounts = WIDtoCIDtoCounts[WID];

            // Iterate through all c_ids in the window
			int j=0;
            for (auto& cidEntry : CIDtoCounts) {
                long int c_id = cidEntry.first;
                long int clickCount = cidEntry.second.first;
                long int viewCount = cidEntry.second.second;
                j++;    
                // If view count is zero, set it to 1 to avoid division by zero
                if (viewCount == 0) {
                    D(cout << "WINJOINYSBM->MISSING VIEW COUNT FOR WID: " << WID << " c_id: " << c_id << ". SETTING VIEW COUNT TO 1." << endl;);
                    viewCount = 1;
                }

                // Get the event time from both click and view messages
                long int clickEventTime = CIDtoEventTime[WID][c_id].first;  // Get the click event time
                long int viewEventTime = CIDtoEventTime[WID][c_id].second;  // Get the view event time

                // Find the maximum event time between click and view
                long int maxEventTime = std::max(clickEventTime, viewEventTime);

                double ratio = static_cast<double>(clickCount) / viewCount;
                D(cout << "WINJOINYSBM->CALCULATED RATIO FOR WID: " << WID << " c_id: " << c_id << " RATIO: " << ratio << endl;);

                // Prepare the output message
                outMessage = new Message(sizeof(EventPC) * 100, 1);
                memcpy(outMessage->buffer + sizeof(int), &wrapper_unit, sizeof(WrapperUnit));
                outMessage->size += sizeof(int) + outMessage->wrapper_length * sizeof(WrapperUnit);

                // Calculate the latency using the max event time
                eventWJ.WID = WID;
                eventWJ.c_id = c_id;
                eventWJ.count = clickCount + viewCount;
                eventWJ.latency = (time_now - maxEventTime);

                cout << "  " << j << "\tWID: " << eventWJ.WID
                     << "\tc_id: " << eventWJ.c_id
                     << "\tClickCount: " << clickCount
                     << "\tViewCount: " << viewCount
                     << "\tClick/View Ratio: " << ratio
                     << "\tLatency: " << eventWJ.latency << endl;

                // Serialize and send the event
                sede.YSBserializePC(&eventWJ, outMessage);

                // Send message to the collector
                int n = 0;
                for (vector<Vertex*>::iterator v = next.begin(); v != next.end(); ++v) {
                    if (outMessage->size > 0) {
                        int idx = n * worldSize + 0; // Send to rank 0 only
                        pthread_mutex_lock(&senderMutexes[idx]);
                        outMessages[idx].push_back(outMessage);
                        pthread_cond_signal(&senderCondVars[idx]);
                        pthread_mutex_unlock(&senderMutexes[idx]);
                    } else {
                        delete outMessage;
                    }
                    n++;
                    break; // Only one successor node allowed
                }
            }

            // Clean up the window data
            WIDtoCIDtoCounts.erase(WID);
            D(cout << "WINJOINYSBM->WID: " << WID << " IS COMPLETED AND REMOVED" << endl;);
        }

        pthread_mutex_unlock(&WIDtoCIDtoCounts_mutex);
        tmpMessages->clear();
    }  // The ALIVE loop ends here

    delete tmpMessages;
}
