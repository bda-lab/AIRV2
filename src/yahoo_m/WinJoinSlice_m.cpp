#include "WinJoinSlice_m.hpp"
#include <mpi.h>
#include <cstring>
#include <iostream>
#include <list>
#include <unistd.h>
#include "../communication/Message.hpp"
#include "../communication/Window.hpp"
#include "../serialization/Serialization.hpp"

using namespace std;

WinJoinSliceM::WinJoinSliceM(int tag, int rank, int worldSize)
    : Vertex(tag, rank, worldSize) {
    pthread_mutex_init(&WIDtoCIDtoCounts_mutex, NULL);
    D(cout << "WINJOINSLICEM [" << tag << "] CREATED @ " << rank << endl;);
}

WinJoinSliceM::~WinJoinSliceM() {
    pthread_mutex_destroy(&WIDtoCIDtoCounts_mutex);
    D(cout << "WINJOINSLICEM [" << tag << "] DELETED @ " << rank << endl;);
}

void WinJoinSliceM::batchProcess() {
    D(cout << "WINJOINSLICEM->BATCHPROCESS [" << tag << "] @ " << rank << endl;);
}

void WinJoinSliceM::streamProcess(int channel) {
    D(cout << "WINJOINSLICEM->STREAMPROCESS [" << tag << "] @ " << rank << " IN-CHANNEL " << channel << endl;);

    Message* inMessage, *outMessage = nullptr, *outMessage1 = nullptr, *outMessage2 = nullptr, *outMessage3 = nullptr;;
    list<Message*>* tmpMessages = new list<Message*>();
    list<long int> completed_windows;
    Serialization sede;

    WrapperUnit wrapper_unit;
    EventPC_m eventPC;
    EventSliceM eventSlice;

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

            D(cout << "WINJOINSLICEM->POP MESSAGE: TAG [" << tag << "] @ " << rank << " BUFFER SIZE " << inMessage->size << endl;);

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
                        D(cout << "WINJOINSLICEM->WID COMPLETE: " << WID << endl;);
                    }
                } else {
                    // If the window ID does not exist, insert it into the map
                    WIDtoWrapperUnit.emplace(WID, make_pair(wrapper_unit.completeness_tag_numerator, wrapper_unit.completeness_tag_denominator));
                }

                pthread_mutex_unlock(&WIDtoWrapperUnit_mutex);

                D(cout << "WINJOINSLICEM->PROCESSING WID: " << WID << " @ " << rank << endl;);

                int offset = sizeof(int) + inMessage->wrapper_length * sizeof(WrapperUnit);
                int event_count = (inMessage->size - offset) / sizeof(EventPC_m);

                D(cout << "WINJOINSLICEM->EVENT COUNT: " << event_count << " FOR WID: " << WID << endl;);

                pthread_mutex_lock(&WIDtoCIDtoCounts_mutex);

                for (int i = 0; i < event_count; ++i) {
                    sede.YSBdeserializePC_m(inMessage, &eventPC, offset + i * sizeof(EventPC_m));

                    long int c_id = eventPC.c_id;
                    D(cout << "WINJOINSLICEM->PROCESSING EVENT WITH c_id: " << c_id << " TYPE: " << eventPC.type << " COUNT: " << eventPC.count << endl;);

                    // Check if WID already exists in the outer hashmap
                    if (WIDtoCIDtoCounts.find(WID) == WIDtoCIDtoCounts.end()) {
                        WIDtoCIDtoCounts[WID] = InnerHMapM();
                        
                        D(cout << "WINJOINSLICEM->NEW WID ENTRY CREATED FOR WID: " << WID << endl;);
                    }

                    // Check if c_id already exists in the inner hashmap
                    if (WIDtoCIDtoCounts[WID].find(c_id) == WIDtoCIDtoCounts[WID].end()) {
                        WIDtoCIDtoCounts[WID][c_id] = make_pair(0, 0); // Initialize both counts as 0
                        
                        D(cout << "WINJOINSLICEM->NEW c_id ENTRY CREATED FOR WID: " << WID << " c_id: " << c_id << endl;);
                    }

                    // Update click or view count based on event type
                    if (eventPC.type == 1) {
                        WIDtoCIDtoCounts[WID][c_id].first += eventPC.count; // Update click count

                        // Update click event time
                        CIDtoEventTime[WID][c_id].first = eventPC.event_time;
                        D(cout << "WINJOINSLICEM->UPDATED CLICK COUNT AND EVENT TIME FOR c_id: " << c_id << endl;);

                    } else if (eventPC.type == 2) {
                        WIDtoCIDtoCounts[WID][c_id].second += eventPC.count; // Update view count

                        // Update view event time
                        CIDtoEventTime[WID][c_id].second = eventPC.event_time;
                        D(cout << "WINJOINSLICEM->UPDATED VIEW COUNT AND EVENT TIME FOR c_id: " << c_id << endl;);
                    }
                }

                pthread_mutex_unlock(&WIDtoCIDtoCounts_mutex);
                delete inMessage;
            }
        }

        // Process all completed windows
        pthread_mutex_lock(&WIDtoCIDtoCounts_mutex);

        long int time_now = (long int)(MPI_Wtime() * 1000.0);
        while (!completed_windows.empty()) {
           
            long int WID = completed_windows.front();
            completed_windows.pop_front();

            D(cout << "WINJOINSLICEM->PROCESSING COMPLETED WINDOW FOR WID: " << WID << endl;);

            InnerHMapM& CIDtoCounts = WIDtoCIDtoCounts[WID];

            // Initialize variables for total count and total latency
            long int total_clicks_views = 0;     // Initialize to accumulate total click + view counts
            int total_latency = 0;         // Initialize to accumulate total latency
            int total_cid_count = 0;             // Initialize to track the number of c_id's
            int avg_latency = 0;

            // Iterate through all c_ids in the window
            int j = 0;
            outMessage = new Message(sizeof(EventSlice) * 100); // Create a message with capacity based on c_ids
            outMessage1 = new Message(sizeof(EventSlice) * 100);
            outMessage2 = new Message(sizeof(EventSlice) * 100);
            outMessage3 = new Message(sizeof(EventSlice) * 100);

            for (auto& cidEntry : CIDtoCounts) {
                long int c_id = cidEntry.first;
                long int clickCount = cidEntry.second.first;
                long int viewCount = cidEntry.second.second;
                j++;

                // If view count is zero, set it to 1 to avoid division by zero
                if (viewCount == 0) {
                    D(cout << "WINJOINSLICEM->MISSING VIEW COUNT FOR WID: " << WID << " c_id: " << c_id << ". SETTING VIEW COUNT TO 1." << endl;);
                    viewCount = 1;
                }

                // Get the event time from both click and view messages
                long int clickEventTime = CIDtoEventTime[WID][c_id].first;  // Get the click event time
                long int viewEventTime = CIDtoEventTime[WID][c_id].second;  // Get the view event time

                // Find the maximum event time between click and view
                long int maxEventTime = std::max(clickEventTime, viewEventTime);

                double ratio = static_cast<double>(clickCount) / viewCount;
                D(cout << "WINJOINSLICEM->CALCULATED RATIO FOR WID: " << WID << " c_id: " << c_id << " RATIO: " << ratio << endl;);

                // Prepare the event to serialize
                eventSlice.slice_id = WID;
                eventSlice.c_id = c_id;
                eventSlice.ClickCount = clickCount;
                eventSlice.ViewCount = viewCount;
                eventSlice.ratio = ratio;
                eventSlice.latency = (time_now - maxEventTime);

                // Print the event details before serialization
                //sede.YSBprintSliceM(&eventSlice);

                // Serialize the event and append to the output message
                sede.YSBserializeSliceM(&eventSlice, outMessage);
                sede.YSBserializeSliceM(&eventSlice, outMessage1);
                sede.YSBserializeSliceM(&eventSlice, outMessage2);
                sede.YSBserializeSliceM(&eventSlice, outMessage3);

                // Update total counts and total latency
                total_clicks_views += clickCount + viewCount;  // Accumulate total events
                total_latency += eventSlice.latency;              // Accumulate total latency
                total_cid_count++;                             // Increment number of processed events
            }

            // Send the outMessage if it contains serialized data
            if (outMessage && outMessage->size > 0) {
                int idx = 0;  // Assuming sending to rank 0, change as per your system's requirements
                pthread_mutex_lock(&senderMutexes[idx]);
                outMessages[idx].push_back(outMessage);
                pthread_cond_signal(&senderCondVars[idx]);
                pthread_mutex_unlock(&senderMutexes[idx]);

                D(cout << "WINJOINSLICEM->SENT MESSAGE FOR WID: " << WID << " WITH SIZE: " << outMessage->size << endl;);
            } 
            if (outMessage1 && outMessage1->size > 0) {
                int idx = 1;  // Assuming sending to rank 1, change as per your system's requirements
                pthread_mutex_lock(&senderMutexes[idx]);
                outMessages[idx].push_back(outMessage1);
                pthread_cond_signal(&senderCondVars[idx]);
                pthread_mutex_unlock(&senderMutexes[idx]);

                D(cout << "WINJOINSLICEM->SENT MESSAGE FOR WID: " << WID << " WITH SIZE: " << outMessage->size << endl;);
            } 
            if (outMessage2 && outMessage2->size > 0) {
                int idx = 2;  // Assuming sending to rank 0, change as per your system's requirements
                pthread_mutex_lock(&senderMutexes[idx]);
                outMessages[idx].push_back(outMessage2);
                pthread_cond_signal(&senderCondVars[idx]);
                pthread_mutex_unlock(&senderMutexes[idx]);

                D(cout << "WINJOINSLICEM->SENT MESSAGE FOR WID: " << WID << " WITH SIZE: " << outMessage->size << endl;);
            } 
            if (outMessage3 && outMessage3->size > 0) {
                int idx = 3;  // Assuming sending to rank 0, change as per your system's requirements
                pthread_mutex_lock(&senderMutexes[idx]);
                outMessages[idx].push_back(outMessage3);
                pthread_cond_signal(&senderCondVars[idx]);
                pthread_mutex_unlock(&senderMutexes[idx]);

                D(cout << "WINJOINSLICEM->SENT MESSAGE FOR WID: " << WID << " WITH SIZE: " << outMessage->size << endl;);
            } 
            else {
                delete outMessage;
                delete outMessage1;
                delete outMessage2;
                delete outMessage3;
            }

            // Clean up the window data
            WIDtoCIDtoCounts.erase(WID);
            D(cout << "WINJOINSLICEM->WID: " << WID << " IS COMPLETED AND REMOVED" << endl;);

            // Print total count and average latency for the WID
            avg_latency = total_latency / total_cid_count;  // Calculate average latency
            //cout << "WID: " << WID << " \tTotal Count: " << total_clicks_views << " \tAverage Latency: " << avg_latency << endl;
        }

        pthread_mutex_unlock(&WIDtoCIDtoCounts_mutex);
    }

    // Final cleanup
    delete tmpMessages;
    D(cout << "WINJOINSLICEM->STREAM PROCESSING COMPLETED @ " << rank << endl;);
}
