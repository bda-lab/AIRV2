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

WinJoinSliceM::WinJoinSliceM(int tag, int rank, int worldSize, int q)
	: Vertex(tag, rank, worldSize)
{
    queries = q;
	pthread_mutex_init(&WIDtoCIDtoCounts_mutex, NULL);
	D(cout << "WINJOINYSBM [" << tag << "] CREATED @ " << rank << endl;);
}

WinJoinSliceM::~WinJoinSliceM()
{
	pthread_mutex_destroy(&WIDtoCIDtoCounts_mutex);
	D(cout << "WINJOINYSBM [" << tag << "] DELETED @ " << rank << endl;);
}

void WinJoinSliceM::batchProcess()
{
	D(cout << "WINJOINYSBM->BATCHPROCESS [" << tag << "] @ " << rank << endl;);
}

void WinJoinSliceM::streamProcess(int channel)
{
	D(cout << "WINJOINYSBM->STREAMPROCESS [" << tag << "] @ " << rank << " IN-CHANNEL " << channel << endl;);

	Message *inMessage;
    Message *outMessage[queries];
	list<Message *> *tmpMessages = new list<Message *>();
	list<long int> completed_windows;
	Serialization sede;

	WrapperUnit wrapper_unit;
	EventPC_m eventPC;
	EventSliceM eventSlice;

	int c = 0; // Counter for number of iterations

	WIDtoWrapperUnitHMap::iterator WIDtoWrapperUnit_it; // Declare the iterator here

	while (ALIVE)
	{
		int flag = 0;
		int j = 0;
		// The ALIVE loop starts here
		pthread_mutex_lock(&listenerMutexes[channel]);

		while (inMessages[channel].empty())
		{
			pthread_cond_wait(&listenerCondVars[channel], &listenerMutexes[channel]);
		}

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

			D(cout << "WINJOINYSBM->POP MESSAGE: TAG [" << tag << "] @ " << rank << " BUFFER SIZE " << inMessage->size << endl;);

			// Deserialize wrapper and event
			sede.unwrap(inMessage);

			if (inMessage->wrapper_length > 0)
			{
				sede.unwrapFirstWU(inMessage, &wrapper_unit);
				long int WID = wrapper_unit.window_start_time;

				pthread_mutex_lock(&WIDtoWrapperUnit_mutex);

				// Check if WID already exists in the completeness map
				if ((WIDtoWrapperUnit_it = WIDtoWrapperUnit.find(WID)) != WIDtoWrapperUnit.end())
				{
					WIDtoWrapperUnit_it->second.first += wrapper_unit.completeness_tag_numerator;

					// Check if the window is complete
					if (WIDtoWrapperUnit_it->second.first == WIDtoWrapperUnit_it->second.second)
					{
						completed_windows.push_back(WID);
						WIDtoWrapperUnit.erase(WID); // Erase completed window entry
						D(cout << "WINJOINYSBM->WID COMPLETE: " << WID << endl;);
					}
				}
				else
				{
					// If the window ID does not exist, insert it into the map
					WIDtoWrapperUnit.emplace(WID, make_pair(wrapper_unit.completeness_tag_numerator, wrapper_unit.completeness_tag_denominator));
				}

				pthread_mutex_unlock(&WIDtoWrapperUnit_mutex);

				D(cout << "WINJOINYSBM->PROCESSING WID: " << WID << " @ " << rank << endl;);

				int offset = sizeof(int) + inMessage->wrapper_length * sizeof(WrapperUnit);
				int event_count = (inMessage->size - offset) / sizeof(EventPC_m);
                
                for (int i = 0; i < queries; i++) {
                    outMessage[i] = new Message(sizeof(EventSliceM) * 100); // Create new message with max. required capacity
                    //cout << "WINJOINYSBM->Initialized outMessage for Query: " << i << " with size: " << outMessage[i]->capacity << endl;
                }

				D(cout << "WINJOINYSBM->EVENT COUNT: " << event_count << " FOR WID: " << WID << endl;);

				pthread_mutex_lock(&WIDtoCIDtoCounts_mutex); // Lock while modifying WIDtoCIDtoCounts map

				for (int i = 0; i < event_count; ++i)
				{
					sede.YSBdeserializePC_m(inMessage, &eventPC, offset + i * sizeof(EventPC_m));

					long int c_id = eventPC.c_id;
					D(cout << "WINJOINYSBM->PROCESSING EVENT WITH c_id: " << c_id << " TYPE: " << eventPC.type << " COUNT: " << eventPC.count << endl;);

					// Check if WID already exists in the outer hashmap
					if (WIDtoCIDtoCounts.find(WID) == WIDtoCIDtoCounts.end())
					{
						WIDtoCIDtoCounts[WID] = InnerHMapM();
						D(cout << "WINJOINYSBM->NEW WID ENTRY CREATED FOR WID: " << WID << endl;);
					}

					// Check if c_id already exists in the inner hashmap
					if (WIDtoCIDtoCounts[WID].find(c_id) == WIDtoCIDtoCounts[WID].end())
					{
						WIDtoCIDtoCounts[WID][c_id] = make_pair(0, 0); // Initialize both counts as 0
						D(cout << "WINJOINYSBM->NEW c_id ENTRY CREATED FOR WID: " << WID << " c_id: " << c_id << endl;);
					}

					// Update click or view count based on event type
					if (eventPC.type == 1)
					{
						WIDtoCIDtoCounts[WID][c_id].first += eventPC.count; // Update click count

						// Update click event time
						CIDtoEventTime[WID][c_id].first = eventPC.event_time;
						D(cout << "WINJOINYSBM->UPDATED CLICK COUNT AND EVENT TIME FOR c_id: " << c_id << endl;);
					}
					else if (eventPC.type == 2)
					{
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
		while (!completed_windows.empty())
		{
			long int WID = completed_windows.front();
			completed_windows.pop_front();

			D(cout << "WINJOINYSBM->PROCESSING COMPLETED WINDOW FOR WID: " << WID << endl;);

			InnerHMapM &CIDtoCounts = WIDtoCIDtoCounts[WID];

			// Iterate through all c_ids in the window and serialize them into the outMessage
			for (auto &cidEntry : CIDtoCounts)
			{
				long int c_id = cidEntry.first;
				long int clickCount = cidEntry.second.first;
				long int viewCount = cidEntry.second.second;

				// If view count is zero, set it to 1 to avoid division by zero
				if (viewCount == 0)
				{
					D(cout << "WINJOINYSBM->MISSING VIEW COUNT FOR WID: " << WID << " c_id: " << c_id << ". SETTING VIEW COUNT TO 1." << endl;);
					viewCount = 1;
				}

				// Get the event time from both click and view events
				long int clickEventTime = CIDtoEventTime[WID][c_id].first;
				long int viewEventTime = CIDtoEventTime[WID][c_id].second;

				// Find the maximum event time between click and view
				long int maxEventTime = std::max(clickEventTime, viewEventTime);

				double ratio = static_cast<double>(clickCount) / viewCount;
				D(cout << "WINJOINYSBM->CALCULATED RATIO FOR WID: " << WID << " c_id: " << c_id << " RATIO: " << ratio << endl;);

				// Prepare the EventWJ struct for the current event
				eventSlice.slice_id = WID;
				eventSlice.c_id = c_id;
				eventSlice.ClickCount = clickCount;
				eventSlice.ViewCount = viewCount;
				eventSlice.ratio = ratio;
				eventSlice.latency = maxEventTime;

				// Serialize the event into the single outMessage
                for (int i = 0; i < queries; i++){
				    sede.YSBserializeSliceM(&eventSlice, outMessage[i]);
                    /*cout << "WINJOINYSBM->Serialized Event for Query: " << i << " WID: " << WID 
           << " Message Size After Serialization: " << outMessage[i]->size << endl;*/
                    // Debugging print for serialization
                    /*cout << "WINJOINYSBM->Serialized Event for Query: " << i << " WID: " << WID << " c_id: " << c_id 
                           << " ClickCount: " << clickCount << " ViewCount: " << viewCount << " Ratio: " << ratio << endl;*/
                }
			}

			// After all events are serialized into outMessage, send the message
			int n = 0;
            int idx;
            cout << "WINJOINYSBM->Next vector size: " << next.size() << endl;  // Print size of 'next'

			for (vector<Vertex *>::iterator v = next.begin(); v != next.end(); ++v)
            {
                for (int i = 0; i < queries; i++)
                {
                    if (outMessage[i] && outMessage[i]->size > 0)
                    {
                        idx = i; // always keep workload on same rank
                        pthread_mutex_lock(&senderMutexes[idx]);
                        outMessages[idx].push_back(outMessage[i]);
                        pthread_cond_signal(&senderCondVars[idx]);
                        pthread_mutex_unlock(&senderMutexes[idx]);
                        // Debugging print for sending
                        cout << "WINJOINYSBM->Sent Message for Query: " << i << " WID: " << WID << " Message Size: " << outMessage[i]->size << endl;
                    }
                    else{
                        cout << "WINJOINYSBM->OutMessage for Query: " << i << " is either null or has zero size!" << endl;
                    }
                }
                n++;
            }
			// Clean up the window data after processing
			WIDtoCIDtoCounts.erase(WID);
			cout << "WINJOINYSBM->WID: " << WID << " IS COMPLETED AND REMOVED" << endl;
		}

		pthread_mutex_unlock(&WIDtoCIDtoCounts_mutex); // Unlock after completed windows are processed

		tmpMessages->clear();

		delete inMessage; //  Delete inMessage  after processing the message
	}

	delete tmpMessages; // Delete temporary message list at the end
}
