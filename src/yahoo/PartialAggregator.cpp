/**
 * Copyright (c) 2020 University of Luxembourg. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are
 * permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this list of
 * conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list
 * of conditions and the following disclaimer in the documentation and/or other materials
 * provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its contributors may be
 * used to endorse or promote products derived from this software without specific prior
 * written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE UNIVERSITY OF LUXEMBOURG AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * THE UNIVERSITY OF LUXEMBOURG OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
 * OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
 * EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 **/

/*
 * Aggregate.cpp
 *
 *  Created on: 17, Dec, 2018
 *      Author: vinu.venugopal
 */

#include "PartialAggregator.hpp"

#include <iostream>
#include <vector>
#include <cstring>
#include <iterator>
#include <cstring>
#include <string>
#include <sstream>
#include <unordered_map>

#include "../communication/Message.hpp"
#include "../function/Function.hpp"
#include "../serialization/Serialization.hpp"

using namespace std;

PartialAggregator::PartialAggregator(int tag, int rank, int worldSize) : Vertex(tag, rank, worldSize)
{
	D(cout << "PARTIALAGGREGATOR [" << tag << "] CREATED @ " << rank << endl;);
}

PartialAggregator::~PartialAggregator()
{
	D(cout << "PARTIALAGGREGATOR [" << tag << "] DELETED @ " << rank << endl;);
}

void PartialAggregator::batchProcess()
{
	D(cout << "PARTIALAGGREGATOR->BATCHPROCESS [" << tag << "] @ " << rank
		   << endl;);
}

void PartialAggregator::streamProcess(int channel)
{

	D(cout << "PARTIALAGGREGATOR->STREAMPROCESS [" << tag << "] @ " << rank
		   << " IN-CHANNEL " << channel << endl;);

	Message *inMessage;
	Message **outMessagesToWindowIDs = new Message *[worldSize];
	list<Message *> *tmpMessages = new list<Message *>();
	Serialization sede;

	OuterHMap WIDtoIHM(10); // Window_ID to Inner-HashMap (IHM) mapping
	OuterHMap::iterator WIDtoIHM_it;
	InnerHMap::iterator CIDtoCountAndMaxEventTime_it; // IHM: Campaign_ID to Max-Event-time mapping

	// WrapperUnit wrapper_unit;
	EventJ eventJ;
	EventPA eventPA;

	int c = 0;
	while (ALIVE)
	{

		pthread_mutex_lock(&listenerMutexes[channel]);

		while (inMessages[channel].empty())
			pthread_cond_wait(&listenerCondVars[channel],
							  &listenerMutexes[channel]);

		//		if(inMessages[channel].size()>1)
		//				  cout<<tag<<" CHANNEL-"<<channel<<" BUFFER SIZE:"<<inMessages[channel].size()<<endl;

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

			D(cout << "PARTIALAGGREGATOR->POP MESSAGE: TAG [" << tag << "] @ "
				   << rank << " CHANNEL " << channel << " BUFFER "
				   << inMessage->size << endl;);

			long int WID, c_id;

			sede.unwrap(inMessage);
			// if (inMessage->wrapper_length > 0) {
			//	sede.unwrapFirstWU(inMessage, &wrapper_unit);
			//	sede.printWrapper(&wrapper_unit);
			// }

			int offset = sizeof(int) + (inMessage->wrapper_length * sizeof(WrapperUnit));

			// Caution: copying the header from the inMessage only works if there is only one wrapper and one window per message!
			for (int w = 0; w < worldSize; w++)
			{
				outMessagesToWindowIDs[w] = new Message(
					inMessage->size - offset, inMessage->wrapper_length); // create new message with max. required capacity
				memcpy(outMessagesToWindowIDs[w]->buffer, inMessage->buffer,
					   offset); // simply copy header from old message for now!
				outMessagesToWindowIDs[w]->size += offset;
			}

			int event_count = (inMessage->size - offset) / sizeof(EventFT);
			D(cout << "\n#EVENT_COUNT: " << event_count << " TAG: " << tag << " RANK: " << rank << endl);

			int i = 0, j = 0, k = 0;

			while (i < event_count)
			{

				sede.YSBdeserializeJ(inMessage, &eventJ,
									 offset + (i * sizeof(EventJ)));

				c_id = std::stol(eventJ.c_id, nullptr, 16);
				WID = (eventJ.event_time / AGG_WIND_SPAN);

				if ((WIDtoIHM_it = WIDtoIHM.find(WID)) != WIDtoIHM.end())
				{

					if ((CIDtoCountAndMaxEventTime_it =
							 WIDtoIHM_it->second.find(c_id)) != (WIDtoIHM_it->second).end())
					{

						CIDtoCountAndMaxEventTime_it->second.first =
							CIDtoCountAndMaxEventTime_it->second.first + 1;

						if (CIDtoCountAndMaxEventTime_it->second.second < eventJ.event_time)
						{ // new max. event time!
							CIDtoCountAndMaxEventTime_it->second.second =
								eventJ.event_time;
						}
					}
					else
					{ // new entry in inner hashmap!
						WIDtoIHM_it->second.emplace(c_id,
													std::make_pair(1, eventJ.event_time));
						k++;
					}
				}
				else
				{ // new entry in the outer hashmap!
					InnerHMap new_CIDtoCountAndMaxEventTime(100);
					new_CIDtoCountAndMaxEventTime.emplace(c_id,
														  std::make_pair(1, eventJ.event_time));
					WIDtoIHM.emplace(WID, new_CIDtoCountAndMaxEventTime);

					j++;
				}

				i++;
			}

			for (WIDtoIHM_it = WIDtoIHM.begin(); WIDtoIHM_it != WIDtoIHM.end();
				 WIDtoIHM_it++)
			{
				int counter = 0, map_size_cal = 0; // for debugging!

				WID = WIDtoIHM_it->first;

				for (CIDtoCountAndMaxEventTime_it =
						 (WIDtoIHM_it->second).begin();
					 CIDtoCountAndMaxEventTime_it != (WIDtoIHM_it->second).end();
					 CIDtoCountAndMaxEventTime_it++)
				{
					map_size_cal++;
					eventPA.max_event_time =
						CIDtoCountAndMaxEventTime_it->second.second;
					eventPA.count = CIDtoCountAndMaxEventTime_it->second.first;
					eventPA.c_id = CIDtoCountAndMaxEventTime_it->first;

					counter = counter + eventPA.count;

					sede.YSBserializePA(&eventPA,
										outMessagesToWindowIDs[WID % worldSize]);
				}

				D(cout << " #W_ID: " << eventPA.max_event_time / AGG_WIND_SPAN
					   << " #Event_time: " << eventPA.max_event_time
					   << "\t map size: " << WIDtoIHM_it->second.size()
					   << "\t map_size_cal: " << map_size_cal
					   << endl;);

				WIDtoIHM_it->second.clear();
			}
			WIDtoIHM.clear();

			int n = 0;
			for (vector<Vertex *>::iterator v = next.begin(); v != next.end();
				 ++v)
			{

				for (int w = 0; w < worldSize; w++)
				{

					int idx = n * worldSize + w; // iterate over all ranks

					if (outMessagesToWindowIDs[w]->size > 20)
					{ // quick fix for now

						// Normal mode: synchronize on outgoing message channel & send message
						pthread_mutex_lock(&senderMutexes[idx]);
						outMessages[idx].push_back(outMessagesToWindowIDs[w]);

						D(cout << "PARTIALAGGREGATOR->PUSHBACK MESSAGE [" << tag
							   << "] #" << c << " @ " << rank << " IN-CHANNEL "
							   << channel << " OUT-CHANNEL " << idx << " SIZE "
							   << outMessagesToWindowIDs[w]->size << " CAP "
							   << outMessagesToWindowIDs[w]->capacity << endl);

						pthread_cond_signal(&senderCondVars[idx]);
						pthread_mutex_unlock(&senderMutexes[idx]);
					}
					else
					{

						delete outMessagesToWindowIDs[w];
					}
				}

				n++;
			}

			delete inMessage;
			c++;
		}

		tmpMessages->clear();
	}

	delete tmpMessages;
}