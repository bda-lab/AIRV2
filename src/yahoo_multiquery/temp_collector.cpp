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
 * QueryCollector.cpp
 *
 *  Created on: Dec 26, 2018
 *      Author: vinu.venugopal
 */
#include <mpi.h>
// #include <mpi_time.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include "../serialization/Serialization.hpp"
#include "QueryCollector.hpp"

using namespace std;

QueryCollector::QueryCollector(int tag, int rank, int worldSize) : Vertex(tag, rank, worldSize)
{

	// Global stats
	sum_latency = 0;
	sum_counts = 0;
	num_messages = 0;

	S_CHECK(if (rank == 0) {
		datafile.open("Data/results" + to_string(rank) + ".tsv");
	})

	D(std::cout << "QueryCollector [" << tag << "] CREATED @ " << rank << endl;)
}

QueryCollector::~QueryCollector()
{
	D(std::cout << "QueryCollector [" << tag << "] DELETED @ " << rank << endl;)
}

void QueryCollector::batchProcess()
{
	D(std::cout << "QueryCollector->BATCHPROCESS [" << tag << "] @ " << rank << endl;)
}

void QueryCollector::streamProcess(int channel)
{

	D(std::cout << "QueryCollector->STREAMPROCESS [" << tag << "] @ " << rank
				<< " IN-CHANNEL " << channel << endl;)

	// if (rank == 0)
	// {

	// 	Message *inMessage;
	// 	list<Message *> *tmpMessages = new list<Message *>();
	// 	Serialization sede;

	// 	EventPC eventPC;

	// 	int c = 0;
	// 	while (ALIVE)
	// 	{

	// 		pthread_mutex_lock(&listenerMutexes[channel]);

	// 		while (inMessages[channel].empty())
	// 			pthread_cond_wait(&listenerCondVars[channel],
	// 							  &listenerMutexes[channel]);

	// 		while (!inMessages[channel].empty())
	// 		{
	// 			inMessage = inMessages[channel].front();
	// 			inMessages[channel].pop_front();
	// 			tmpMessages->push_back(inMessage);
	// 		}

	// 		pthread_mutex_unlock(&listenerMutexes[channel]);

	// 		while (!tmpMessages->empty())
	// 		{

	// 			inMessage = tmpMessages->front();
	// 			tmpMessages->pop_front();

	// 			std::cout << "QueryCollector->POP MESSAGE: TAG [" << tag << "] @ "
	// 				 << rank << " CHANNEL " << channel << " BUFFER "
	// 				 << inMessage->size << endl;

	// 			int event_count = inMessage->size / sizeof(EventPC);
	// 			// std::cout << "EVENT_COUNT: " << event_count << endl;

	// 			int i = 0, count = 0;
	// 			std::cout << "*********************************************QUERY 1*********************************************" << endl;
	// 			while (i < event_count)
	// 			{
	// 				sede.YSBdeserializePC(inMessage, &eventPC,
	// 									  i * sizeof(EventPC));
	// 				long int time_now = (long int)(MPI_Wtime() * 1000.0);

	// 				sum_latency += (time_now - eventPC.latency);
	// 				count += eventPC.count;
	// 				//					sede.YSBprintPC(&eventPC);
	// 				S_CHECK(
	// 					datafile
	// 						<< eventPC.WID << "\t"
	// 						<< eventPC.c_id << "\t"
	// 						<< eventPC.count
	// 						<< endl;

	// 				)
	// 				std::cout << "WID: " << eventPC.WID <<"\tQuery 1\t"<< "\tc_id: " << eventPC.c_id << "\tcount: " << eventPC.count << "\tmax_event_time: " << eventPC.latency << endl;

	// 				i++;
	// 			}
	// 			sum_counts += event_count; // count of distinct c_id's processed
	// 			num_messages++;

	// 			std::cout << "\n  #" << num_messages << " COUNT: " << count
	// 				 << "\tAVG_LATENCY: " << ((double)sum_latency / sum_counts) / 1000.0 << "\tGlobal Sum Counts: " << sum_counts << "\tGlobal Sum Latency: " << sum_latency << "\tN=" << event_count << "\n"
	// 				 << endl;

	// 			delete inMessage; // delete message from incoming queue
	// 			c++;
	// 		}

	// 		tmpMessages->clear();
	// 	}

	// 	delete tmpMessages;
	// }
	if (rank == 2)
	{

		Message *inMessage;
		list<Message *> *tmpMessages = new list<Message *>();
		Serialization sede;

		EventPC eventPC;

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

				std::cout << "QueryCollector->POP MESSAGE: TAG [" << tag << "] @ "
						  << rank << " CHANNEL " << channel << " BUFFER "
						  << inMessage->size << endl;

				int event_count = inMessage->size / sizeof(EventPC);
				// std::cout << "EVENT_COUNT: " << event_count << endl;

				int i = 0, count = 0;
				std::cout << "*********************************************QUERY " << rank + 1 << "*********************************************" << endl;
				while (i < event_count)
				{
					sede.YSBdeserializePC(inMessage, &eventPC,
										  i * sizeof(EventPC));
					long int time_now = (long int)(MPI_Wtime() * 1000.0);

					sum_latency += (time_now - eventPC.latency);
					count += eventPC.count;
					//					sede.YSBprintPC(&eventPC);
					S_CHECK(
						datafile
							<< eventPC.WID << "\t"
							<< eventPC.c_id << "\t"
							<< eventPC.count
							<< endl;

					)
					std::cout << "WID: " << eventPC.WID << "\tQuery " << rank + 1 << "\t" << "\tc_id: " << eventPC.c_id << "\tcount: " << eventPC.count << "\tmax_event_time: " << eventPC.latency << endl;

					i++;
				}
				sum_counts += event_count; // count of distinct c_id's processed
				num_messages++;

				std::cout << "\n  #" << num_messages << " COUNT: " << count
						  << "\tAVG_LATENCY: " << ((double)sum_latency / sum_counts) / 1000.0 << "\tGlobal Sum Counts: " << sum_counts << "\tGlobal Sum Latency: " << sum_latency << "\tN=" << event_count << "\n"
						  << endl;

				delete inMessage; // delete message from incoming queue
				c++;
			}

			tmpMessages->clear();
		}

		delete tmpMessages;
	}
	// if (rank == 1)
	// {
	// 	Message *inMessage;
	// 	list<Message *> *tmpMessages = new list<Message *>();
	// 	Serialization sede;

	// 	EventPC eventPC;
	// 	map<int, pair<EventPC, bool>> partialResults; // To store the slices
	// 	int c = 0;

	// 	while (ALIVE)
	// 	{
	// 		pthread_mutex_lock(&listenerMutexes[channel]);

	// 		while (inMessages[channel].empty())
	// 			pthread_cond_wait(&listenerCondVars[channel],
	// 							  &listenerMutexes[channel]);

	// 		while (!inMessages[channel].empty())
	// 		{
	// 			inMessage = inMessages[channel].front();
	// 			inMessages[channel].pop_front();
	// 			tmpMessages->push_back(inMessage);
	// 		}

	// 		pthread_mutex_unlock(&listenerMutexes[channel]);

	// 		while (!tmpMessages->empty())
	// 		{
	// 			inMessage = tmpMessages->front();
	// 			tmpMessages->pop_front();

	// 			std::std::cout << "QueryCollector->POP MESSAGE: TAG [" << tag << "] @ "
	// 				 << rank << " CHANNEL " << channel << " BUFFER "
	// 				 << inMessage->size << endl;

	// 			int event_count = inMessage->size / sizeof(EventPC);

	// 			int i = 0, count = 0;
	// 			while (i < event_count)
	// 			{
	// 				sede.YSBdeserializePC(inMessage, &eventPC,
	// 									  i * sizeof(EventPC));
	// 				int slice_id = eventPC.WID / 2; // Group slices for query 2
	// 				std::std::cout << "Slice ID: " << slice_id << " WID: " << eventPC.WID << endl;
	// 				if (eventPC.WID % 2 == 0) // even WID -> first slice
	// 				{
	// 					std::std::cout<<"Even WID"<<endl;
	// 					partialResults[slice_id].first = eventPC;
	// 					partialResults[slice_id].second = false;
	// 				}
	// 				else // odd WID -> second slice, check completeness
	// 				{
	// 					std::std::cout<<"Odd WID"<<endl;
	// 					if (partialResults.find(slice_id) != partialResults.end())
	// 					{
	// 						sum_latency += (MPI_Wtime() * 1000.0) - partialResults[slice_id].first.latency;
	// 						count = partialResults[slice_id].first.count + eventPC.count;
	// 						partialResults[slice_id].second = true; // Mark as complete

	// 						// Output result for query 2
	// 						std::std::cout << "WID: " << slice_id + 1 << " Query 2 c_id: " << eventPC.c_id
	// 							 << " count: " << count
	// 							 << " latency: " << ((double)sum_latency / sum_counts) / 1000.0 << endl;
	// 					}
	// 				}
	// 				i++;
	// 			}

	// 			sum_counts += event_count; // count of distinct c_id's processed
	// 			num_messages++;

	// 			std::std::cout << "\n  #" << num_messages << " COUNT: " << count
	// 				 << "\tAVG_LATENCY: " << ((double)sum_latency / sum_counts) / 1000.0 << "\tGlobal Sum Counts: " << sum_counts << "\tGlobal Sum Latency: " << sum_latency << "\tN=" << event_count << "\n"
	// 				 << endl;

	// 			delete inMessage; // delete message from incoming queue
	// 			c++;
	// 		}

	// 		tmpMessages->clear();
	// 	}

	// 	delete tmpMessages;
	// }
}
