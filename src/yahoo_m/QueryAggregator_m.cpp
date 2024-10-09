#include "QueryAggregator_m.hpp"

#include <mpi.h>
#include <cstring>
#include <iostream>
#include <list>
#include <unistd.h>
#include <bits/stdc++.h>
#include "../communication/Message.hpp"
#include "../communication/Window.hpp"
#include "../serialization/Serialization.hpp"

using namespace std;

QueryAggregatorM::QueryAggregatorM(int tag, int rank, int worldSize) : Vertex(tag, rank, worldSize)
{
    D(cout << "QueryAggregatorM [" << tag << "] CREATED @ " << rank << endl;);
    pthread_mutex_init(&WIDtoIHM_mutex, NULL);
    pthread_mutex_init(&WIDtoWrapperUnit_mutex, NULL);
    // std::unordered_map<long int, set<long int>> slicePresenceMap;
    // std::unordered_map<long int, InnerHMap> WIDtoIHM;
}

QueryAggregatorM::~QueryAggregatorM()
{
    D(cout << "QueryAggregator [" << tag << "] DELETED @ " << rank << endl;);
}

void QueryAggregatorM::batchProcess()
{
    D(cout << "QueryAggregator->BATCHPROCESS [" << tag << "] @ " << rank << endl;);
}

void QueryAggregatorM::streamProcess(int channel)
{
    D(cout << "QueryAggregatorM->STREAMPROCESS [" << tag << "] @ " << rank
           << " IN-CHANNEL " << channel << endl;);
    if (rank == 0)
    {
        Message *inMessage, *outMessage;
        list<Message *> *tmpMessages = new list<Message *>();
        Serialization sede;

        EventSliceM eventSlice;
        EventWJ eventWJ;

        int c = 0;
        while (ALIVE)
        {
            pthread_mutex_lock(&listenerMutexes[channel]);

            while (inMessages[channel].empty())
                pthread_cond_wait(&listenerCondVars[channel], &listenerMutexes[channel]);

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

                // cout << "QueryAggregator->POP MESSAGE: TAG [" << tag << "] @ "
                //      << rank << " CHANNEL " << channel << " BUFFER "
                //      << inMessage->size << endl;

                long int WID;
                list<long int> completed_windows;

                int event_count = (inMessage->size) / sizeof(EventSliceM);
                pthread_mutex_lock(&WIDtoIHM_mutex);
                outMessage = new Message(sizeof(EventWJ) * 100); // Create new message with max. required capacity

                for (int i = 0; i < event_count; i++)
                {
                    sede.YSBdeserializeSliceM(inMessage, &eventSlice, i * sizeof(EventSliceM));
                    eventWJ.WID = eventSlice.slice_id;
                    eventWJ.c_id = eventSlice.c_id;
                    eventWJ.ClickCount = eventSlice.ClickCount;
                    eventWJ.ViewCount = eventSlice.ViewCount;
                    eventWJ.ratio = eventSlice.ratio;
                    eventWJ.latency = eventSlice.latency;
                    sede.YSBserializeWJ(&eventWJ, outMessage);
                }

                pthread_mutex_unlock(&WIDtoIHM_mutex);

                for (vector<Vertex *>::iterator v = next.begin(); v != next.end(); ++v)
                {
                    if (outMessage && outMessage->size > 0)
                    {
                        int idx = 0; // Channel for rank 0
                        pthread_mutex_lock(&senderMutexes[idx]);
                        outMessages[idx].push_back(outMessage);

                        // cout << "SliceAggregator->PUSHBACK MESSAGE [" << tag << "] #"
                        //      << " @ " << rank << " IN-CHANNEL " << channel
                        //      << " OUT-CHANNEL " << idx << " SIZE "
                        //      << outMessage->size << " CAP "
                        //      << outMessage->capacity << endl;

                        pthread_cond_signal(&senderCondVars[idx]);
                        pthread_mutex_unlock(&senderMutexes[idx]);
                    }
                }
                delete inMessage;
                c++;
            }

            tmpMessages->clear();
        }

        delete tmpMessages;
    }
     

      else{

    Message *inMessage, *outMessage;
    list<Message *> *tmpMessages = new list<Message *>();
    Serialization sede;

    EventSliceM eventSlice;
    EventWJ eventWJ;

    int c = 0;
    while (ALIVE)
    {
        pthread_mutex_lock(&listenerMutexes[channel]);

        while (inMessages[channel].empty())
            pthread_cond_wait(&listenerCondVars[channel], &listenerMutexes[channel]);

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

            long int WID;
            list<long int> completed_windows;

            int event_count = (inMessage->size) / sizeof(EventSliceM);
            pthread_mutex_lock(&WIDtoIHM_mutex);
            outMessage = new Message(sizeof(EventWJ) * 100); // Create new message with max. required capacity

            for (int i = 0; i < event_count; i++)
            {
                sede.YSBdeserializeSliceM(inMessage, &eventSlice, i * sizeof(EventSliceM));
                WID = eventSlice.slice_id / (rank + 1);
                std::pair<long int, int> wid_rank_pair = std::make_pair(WID, rank);

                // **Update**: Track slices properly
                slicePresenceMap[wid_rank_pair].insert(eventSlice.slice_id);
                if (slicePresenceMap[wid_rank_pair].size() == (rank + 1))
                {
                    completed_windows.push_back(WID);
                    slicePresenceMap.erase(wid_rank_pair); // Remove WID from tracking map once complete
                }

                // **Update**: Aggregate slices for the same WID
                WIDtoIHM_it = WIDtoIHM.find(WID);
                if (WIDtoIHM_it != WIDtoIHM.end())
                {
                    CIDtoCountAndMaxEventTime_it = WIDtoIHM_it->second.find(eventSlice.c_id);
                    if (CIDtoCountAndMaxEventTime_it != WIDtoIHM_it->second.end())
                    {
                        // Aggregate counts and max_event_time across slices
                        std::get<0>(CIDtoCountAndMaxEventTime_it->second) += eventSlice.ClickCount; // Update clickCount
                        std::get<1>(CIDtoCountAndMaxEventTime_it->second) += eventSlice.ViewCount;
                        if (std::get<2>(CIDtoCountAndMaxEventTime_it->second) < eventSlice.latency) // Compare latency
                            std::get<2>(CIDtoCountAndMaxEventTime_it->second) = eventSlice.latency; // Update latency
                    }
                    else
                    {
                        // Add new entry with count and latency as a tuple
                        WIDtoIHM_it->second[eventSlice.c_id] = std::make_tuple(eventSlice.ClickCount, eventSlice.ViewCount, eventSlice.latency); // Set viewCount as 0 initially
                    }
                }
                else
                {
                    InnerHMap_Q newInnerMap;
                    // Add new entry for a new WID
                    newInnerMap[eventSlice.c_id] = std::make_tuple(eventSlice.ClickCount, eventSlice.ViewCount, eventSlice.latency); // Set viewCount as 0 initially
                    WIDtoIHM[WID] = newInnerMap;
                }
            }

            // **Update**: Generate output after slices aggregation
            while (!completed_windows.empty())
            {
                WID = completed_windows.front();
                completed_windows.pop_front();

                outMessage = new Message(sizeof(EventWJ) * 100); // Create new message with max. required capacity

                WIDtoIHM_it = WIDtoIHM.find(WID);
                if (WIDtoIHM_it != WIDtoIHM.end())
                {
                    for (CIDtoCountAndMaxEventTime_it = WIDtoIHM_it->second.begin();
                         CIDtoCountAndMaxEventTime_it != WIDtoIHM_it->second.end();
                         CIDtoCountAndMaxEventTime_it++)
                    {
                        eventWJ.WID = WID;
                        eventWJ.c_id = CIDtoCountAndMaxEventTime_it->first;
                        eventWJ.ClickCount = std::get<0>(CIDtoCountAndMaxEventTime_it->second); // Get clickCount
                        eventWJ.ViewCount = std::get<1>(CIDtoCountAndMaxEventTime_it->second); // Get viewCount
                        eventWJ.latency = std::get<2>(CIDtoCountAndMaxEventTime_it->second); // Get latency
                        if (eventWJ.ViewCount > 0) {
                            eventWJ.ratio = static_cast<double>(eventWJ.ClickCount) / eventWJ.ViewCount; // Calculate ratio
                        } else {
                            eventWJ.ratio = 0; // If ViewCount is zero, set ratio to zero
                        }
                        sede.YSBserializeWJ(&eventWJ, outMessage);
                    }

                    WIDtoIHM_it->second.clear(); // Clear inner map
                    WIDtoIHM.erase(WID);         // Remove from outer map
                }
            }

            pthread_mutex_unlock(&WIDtoIHM_mutex);

            for (vector<Vertex *>::iterator v = next.begin(); v != next.end(); ++v)
            {
                if (outMessage && outMessage->size > 0)
                {
                    int idx = rank; // Channel equal to rank number
                    pthread_mutex_lock(&senderMutexes[idx]);
                    outMessages[idx].push_back(outMessage);

                    pthread_cond_signal(&senderCondVars[idx]);
                    pthread_mutex_unlock(&senderMutexes[idx]);
                }
            }
            delete inMessage;
            c++;
        }

        tmpMessages->clear();
    }

    delete tmpMessages;
}

}
