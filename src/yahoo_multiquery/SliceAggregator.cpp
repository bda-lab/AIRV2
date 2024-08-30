#include "SliceAggregator.hpp"

#include <mpi.h>
#include <cstring>
#include <iostream>
#include <list>
#include <unistd.h>

#include "../communication/Message.hpp"
#include "../communication/Window.hpp"
#include "../serialization/Serialization.hpp"

using namespace std;

SliceAggregator::SliceAggregator(int tag, int rank, int worldSize) : Vertex(tag, rank, worldSize)
{
    D(cout << "SliceAggregator [" << tag << "] CREATED @ " << rank << endl;);
    pthread_mutex_init(&WIDtoIHM_mutex, NULL);
    pthread_mutex_init(&WIDtoWrapperUnit_mutex, NULL);
}

SliceAggregator::~SliceAggregator()
{
    D(cout << "SliceAggregator [" << tag << "] DELETED @ " << rank << endl;);
}

void SliceAggregator::batchProcess()
{
    D(cout << "SliceAggregator->BATCHPROCESS [" << tag << "] @ " << rank << endl;);
}

// -------------------------------------------------------------------------------
void SliceAggregator::streamProcess(int channel)
{
    D(cout << "FULLAGGREGATOR->STREAMPROCESS [" << tag << "] @ " << rank
           << " IN-CHANNEL " << channel << endl;);

    Message *inMessage, *outMessage = nullptr, *outMessage1 = nullptr, *outMessage2 = nullptr, *outMessage3 = nullptr;
    list<Message *> *tmpMessages = new list<Message *>();
    Serialization sede;

    WIDtoWrapperUnitHMap::iterator WIDtoWrapperUnit_it;
    OuterHMap::iterator WIDtoIHM_it;
    InnerHMap::iterator CIDtoCountAndMaxEventTime_it;

    WrapperUnit wrapper_unit;
    EventPA eventPA;
    EventSlice eventSlice;

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

            D(cout << "FULLAGGREGATOR->POP MESSAGE: TAG [" << tag << "] @ "
                   << rank << " CHANNEL " << channel << " BUFFER "
                   << inMessage->size << endl);

            long int WID;
            list<long int> completed_windows;

            sede.unwrap(inMessage);
            if (inMessage->wrapper_length > 0)
            {

                sede.unwrapFirstWU(inMessage, &wrapper_unit);
                // cout << "Slice Aggregator: Unwrapping the first wrapper unit" << endl;
                // sede.printWrapper(&wrapper_unit);
                // cout << "*******************************************************" << endl;
                WID = wrapper_unit.window_start_time / AGG_WIND_SPAN;

                if (wrapper_unit.completeness_tag_denominator == 1)
                {
                    completed_windows.push_back(WID);
                }
                else
                {
                    pthread_mutex_lock(&WIDtoWrapperUnit_mutex);

                    if ((WIDtoWrapperUnit_it = WIDtoWrapperUnit.find(WID)) != WIDtoWrapperUnit.end())
                    {
                        WIDtoWrapperUnit_it->second.first += wrapper_unit.completeness_tag_numerator;

                        if (WIDtoWrapperUnit_it->second.first >= WIDtoWrapperUnit_it->second.second)
                        {
                            completed_windows.push_back(WID);
                            WIDtoWrapperUnit.erase(WID);
                        }
                    }
                    else
                    {
                        WIDtoWrapperUnit.emplace(WID,
                                                 make_pair(wrapper_unit.completeness_tag_numerator,
                                                           wrapper_unit.completeness_tag_denominator));
                    }

                    pthread_mutex_unlock(&WIDtoWrapperUnit_mutex);
                }
            }

            int offset = sizeof(int) + (inMessage->wrapper_length * sizeof(WrapperUnit));
            // int wrappers_per_msg = 1;
            outMessage = new Message(sizeof(EventSlice) * 100);  // create new message with max. required capacity
            outMessage1 = new Message(sizeof(EventSlice) * 100); // create new message with max. required capacity
            outMessage2 = new Message(sizeof(EventSlice) * 100); // create new message with max. required capacity
            outMessage3 = new Message(sizeof(EventSlice) * 100); // create new message with max. required capacity
            // outMessage = new Message(sizeof(EventSlice) * 100, wrappers_per_msg);
            // memcpy(outMessage->buffer, &wrappers_per_msg, sizeof(int));

            // outMessage1 = new Message(sizeof(EventSlice) * 100, wrappers_per_msg);
            // memcpy(outMessage1->buffer, &wrappers_per_msg, sizeof(int));

            // outMessage2 = new Message(sizeof(EventSlice) * 100, wrappers_per_msg);
            // memcpy(outMessage2->buffer, &wrappers_per_msg, sizeof(int));

            // outMessage3 = new Message(sizeof(EventSlice) * 100, wrappers_per_msg);
            // memcpy(outMessage3->buffer, &wrappers_per_msg, sizeof(int));

            pthread_mutex_lock(&WIDtoIHM_mutex);

            int i = 0, j = 0, k = 0;
            int event_count = (inMessage->size - offset) / sizeof(EventPA);

            while (i < event_count)
            {
                sede.YSBdeserializePA(inMessage, &eventPA, offset + (i * sizeof(EventPA)));
                WID = eventPA.max_event_time / AGG_WIND_SPAN;

                if ((WIDtoIHM_it = WIDtoIHM.find(WID)) != WIDtoIHM.end())
                {
                    if ((CIDtoCountAndMaxEventTime_it = WIDtoIHM_it->second.find(eventPA.c_id)) != WIDtoIHM_it->second.end())
                    {
                        CIDtoCountAndMaxEventTime_it->second.first += eventPA.count;

                        if (CIDtoCountAndMaxEventTime_it->second.second < eventPA.max_event_time)
                        {
                            CIDtoCountAndMaxEventTime_it->second.second = eventPA.max_event_time;
                        }
                    }
                    else
                    {
                        WIDtoIHM_it->second.emplace(eventPA.c_id, make_pair(eventPA.count, eventPA.max_event_time));
                        k++;
                    }
                }
                else
                {
                    InnerHMap new_CIDtoCountAndMaxEventTime(100);
                    new_CIDtoCountAndMaxEventTime.emplace(eventPA.c_id, make_pair(eventPA.count, eventPA.max_event_time));
                    WIDtoIHM.emplace(WID, new_CIDtoCountAndMaxEventTime);
                    j++;
                }

                i++;
            }

            while (!completed_windows.empty())
            {
                WID = completed_windows.front();
                completed_windows.pop_front();

                WIDtoIHM_it = WIDtoIHM.find(WID);
                if (WIDtoIHM_it != WIDtoIHM.end())
                {
                    j = 0;

                    // wrapper_unit.completeness_tag_numerator = WID / 1;
                    // wrapper_unit.completeness_tag_denominator = WID / 1;
                    // wrapper_unit.window_start_time = WID;
                    // memcpy(outMessage->buffer + sizeof(int), &wrapper_unit, sizeof(WrapperUnit));
                    // outMessage->size += sizeof(WrapperUnit);

                    // wrapper_unit1.completeness_tag_numerator = WID / 2;
                    // wrapper_unit1.completeness_tag_denominator = WID / 2;
                    // wrapper_unit1.window_start_time = WID;
                    // memcpy(outMessage1->buffer + sizeof(int), &wrapper_unit1, sizeof(WrapperUnit));
                    // outMessage1->size += sizeof(WrapperUnit);

                    // wrapper_unit2.completeness_tag_numerator = WID / 3;
                    // wrapper_unit2.completeness_tag_denominator = WID / 3;
                    // wrapper_unit2.window_start_time = WID;
                    // memcpy(outMessage2->buffer + sizeof(int), &wrapper_unit2, sizeof(WrapperUnit));
                    // outMessage2->size += sizeof(WrapperUnit);

                    // wrapper_unit3.completeness_tag_numerator = WID / 4;
                    // wrapper_unit3.completeness_tag_denominator = WID / 4;
                    // wrapper_unit3.window_start_time = WID;
                    // memcpy(outMessage3->buffer + sizeof(int), &wrapper_unit3, sizeof(WrapperUnit));
                    // outMessage3->size += sizeof(WrapperUnit);

                    for (CIDtoCountAndMaxEventTime_it = WIDtoIHM_it->second.begin();
                         CIDtoCountAndMaxEventTime_it != WIDtoIHM_it->second.end();
                         CIDtoCountAndMaxEventTime_it++)
                    {
                        eventSlice.slice_id = WID;
                        eventSlice.c_id = CIDtoCountAndMaxEventTime_it->first;
                        eventSlice.count = CIDtoCountAndMaxEventTime_it->second.first;
                        eventSlice.latency = CIDtoCountAndMaxEventTime_it->second.second;

                        sede.YSBserializeSlice(&eventSlice, outMessage);
                        // sede.YSBprintSlice(&eventSlice);
                        // sede.printWrapper(&wrapper_unit);
                        // cout << "***************************************************************************************************" << endl;
                        sede.YSBserializeSlice(&eventSlice, outMessage1);
                        // sede.YSBprintSlice(&eventSlice);
                        // sede.printWrapper(&wrapper_unit1);
                        // cout << "***************************************************************************************************" << endl;
                        sede.YSBserializeSlice(&eventSlice, outMessage2);
                        // sede.YSBprintSlice(&eventSlice);
                        // sede.printWrapper(&wrapper_unit2);
                        // cout << "***************************************************************************************************" << endl;
                        sede.YSBserializeSlice(&eventSlice, outMessage3);
                        // sede.YSBprintSlice(&eventSlice);
                        // sede.printWrapper(&wrapper_unit3);
                        // cout << "***************************************************************************************************" << endl;

                        j++;
                    }

                    WIDtoIHM_it->second.clear();
                    WIDtoIHM.erase(WID);
                }
            }

            pthread_mutex_unlock(&WIDtoIHM_mutex);

            int n = 0;
            int idx;
            for (vector<Vertex *>::iterator v = next.begin(); v != next.end(); ++v)
            {
                if (outMessage && outMessage->size > 0)
                {
                    idx = 0; // Channel for rank 0
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

                if (outMessage1 && outMessage1->size > 0)
                {
                    idx = 1; // Channel for rank 1
                    pthread_mutex_lock(&senderMutexes[idx]);
                    outMessages[idx].push_back(outMessage1);

                    // cout << "SliceAggregator->PUSHBACK MESSAGE [" << tag << "] #"
                    //      << " @ " << rank << " IN-CHANNEL " << channel
                    //      << " OUT-CHANNEL " << idx << " SIZE "
                    //      << outMessage1->size << " CAP "
                    //      << outMessage1->capacity << endl;

                    pthread_cond_signal(&senderCondVars[idx]);
                    pthread_mutex_unlock(&senderMutexes[idx]);
                }

                if (outMessage2 && outMessage2->size > 0)
                {
                    idx = 2; // Channel for rank 2
                    pthread_mutex_lock(&senderMutexes[idx]);
                    outMessages[idx].push_back(outMessage2);

                    // cout << "SliceAggregator->PUSHBACK MESSAGE [" << tag << "] #"
                    //      << " @ " << rank << " IN-CHANNEL " << channel
                    //      << " OUT-CHANNEL " << idx << " SIZE "
                    //      << outMessage2->size << " CAP "
                    //      << outMessage2->capacity << endl;

                    pthread_cond_signal(&senderCondVars[idx]);
                    pthread_mutex_unlock(&senderMutexes[idx]);
                }

                if (outMessage3 && outMessage3->size > 0)
                {
                    idx = 3; // Channel for rank 3
                    pthread_mutex_lock(&senderMutexes[idx]);
                    outMessages[idx].push_back(outMessage3);

                    // cout << "SliceAggregator->PUSHBACK MESSAGE [" << tag << "] #"
                    //      << " @ " << rank << " IN-CHANNEL " << channel
                    //      << " OUT-CHANNEL " << idx << " SIZE "
                    //      << outMessage3->size << " CAP "
                    //      << outMessage3->capacity << endl;

                    pthread_cond_signal(&senderCondVars[idx]);
                    pthread_mutex_unlock(&senderMutexes[idx]);
                }
            }

            delete inMessage;
        }
    }
}
