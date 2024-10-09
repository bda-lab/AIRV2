#ifndef USECASES_YSBM_MultiQuery_HPP_
#define USECASES_YSBM_MultiQuery_HPP_

#include "../dataflow/Dataflow.hpp"

using namespace std;

class YSBM_MultiQuery : public Dataflow
{

public:
	Vertex *generator, *filter, *joinClick, *joinView, *par_aggregateClick, *par_aggregateView, *full_aggregateClick, *full_aggregateView, *ratioFinder, *queryAggregator, *collector;

	YSBM_MultiQuery(unsigned long tp);

	~YSBM_MultiQuery();
};

#endif /* USECASES_YSBM_MultiQuery_HPP_ */
