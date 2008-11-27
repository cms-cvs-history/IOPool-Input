#ifndef IOPool_Input_ProvenanceAdaptor_h
#define IOPool_Input_ProvenanceAdaptor_h

/*----------------------------------------------------------------------

ProvenanceAdaptor.h 

----------------------------------------------------------------------*/
#include <map>
#include <memory>
#include <string>
#include <vector>
#include "boost/utility.hpp"
#include "DataFormats/Provenance/interface/ProvenanceFwd.h"
#include "DataFormats/Provenance/interface/BranchID.h"
#include "DataFormats/Provenance/interface/ProcessHistoryRegistry.h"
#include "DataFormats/Provenance/interface/ModuleDescriptionRegistry.h"
#include "DataFormats/Provenance/interface/ParameterSetID.h"
#include "DataFormats/Provenance/interface/ParameterSetBlob.h"
#include "FWCore/Framework/interface/Frameworkfwd.h"

namespace edm {

  //------------------------------------------------------------
  // Class ProvenanceAdaptor: supports file reading.

  typedef std::map<ParameterSetID, ParameterSetBlob> ParameterSetMap;
  class ProvenanceAdaptor : private boost::noncopyable {
  public:
  typedef std::pair<std::string, BranchID> Product;
  typedef std::vector<Product> OrderedProducts;
  ProvenanceAdaptor(
	     ProductRegistry const& productRegistry,
	     ProcessHistoryMap const& pHistMap,
	     ParameterSetMap const& psetMap,
	     ModuleDescriptionMap const&  mdMap);

  
  std::auto_ptr<BranchIDLists> branchIDLists();

  private:
    ProductRegistry const& productRegistry_;
    OrderedProducts orderedProducts_;
  }; // class ProvenanceAdaptor

}
#endif
