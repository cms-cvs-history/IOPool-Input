/*----------------------------------------------------------------------

ProvenanceAdaptor.cc

----------------------------------------------------------------------*/
#include <cassert>
#include "IOPool/Input/src/ProvenanceAdaptor.h"
#include <set>
#include "DataFormats/Provenance/interface/ProductRegistry.h"
#include "DataFormats/Provenance/interface/ProcessConfiguration.h"
#include "DataFormats/Provenance/interface/ProcessHistory.h"
#include "FWCore/Utilities/interface/Algorithms.h"

namespace edm {

  
  //------------------------------------------------------------
  // Class ProvenanceAdaptor: adapts old provenance (fileFormatVersion_.value_ < 11) to new provenance.
  namespace {
    typedef std::vector<std::string> OneHistory;
    typedef std::set<OneHistory> Histories;
    typedef ProvenanceAdaptor::Product Product;
    struct Sorter {
      explicit Sorter(Histories const& histories) : histories_(histories) {}
      bool operator()(Product const& a, Product const& b) const;
      Histories const histories_;
    };
    bool Sorter::operator()(Product const& a, Product const& b) const {
      assert (a != b);
      if (a.first == b.first) return false;
      bool mayBeTrue = false;
      for (Histories::const_iterator it = histories_.begin(), itEnd = histories_.end(); it != itEnd; ++it) {
	OneHistory::const_iterator itA = find_in_all(*it, a.first);
	if (itA == it->end()) continue;
	OneHistory::const_iterator itB = find_in_all(*it, b.first);
	if (itB == it->end()) continue;
	assert (itA != itB);
	if (itB < itA) {
	  // process b precedes process a;
	  return false;
	}
	// process a precedes process b;
	mayBeTrue = true;
      }
      return mayBeTrue;
    }
  }

  ProvenanceAdaptor::ProvenanceAdaptor(
	     ProductRegistry const& productRegistry,
	     ProcessHistoryMap const& pHistMap,
	     ParameterSetMap const& psetMap,
	     ModuleDescriptionMap const&  mdMap) :
		productRegistry_(productRegistry),
		orderedProducts_() {
    std::set<std::string> processNamesThatProduced;
    ProductRegistry::ProductList const& prodList = productRegistry.productList();
    for (ProductRegistry::ProductList::const_iterator it = prodList.begin(), itEnd = prodList.end(); it != itEnd; ++it) {
      processNamesThatProduced.insert(it->second.processName());
      orderedProducts_.push_back(std::make_pair(it->second.processName(), it->second.branchID()));
    }
    assert (!orderedProducts_.empty());
    if (processNamesThatProduced.size() == 1) return;
    Histories processHistories;
    size_t max = 0;
    for(ProcessHistoryMap::const_iterator it = pHistMap.begin(), itEnd = pHistMap.end(); it != itEnd; ++it) {
      ProcessHistory const& pHist = it->second;
      OneHistory processHistory;
      for(ProcessHistory::const_iterator i = pHist.begin(), iEnd = pHist.end(); i != iEnd; ++i) {
	if (processNamesThatProduced.find(i->processName()) != processNamesThatProduced.end()) {
	  processHistory.push_back(i->processName());
	}
      }
      max = (processHistory.size() > max ? processHistory.size() : max);
      assert(max <= processNamesThatProduced.size());
      if (processHistory.size() > 1) {
        processHistories.insert(processHistory);
      }
    }
    if (processHistories.empty()) {
      return;
    }
    stable_sort_all(orderedProducts_, Sorter(processHistories));
  }

  std::auto_ptr<BranchIDLists>
  ProvenanceAdaptor::branchIDLists() const {
    std::auto_ptr<BranchIDLists> pv(new BranchIDLists);
    std::auto_ptr<BranchIDList> p(new BranchIDList);
    std::string processName;
    for (OrderedProducts::const_iterator it = orderedProducts_.begin(), itEnd = orderedProducts_.end(); it != itEnd; ++it) {
      bool newvector = it->first != processName && !processName.empty();
      if (newvector) {
	pv->push_back(*p);
	processName = it->first;
	p.reset(new BranchIDList);
      }
      p->push_back(it->second.id());
    }
    pv->push_back(*p);
    return pv;
  }

  void
  ProvenanceAdaptor::branchListIndexes(BranchListIndexes & indexes)  const {
  }
}
