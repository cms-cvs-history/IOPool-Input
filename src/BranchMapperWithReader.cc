/*----------------------------------------------------------------------
  
BranchMapperWithReader:

----------------------------------------------------------------------*/
#include "BranchMapperWithReader.h"

namespace edm {
  void
  BranchMapperWithReader<EventEntryInfo>::readProvenance_() const {
    branchPtr_->SetAddress(&pInfoVector_);
    input::getEntry(branchPtr_, entryNumber_);
    BranchMapperWithReader<EventEntryInfo> * me = const_cast<BranchMapperWithReader<EventEntryInfo> *>(this);
    for (std::vector<EventEntryInfo>::const_iterator it = infoVector_.begin(), itEnd = infoVector_.end();
      it != itEnd; ++it) {
      me->insert(it->makeProductProvenance());
      me->oldProductIDToBranchIDMap_.insert(std::make_pair(it->productID().oldID(), it->branchID()));
    }
  }

  BranchID
  BranchMapperWithReader<EventEntryInfo>::oldProductIDToBranchID_(ProductID const& oldProductID) const {
    std::map<unsigned int, BranchID>::const_iterator it = oldProductIDToBranchIDMap_.find(oldProductID.oldID());    
    if (it == oldProductIDToBranchIDMap_.end()) {
	throw 0; // QQQ
    }
    return it->second;
  }
}
