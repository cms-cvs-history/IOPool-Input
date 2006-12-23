#ifndef Input_RootTree_h
#define Input_RootTree_h

/*----------------------------------------------------------------------

RootTree.h // used by ROOT input sources

$Id: RootTree.h,v 1.14 2006/12/14 04:30:59 wmtan Exp $

----------------------------------------------------------------------*/

#include <memory>
#include <string>

#include "boost/shared_ptr.hpp"

#include "IOPool/Input/src/Inputfwd.h"
#include "FWCore/Framework/interface/Frameworkfwd.h"
#include "FWCore/ParameterSet/interface/Registry.h"
#include "DataFormats/Common/interface/BranchEntryDescription.h"
#include "DataFormats/Common/interface/BranchDescription.h"
#include "DataFormats/Common/interface/BranchKey.h"
#include "DataFormats/Common/interface/BranchType.h"
#include "DataFormats/Common/interface/Wrapper.h"
#include "TBranch.h"
#include "TFile.h"

namespace edm {

  class RootTree {
  public:
    typedef input::BranchMap BranchMap;
    typedef std::map<ProductID, BranchDescription> ProductMap;
    typedef input::EntryNumber EntryNumber;
    RootTree(TFile & file, BranchType const& branchType);
    ~RootTree() {}
    
    void addBranch(BranchKey const& key,
		   BranchDescription const& prod,
		   BranchMap & branches,
		   ProductMap & products,
		   std::string const& oldBranchName);
    bool next() {return ++entryNumber_ < entries_;} 
    bool previous() {return --entryNumber_ >= 0;} 
    EntryNumber const& entryNumber() const {return entryNumber_;}
    EntryNumber const& entries() const {return entries_;}
    std::vector<BranchEntryDescription> & provenance() {return provenance_;}
    std::vector<BranchEntryDescription *> & provenancePtrs() {return provenancePtrs_;}
    EntryNumber getBestEntryNumber(unsigned int major, unsigned int minor) const;
    EntryNumber getExactEntryNumber(unsigned int major, unsigned int minor) const;
    void setEntryNumber(EntryNumber theEntryNumber) {entryNumber_ = theEntryNumber;}
    void resetEntryNumber() {entryNumber_ = origEntryNumber_;}
    void setOrigEntryNumber() {origEntryNumber_ = entryNumber_;}
    TTree *tree() {return tree_;}
    TTree *metaTree() {return metaTree_;}
    TBranch *auxBranch() {return auxBranch_;}
    std::vector<std::string> const& branchNames() const {return branchNames_;}
  private:
// We use bare pointers for pointers to ROOT entities.
// Root owns them and uses bare pointers internally.
// Therefore,using smart pointers here will do no good.
    TTree *tree_;
    TTree *metaTree_;
    TBranch *auxBranch_;
    EntryNumber entries_;
    EntryNumber entryNumber_;
    EntryNumber origEntryNumber_;
    std::vector<std::string> branchNames_;
    std::vector<BranchEntryDescription> provenance_;
    std::vector<BranchEntryDescription *> provenancePtrs_;
  };
}
#endif