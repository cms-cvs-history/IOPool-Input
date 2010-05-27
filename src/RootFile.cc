/*----------------------------------------------------------------------
----------------------------------------------------------------------*/

#include "RootFile.h"
#include "DuplicateChecker.h"
#include "ProvenanceAdaptor.h"

#include "FWCore/Utilities/interface/EDMException.h"
#include "FWCore/Utilities/interface/GlobalIdentifier.h"
#include "DataFormats/Provenance/interface/BranchDescription.h"
#include "DataFormats/Provenance/interface/BranchType.h"
#include "FWCore/Framework/interface/FileBlock.h"
#include "FWCore/Framework/interface/EventPrincipal.h"
#include "FWCore/Framework/interface/GroupSelector.h"
#include "FWCore/Framework/interface/LuminosityBlockPrincipal.h"
#include "FWCore/Framework/interface/RunPrincipal.h"
#include "FWCore/ParameterSet/interface/FillProductRegistryTransients.h"
#include "FWCore/Sources/interface/EventSkipperByID.h"
#include "DataFormats/Provenance/interface/BranchChildren.h"
#include "DataFormats/Provenance/interface/MinimalEventID.h"
#include "DataFormats/Provenance/interface/ProductRegistry.h"
#include "DataFormats/Provenance/interface/ParameterSetBlob.h"
#include "DataFormats/Provenance/interface/ParentageRegistry.h"
#include "DataFormats/Provenance/interface/ProcessConfigurationRegistry.h"
#include "DataFormats/Provenance/interface/ProcessHistoryRegistry.h"
#include "DataFormats/Provenance/interface/RunID.h"
#include "DataFormats/Common/interface/RefCoreStreamer.h"
#include "FWCore/ServiceRegistry/interface/Service.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/ParameterSet/interface/Registry.h"
#include "FWCore/Utilities/interface/Algorithms.h"
#include "DataFormats/Common/interface/EDProduct.h"
//used for friendlyName translation
#include "FWCore/Utilities/interface/FriendlyName.h"

//used for backward compatibility
#include "DataFormats/Provenance/interface/BranchEntryDescription.h"
#include "DataFormats/Provenance/interface/EntryDescriptionRegistry.h"
#include "DataFormats/Provenance/interface/EventAux.h"
#include "DataFormats/Provenance/interface/LuminosityBlockAux.h"
#include "DataFormats/Provenance/interface/RunAux.h"
#include "DataFormats/Provenance/interface/RunLumiEntryInfo.h"
#include "FWCore/ParameterSet/interface/ParameterSetConverter.h"

#include "TROOT.h"
#include "TClass.h"
#include "TFile.h"
#include "TTree.h"
#include "Rtypes.h"
#include <algorithm>
#include <map>
#include <list>

namespace edm {
  namespace {
    int
    forcedRunOffset(RunNumber_t const& forcedRunNumber, IndexIntoFile::const_iterator inxBegin, IndexIntoFile::const_iterator inxEnd) {
      if(inxBegin == inxEnd) return 0;
      int defaultOffset = (inxBegin->run() != 0 ? 0 : 1);
      int offset = (forcedRunNumber != 0U ? forcedRunNumber - inxBegin->run() : defaultOffset);
      if(offset < 0) {
        throw edm::Exception(errors::Configuration)
          << "The value of the 'setRunNumber' parameter must not be\n"
          << "less than the first run number in the first input file.\n"
          << "'setRunNumber' was " << forcedRunNumber <<", while the first run was "
          << forcedRunNumber - offset << ".\n";
      }
      return offset;
    }
  }

//---------------------------------------------------------------------
  RootFile::RootFile(std::string const& fileName,
		     std::string const& catalogName,
		     ProcessConfiguration const& processConfiguration,
		     std::string const& logicalFileName,
		     boost::shared_ptr<TFile> filePtr,
		     boost::scoped_ptr<EventSkipperByID> const& eventSkipperByID,
		     bool skipAnyEvents,
		     int remainingEvents,
		     int remainingLumis,
		     unsigned int treeCacheSize,
                     int treeMaxVirtualSize,
		     InputSource::ProcessingMode processingMode,
		     RunNumber_t const& forcedRunNumber,
                     bool noEventSort,
		     GroupSelectorRules const& groupSelectorRules,
                     bool dropMergeable,
                     boost::shared_ptr<DuplicateChecker> duplicateChecker,
                     bool dropDescendants,
                     std::vector<boost::shared_ptr<IndexIntoFile> > const& indexesIntoFiles,
                     std::vector<boost::shared_ptr<IndexIntoFile> >::size_type currentIndexIntoFile,
                     std::vector<ProcessHistoryID> & orderedProcessHistoryIDs) :
      file_(fileName),
      logicalFile_(logicalFileName),
      catalog_(catalogName),
      processConfiguration_(processConfiguration),
      filePtr_(filePtr),
      fileFormatVersion_(),
      fid_(),
      indexIntoFileSharedPtr_(new IndexIntoFile),
      indexIntoFile_(*indexIntoFileSharedPtr_),
      orderedProcessHistoryIDs_(orderedProcessHistoryIDs),
      indexIntoFileBegin_(indexIntoFile_.begin()),
      indexIntoFileEnd_(indexIntoFileBegin_),
      indexIntoFileIter_(indexIntoFileBegin_),
      eventProcessHistoryIDs_(),
      eventProcessHistoryIter_(eventProcessHistoryIDs_.begin()),
      skipAnyEvents_(skipAnyEvents),
      noEventSort_(noEventSort),
      whyNotFastClonable_(0),
      reportToken_(0),
      eventAux_(),
      eventTree_(filePtr_, InEvent),
      lumiTree_(filePtr_, InLumi),
      runTree_(filePtr_, InRun),
      treePointers_(),
      productRegistry_(),
      branchIDLists_(),
      processingMode_(processingMode),
      forcedRunOffset_(0),
      newBranchToOldBranch_(),
      eventHistoryTree_(0),
      history_(new History),
      branchChildren_(new BranchChildren),
      duplicateChecker_(duplicateChecker),
      provenanceAdaptor_() {

    eventTree_.setCacheSize(treeCacheSize);

    eventTree_.setTreeMaxVirtualSize(treeMaxVirtualSize);
    lumiTree_.setTreeMaxVirtualSize(treeMaxVirtualSize);
    runTree_.setTreeMaxVirtualSize(treeMaxVirtualSize);

    treePointers_[InEvent] = &eventTree_;
    treePointers_[InLumi]  = &lumiTree_;
    treePointers_[InRun]   = &runTree_;

    // Read the metadata tree.
    TTree *metaDataTree = dynamic_cast<TTree *>(filePtr_->Get(poolNames::metaDataTreeName().c_str()));
    if(!metaDataTree)
      throw edm::Exception(errors::FileReadError) << "Could not find tree " << poolNames::metaDataTreeName()
							 << " in the input file.\n";

    // To keep things simple, we just read in every possible branch that exists.
    // We don't pay attention to which branches exist in which file format versions

    FileFormatVersion *fftPtr = &fileFormatVersion_;
    if(metaDataTree->FindBranch(poolNames::fileFormatVersionBranchName().c_str()) != 0) {
      TBranch *fft = metaDataTree->GetBranch(poolNames::fileFormatVersionBranchName().c_str());
      fft->SetAddress(&fftPtr);
      input::getEntry(fft, 0);
      metaDataTree->SetBranchAddress(poolNames::fileFormatVersionBranchName().c_str(), &fftPtr);
    }

    setRefCoreStreamer(0, !fileFormatVersion().splitProductIDs(), !fileFormatVersion().productIDIsInt()); // backward compatibility

    FileID *fidPtr = &fid_;
    if(metaDataTree->FindBranch(poolNames::fileIdentifierBranchName().c_str()) != 0) {
      metaDataTree->SetBranchAddress(poolNames::fileIdentifierBranchName().c_str(), &fidPtr);
    }

    IndexIntoFile *iifPtr = &indexIntoFile_;
    if(metaDataTree->FindBranch(poolNames::indexIntoFileBranchName().c_str()) != 0) {
      metaDataTree->SetBranchAddress(poolNames::indexIntoFileBranchName().c_str(), &iifPtr);
    }

    // Need to read to a temporary registry so we can do a translation of the BranchKeys.
    // This preserves backward compatibility against friendly class name algorithm changes.
    ProductRegistry inputProdDescReg;
    ProductRegistry *ppReg = &inputProdDescReg;
    metaDataTree->SetBranchAddress(poolNames::productDescriptionBranchName().c_str(),(&ppReg));

    typedef std::map<ParameterSetID, ParameterSetBlob> PsetMap;
    PsetMap psetMap;
    PsetMap *psetMapPtr = &psetMap;
    if(metaDataTree->FindBranch(poolNames::parameterSetMapBranchName().c_str()) != 0) {
      //backward compatibility
      assert(!fileFormatVersion().parameterSetsTree());
      metaDataTree->SetBranchAddress(poolNames::parameterSetMapBranchName().c_str(), &psetMapPtr);
    } else {
      assert(fileFormatVersion().parameterSetsTree());
      TTree* psetTree = dynamic_cast<TTree *>(filePtr_->Get(poolNames::parameterSetsTreeName().c_str()));
      if(0 == psetTree) {
        throw Exception(errors::FileReadError) << "Could not find tree " << poolNames::parameterSetsTreeName()
        << " in the input file.\n";
      }
      typedef std::pair<ParameterSetID, ParameterSetBlob> IdToBlobs;
      IdToBlobs idToBlob;
      IdToBlobs* pIdToBlob = &idToBlob;
      psetTree->SetBranchAddress(poolNames::idToParameterSetBlobsBranchName().c_str(), &pIdToBlob);
      for(Long64_t i = 0; i != psetTree->GetEntries(); ++i) {
        psetTree->GetEntry(i);
        psetMap.insert(idToBlob);
      }
    }

    // backward compatibility
    ProcessHistoryRegistry::collection_type pHistMap;
    ProcessHistoryRegistry::collection_type *pHistMapPtr = &pHistMap;
    if(metaDataTree->FindBranch(poolNames::processHistoryMapBranchName().c_str()) != 0) {
      metaDataTree->SetBranchAddress(poolNames::processHistoryMapBranchName().c_str(), &pHistMapPtr);
    }

    ProcessHistoryRegistry::vector_type pHistVector;
    ProcessHistoryRegistry::vector_type *pHistVectorPtr = &pHistVector;
    if(metaDataTree->FindBranch(poolNames::processHistoryBranchName().c_str()) != 0) {
      metaDataTree->SetBranchAddress(poolNames::processHistoryBranchName().c_str(), &pHistVectorPtr);
    }

    ProcessConfigurationVector procConfigVector;
    ProcessConfigurationVector* procConfigVectorPtr = &procConfigVector;
    if(metaDataTree->FindBranch(poolNames::processConfigurationBranchName().c_str()) != 0) {
      metaDataTree->SetBranchAddress(poolNames::processConfigurationBranchName().c_str(), &procConfigVectorPtr);
    }

    std::auto_ptr<BranchIDListRegistry::collection_type> branchIDListsAPtr(new BranchIDListRegistry::collection_type);
    BranchIDListRegistry::collection_type *branchIDListsPtr = branchIDListsAPtr.get();
    if(metaDataTree->FindBranch(poolNames::branchIDListBranchName().c_str()) != 0) {
      metaDataTree->SetBranchAddress(poolNames::branchIDListBranchName().c_str(), &branchIDListsPtr);
    }

    BranchChildren* branchChildrenBuffer = branchChildren_.get();
    if(metaDataTree->FindBranch(poolNames::productDependenciesBranchName().c_str()) != 0) {
      metaDataTree->SetBranchAddress(poolNames::productDependenciesBranchName().c_str(), &branchChildrenBuffer);
    }

    // backward compatibility
    std::vector<EventProcessHistoryID> *eventHistoryIDsPtr = &eventProcessHistoryIDs_;
    if(metaDataTree->FindBranch(poolNames::eventHistoryBranchName().c_str()) != 0) {
      metaDataTree->SetBranchAddress(poolNames::eventHistoryBranchName().c_str(), &eventHistoryIDsPtr);
    }

    if(metaDataTree->FindBranch(poolNames::moduleDescriptionMapBranchName().c_str()) != 0) {
      if(metaDataTree->GetBranch(poolNames::moduleDescriptionMapBranchName().c_str())->GetSplitLevel() != 0) {
        metaDataTree->SetBranchStatus((poolNames::moduleDescriptionMapBranchName() + ".*").c_str(), 0);
      } else {
        metaDataTree->SetBranchStatus(poolNames::moduleDescriptionMapBranchName().c_str(), 0);
      }
    }

    // Here we read the metadata tree
    input::getEntry(metaDataTree, 0);

    eventProcessHistoryIter_ = eventProcessHistoryIDs_.begin();

    // Here we read the event history tree
    readEventHistoryTree();

    ParameterSetConverter::ParameterSetIdConverter psetIdConverter;
    if(!fileFormatVersion().triggerPathsTracked()) {
      ParameterSetConverter converter(psetMap, psetIdConverter, fileFormatVersion().parameterSetsByReference());
    } else {
      // Merge into the parameter set registry.
      pset::Registry& psetRegistry = *pset::Registry::instance();
      for(PsetMap::const_iterator i = psetMap.begin(), iEnd = psetMap.end(); i != iEnd; ++i) {
        ParameterSet pset(i->second.pset());
        pset.setID(i->first);
        psetRegistry.insertMapped(pset);
      } 
    }
    if(!fileFormatVersion().splitProductIDs()) {
      // Old provenance format input file.  Create a provenance adaptor.
      provenanceAdaptor_.reset(new ProvenanceAdaptor(
	    inputProdDescReg, pHistMap, pHistVector, procConfigVector, psetIdConverter, true));
      // Fill in the branchIDLists branch from the provenance adaptor
      branchIDLists_ = provenanceAdaptor_->branchIDLists();
    } else {
      if(!fileFormatVersion().triggerPathsTracked()) {
        // New provenance format, but change in ParameterSet Format. Create a provenance adaptor.
        provenanceAdaptor_.reset(new ProvenanceAdaptor(
	    inputProdDescReg, pHistMap, pHistVector, procConfigVector, psetIdConverter, false));
      }
      // New provenance format input file. The branchIDLists branch was read directly from the input file. 
      if(metaDataTree->FindBranch(poolNames::branchIDListBranchName().c_str()) == 0) {
	throw edm::Exception(errors::EventCorruption)
	  << "Failed to find branchIDLists branch in metaData tree.\n";
      }
      branchIDLists_.reset(branchIDListsAPtr.release());
    }

    // Merge into the hashed registries.
    ProcessHistoryRegistry::instance()->insertCollection(pHistVector);
    ProcessConfigurationRegistry::instance()->insertCollection(procConfigVector);

    validateFile();

    // Read the parentage tree.  Old format files are handled internally in readParentageTree().
    readParentageTree();

    if (eventSkipperByID) {
      size_t entries = indexIntoFile_.size();
      // Remove runs, lumis, and/or events we do not wish to process by ID.
      indexIntoFile_.erase(std::remove_if(indexIntoFile_.begin(), indexIntoFile_.end(), *eventSkipperByID), indexIntoFile_.end());
      if (entries != indexIntoFile_.size()) {
        whyNotFastClonable_ += FileBlock::EventsOrLumisSelectedByID;
      }
    }

    // Remove any runs containing no lumis.
    for(IndexIntoFile::iterator it = indexIntoFile_.begin(), itEnd = indexIntoFile_.end(); it != itEnd; ++it) {
      if(it->lumi() == 0) {
	assert(it->event() == 0);
	IndexIntoFile::iterator next = it + 1;
	if(next == itEnd || next->lumi() == 0) {
	  *it = IndexIntoFile::Element();
	}
      }
    }
    indexIntoFile_.erase(std::remove(indexIntoFile_.begin(), indexIntoFile_.end(), IndexIntoFile::Element()), indexIntoFile_.end());

    initializeDuplicateChecker(indexesIntoFiles, currentIndexIntoFile);
    if(noEventSort_) indexIntoFile_.sortBy_Index_Run_Lumi_Entry();
    indexIntoFileIter_ = indexIntoFileBegin_ = indexIntoFile_.begin();
    indexIntoFileEnd_ = indexIntoFile_.end();
    forcedRunOffset_ = forcedRunOffset(forcedRunNumber, indexIntoFileBegin_, indexIntoFileEnd_);
    eventProcessHistoryIter_ = eventProcessHistoryIDs_.begin();

    // Set product presence information in the product registry.
    ProductRegistry::ProductList const& pList = inputProdDescReg.productList();
    for(ProductRegistry::ProductList::const_iterator it = pList.begin(), itEnd = pList.end();
        it != itEnd; ++it) {
      BranchDescription const& prod = it->second;
      treePointers_[prod.branchType()]->setPresence(prod);
    }
  
    fillProductRegistryTransients(procConfigVector, inputProdDescReg);

    std::auto_ptr<ProductRegistry> newReg(new ProductRegistry);

    // Do the translation from the old registry to the new one
    {
      ProductRegistry::ProductList const& prodList = inputProdDescReg.productList();
      for(ProductRegistry::ProductList::const_iterator it = prodList.begin(), itEnd = prodList.end();
           it != itEnd; ++it) {
        BranchDescription const& prod = it->second;
        std::string newFriendlyName = friendlyname::friendlyName(prod.className());
	if(newFriendlyName == prod.friendlyClassName()) {
          newReg->copyProduct(prod);
	} else {
          if(fileFormatVersion().splitProductIDs()) {
	    throw edm::Exception(errors::UnimplementedFeature)
	      << "Cannot change friendly class name algorithm without more development work\n"
	      << "to update BranchIDLists.  Contact the framework group.\n";
	  }
          BranchDescription newBD(prod);
          newBD.updateFriendlyClassName();
          newReg->copyProduct(newBD);
	  // Need to call init to get old branch name.
	  prod.init();
	  newBranchToOldBranch_.insert(std::make_pair(newBD.branchName(), prod.branchName()));
	}
      }
      dropOnInput(*newReg, groupSelectorRules, dropDescendants, dropMergeable);
      // freeze the product registry
      newReg->setFrozen();
      productRegistry_.reset(newReg.release());
    }


    // Set up information from the product registry.
    ProductRegistry::ProductList const& prodList = productRegistry()->productList();
    for(ProductRegistry::ProductList::const_iterator it = prodList.begin(), itEnd = prodList.end();
        it != itEnd; ++it) {
      BranchDescription const& prod = it->second;
      treePointers_[prod.branchType()]->addBranch(it->first, prod,
						  newBranchToOldBranch(prod.branchName()));
    }

    // Determine if this file is fast clonable.
    setIfFastClonable(remainingEvents, remainingLumis);

    setRefCoreStreamer(true);  // backward compatibility
  }

  RootFile::~RootFile() {
  }

  void
  RootFile::readEntryDescriptionTree() {
    // Called only for old format files.
    if(!fileFormatVersion().perEventProductIDs()) return; 
    TTree* entryDescriptionTree = dynamic_cast<TTree*>(filePtr_->Get(poolNames::entryDescriptionTreeName().c_str()));
    if(!entryDescriptionTree) 
      throw edm::Exception(errors::FileReadError) << "Could not find tree " << poolNames::entryDescriptionTreeName()
							 << " in the input file.\n";


    EntryDescriptionID idBuffer;
    EntryDescriptionID* pidBuffer = &idBuffer;
    entryDescriptionTree->SetBranchAddress(poolNames::entryDescriptionIDBranchName().c_str(), &pidBuffer);

    EntryDescriptionRegistry& oldregistry = *EntryDescriptionRegistry::instance();

    EventEntryDescription entryDescriptionBuffer;
    EventEntryDescription *pEntryDescriptionBuffer = &entryDescriptionBuffer;
    entryDescriptionTree->SetBranchAddress(poolNames::entryDescriptionBranchName().c_str(), &pEntryDescriptionBuffer);

    // Fill in the parentage registry.
    ParentageRegistry& registry = *ParentageRegistry::instance();

    for(Long64_t i = 0, numEntries = entryDescriptionTree->GetEntries(); i < numEntries; ++i) {
      input::getEntry(entryDescriptionTree, i);
      if(idBuffer != entryDescriptionBuffer.id())
	throw edm::Exception(errors::EventCorruption) << "Corruption of EntryDescription tree detected.\n";
      oldregistry.insertMapped(entryDescriptionBuffer);
      Parentage parents;
      parents.parents() = entryDescriptionBuffer.parents();
      registry.insertMapped(parents);
    }
    entryDescriptionTree->SetBranchAddress(poolNames::entryDescriptionIDBranchName().c_str(), 0);
    entryDescriptionTree->SetBranchAddress(poolNames::entryDescriptionBranchName().c_str(), 0);
  }

  void
  RootFile::readParentageTree()
  { 
    if(!fileFormatVersion().splitProductIDs()) {
      // Old format file.
      readEntryDescriptionTree();
      return;
    }
    // New format file
    TTree* parentageTree = dynamic_cast<TTree*>(filePtr_->Get(poolNames::parentageTreeName().c_str()));
    if(!parentageTree) 
      throw edm::Exception(errors::FileReadError) << "Could not find tree " << poolNames::parentageTreeName()
							 << " in the input file.\n";

    Parentage parentageBuffer;
    Parentage *pParentageBuffer = &parentageBuffer;
    parentageTree->SetBranchAddress(poolNames::parentageBranchName().c_str(), &pParentageBuffer);

    ParentageRegistry& registry = *ParentageRegistry::instance();

    for(Long64_t i = 0, numEntries = parentageTree->GetEntries(); i < numEntries; ++i) {
      input::getEntry(parentageTree, i);
      registry.insertMapped(parentageBuffer);
    }
    parentageTree->SetBranchAddress(poolNames::parentageBranchName().c_str(), 0);
  }

  void
  RootFile::setIfFastClonable(int remainingEvents, int remainingLumis) {
    if(!fileFormatVersion().splitProductIDs()) {
      whyNotFastClonable_ += FileBlock::FileTooOld;
      return;
    }
    if(processingMode_ != InputSource::RunsLumisAndEvents) {
      whyNotFastClonable_ += FileBlock::NotProcessingEvents;
      return;
    }
    // Find entry for first event in file
    IndexIntoFile::const_iterator it = indexIntoFileBegin_;
    while(it != indexIntoFileEnd_ && it->getEntryType() != IndexIntoFile::kEvent) {
      ++it;
    }
    if(it == indexIntoFileEnd_) {
      whyNotFastClonable_ += FileBlock::NoEventsInFile;
      return;
    }

    // From here on, record all reasons we can't fast clone.
    if(!indexIntoFile_.allEventsInEntryOrder()) {
      whyNotFastClonable_ += FileBlock::EventsToBeSorted;
    }
    if(skipAnyEvents_) {
      whyNotFastClonable_ += FileBlock::InitialEventsSkipped;
    }
    if(remainingEvents >= 0 && eventTree_.entries() > remainingEvents) {
      whyNotFastClonable_ += FileBlock::MaxEventsTooSmall;
    }
    if(remainingLumis >= 0 && lumiTree_.entries() > remainingLumis) {
      whyNotFastClonable_ += FileBlock::MaxLumisTooSmall;
    }
    // We no longer fast copy the EventAuxiliary branch, so there
    // is no longer any need to disable fast copying because the run
    // number is being modified.   Also, this check did not work anyway
    // because this function is called before forcedRunOffset_ is set.

    // if(forcedRunOffset_ != 0) { 
    //   whyNotFastClonable_ += FileBlock::RunNumberModified;
    // }
    if(duplicateChecker_.get() != 0) {
      if(!duplicateChecker_->fastCloningOK()) {
        whyNotFastClonable_ += FileBlock::DuplicateEventsRemoved;
      }
    }
  }

  boost::shared_ptr<FileBlock>
  RootFile::createFileBlock() const {
    return boost::shared_ptr<FileBlock>(new FileBlock(fileFormatVersion(),
						     eventTree_.tree(),
						     eventTree_.metaTree(),
						     lumiTree_.tree(),
						     lumiTree_.metaTree(),
						     runTree_.tree(),
						     runTree_.metaTree(),
						     whyNotFastClonable(),
						     file_,
						     branchChildren_));
  }

  std::string const&
  RootFile::newBranchToOldBranch(std::string const& newBranch) const {
    std::map<std::string, std::string>::const_iterator it = newBranchToOldBranch_.find(newBranch);
    if(it != newBranchToOldBranch_.end()) {
      return it->second;
    }
    return newBranch;
  }

  IndexIntoFile::EntryType
  RootFile::getEntryType() const {
    if(indexIntoFileIter_ == indexIntoFileEnd_) {
      return IndexIntoFile::kEnd;
    }
    return indexIntoFileIter_->getEntryType();
  }

  IndexIntoFile::const_iterator
  RootFile::indexIntoFileIter() const {
    assert(indexIntoFileIter_ != indexIntoFileEnd_);
    return indexIntoFileIter_;
  }

  // Temporary KLUDGE until we can properly merge runs and lumis across files
  // This KLUDGE skips duplicate run or lumi entries.
  IndexIntoFile::EntryType
  RootFile::getEntryTypeSkippingDups() {
    if(indexIntoFileIter_ == indexIntoFileEnd_) {
      return IndexIntoFile::kEnd;
    }
    if(indexIntoFileIter_->event() == 0 && indexIntoFileIter_ != indexIntoFileBegin_) {
      if((indexIntoFileIter_-1)->run() == indexIntoFileIter_->run() && (indexIntoFileIter_-1)->lumi() == indexIntoFileIter_->lumi()) {
	++indexIntoFileIter_;
	return getEntryTypeSkippingDups();
      } 
    }
    return indexIntoFileIter_->getEntryType();
  }

  bool
  RootFile::isDuplicateEvent() const {
    assert (indexIntoFileIter_->getEntryType() == IndexIntoFile::kEvent);
    return duplicateChecker_.get() != 0 &&
      duplicateChecker_->isDuplicateAndCheckActive(indexIntoFileIter_->processHistoryIDIndex(),
	EventID(indexIntoFileIter_->run(), indexIntoFileIter_->lumi(), indexIntoFileIter_->event()), file_);
  }

  IndexIntoFile::EntryType
  RootFile::getNextEntryTypeWanted() {
    IndexIntoFile::EntryType entryType = getEntryTypeSkippingDups();
    if(entryType == IndexIntoFile::kEnd) {
      return IndexIntoFile::kEnd;
    }
    if(entryType == IndexIntoFile::kRun) {
      return IndexIntoFile::kRun;
    } else if(processingMode_ == InputSource::Runs) {
      indexIntoFileIter_ = indexIntoFile_.findNextRun(indexIntoFileIter_);      
      return getNextEntryTypeWanted();
    }
    if(entryType == IndexIntoFile::kLumi) {
      return IndexIntoFile::kLumi;
    } else if(processingMode_ == InputSource::RunsAndLumis) {
      indexIntoFileIter_ = indexIntoFile_.findNextLumiOrRun(indexIntoFileIter_);      
      return getNextEntryTypeWanted();
    }
    if(isDuplicateEvent()) {
      ++indexIntoFileIter_;
      return getNextEntryTypeWanted();
    }
    return IndexIntoFile::kEvent;
  }

  namespace {
    typedef long long  EntryNumber_t;
    struct RunItem {
      RunItem(ProcessHistoryID const& phid, RunNumber_t const& run, EntryNumber_t const& entry) :
	phid_(phid), run_(run), entry_(entry) {}
      ProcessHistoryID phid_;
      RunNumber_t run_;
      EntryNumber_t entry_;
    };
    struct LumiItem {
      LumiItem(ProcessHistoryID const& phid, RunNumber_t const& run,
		 LuminosityBlockNumber_t const& lumi, EntryNumber_t const& entry) :
	phid_(phid), run_(run), lumi_(lumi), entry_(entry) {}
      ProcessHistoryID phid_;
      RunNumber_t run_;
      LuminosityBlockNumber_t lumi_;
      EntryNumber_t entry_;
    };
    struct EventItem {
      EventItem(ProcessHistoryID const& phid, RunNumber_t const& run,
		 LuminosityBlockNumber_t const& lumi, EventNumber_t const& event,
		 EntryNumber_t const& entry) :
	phid_(phid), run_(run), lumi_(lumi), event_(event), entry_(entry) {}
      ProcessHistoryID phid_;
      RunNumber_t run_;
      LuminosityBlockNumber_t lumi_;
      EventNumber_t event_;
      EntryNumber_t entry_;
    };
  }

  void
  RootFile::fillIndexIntoFile() {
    // This function is for backward compatibility only.
    // If reading a current format file, indexIntoFile_ is read from the input file.
    //
    typedef std::map<RunNumber_t, RunItem> RunMap;
    typedef std::map<LuminosityBlockID, LumiItem> LumiMap;

    std::vector<RunNumber_t> runs;
    RunMap runMap;
    std::vector<LuminosityBlockID> lumis;
    LumiMap lumiMap;
    std::vector<EventItem> events;
    
    // Loop over run entries and fill information from the run auxiliary
    if (runTree_.isValid()) {
      runs.reserve(runTree_.entries());
      while(runTree_.next()) {
        boost::shared_ptr<RunAuxiliary> runAuxiliary = fillRunAuxiliary();
	runs.push_back(runAuxiliary->run());
        runMap.insert(std::make_pair(runAuxiliary->run(), RunItem(runAuxiliary->processHistoryID(), runAuxiliary->run(), runTree_.entryNumber())));
      }
      runTree_.setEntryNumber(-1);
    }

    // Loop over luminosity block entries and fill information from the lumi auxiliary
    if(lumiTree_.isValid()) {
      lumis.reserve(lumiTree_.entries());
      while(lumiTree_.next()) {
        boost::shared_ptr<LuminosityBlockAuxiliary> lumiAux = fillLumiAuxiliary();
	LuminosityBlockID lbid(lumiAux->run(), lumiAux->luminosityBlock());
	lumis.push_back(lbid);
        lumiMap.insert(std::make_pair(lbid,
	  LumiItem(lumiAux->processHistoryID(), lumiAux->run(), lumiAux->luminosityBlock(), lumiTree_.entryNumber())));
      }
      lumiTree_.setEntryNumber(-1);
    }

    // Loop over event entries and fill information from the event auxiliary and event history
    LuminosityBlockNumber_t lastLumi = 0;
    RunNumber_t lastRun = 0;
    ProcessHistoryID lastPhid;

    events.reserve(eventTree_.entries());
    while(eventTree_.next()) {
      fillEventAuxiliary();
      fillHistory();
      events.push_back(EventItem(history_->processHistoryID(),
                              eventAux().run(),
                              eventAux().luminosityBlock(),
                              eventAux().event(),
                              eventTree_.entryNumber()));

      bool newPhid = (lastPhid != history_->processHistoryID());
      bool newRun = (lastRun != eventAux().run());
      bool newLumi = newRun || (lastLumi != eventAux().luminosityBlock());

      assert(newRun || !newPhid);

      if (newPhid) {
	lastPhid = history_->processHistoryID();
      }
      if (newRun) {
        lastRun = eventAux().run();
        if(runTree_.isValid()) {
          // If the run tree is valid, use the event tree to set the run process history ID
	  RunMap::iterator it = runMap.find(lastRun);
	  assert (it != runMap.end());
	  it->second.phid_ = lastPhid;
	} else {
          // If the run tree is invalid, use the event tree to add run entries.
	  runs.push_back(lastRun);
          runMap.insert(std::make_pair(lastRun, RunItem(lastPhid, lastRun, -1LL)));
	}
      }
      if (newLumi) {
        lastLumi = eventAux().luminosityBlock();
	LuminosityBlockID lbid(lastRun, lastLumi);
        if(lumiTree_.isValid()) {
          // If the lumi tree is valid, use the event tree to set the lumi process history ID
	  LumiMap::iterator it = lumiMap.find(lbid);
	  assert (it != lumiMap.end());
	  it->second.phid_ = lastPhid;
	} else {
          // If the lumi tree is invalid, use the event tree to add lumi entries.
	  lumis.push_back(lbid);
          lumiMap.insert(std::make_pair(lbid, LumiItem(lastPhid, lastRun, lastLumi, -1LL)));
	}
      }
    }
    eventTree_.setEntryNumber(-1);
    eventProcessHistoryIter_ = eventProcessHistoryIDs_.begin();

    // Loop over run entries and fill the index.
    // We do runs first because there can be runs with no events
    // and the order ProcessHistoryIDs are first encountered matters.
    // We want to preserve the order in the input, not put the ProcessHistoryIDs
    // associated with runs with no events last.
    for (std::vector<RunNumber_t>::const_iterator i = runs.begin(), iEnd = runs.end(); i != iEnd; ++i) {
       RunMap::iterator it = runMap.find(*i);
       assert (it != runMap.end());
       indexIntoFile_.addEntry(it->second.phid_, it->second.run_, 0U, 0U, it->second.entry_);
    }

    // Loop over luminosity block entries and fill the index
    for (std::vector<LuminosityBlockID>::const_iterator i = lumis.begin(), iEnd = lumis.end(); i != iEnd; ++i) {
       LumiMap::iterator it = lumiMap.find(*i);
       assert (it != lumiMap.end());
       indexIntoFile_.addEntry(it->second.phid_, it->second.run_, it->second.lumi_, 0U, it->second.entry_);
    }

    // Loop over event entries and fill the index
    for (std::vector<EventItem>::const_iterator i = events.begin(), iEnd = events.end(); i != iEnd; ++i) {
       indexIntoFile_.addEntry(i->phid_, i->run_, i->lumi_, i->event_, i->entry_);
    }

    // We want the ProcessHistoryIDs in the order encountered in all the
    // input files, not in the order encountered in this particular input file.
    indexIntoFile_.fixIndexes(orderedProcessHistoryIDs_);

    indexIntoFile_.sortBy_Index_Run_Lumi_Event();
  }

  void
  RootFile::validateFile() {
    if(!fid_.isValid()) {
      fid_ = FileID(createGlobalIdentifier());
    }
    if(!eventTree_.isValid()) {
      throw edm::Exception(errors::EventCorruption) <<
	 "'Events' tree is corrupted or not present\n" << "in the input file.\n";
    }
    if (indexIntoFile_.empty()) {
      fillIndexIntoFile();
    }
  }

  void
  RootFile::reportOpened(std::string const& inputType) {
    // Report file opened.
    std::string const label = "source";
    std::string moduleName = "PoolSource";
    Service<JobReport> reportSvc;
    reportToken_ = reportSvc->inputFileOpened(file_,
               logicalFile_,
               catalog_,
               inputType,
               moduleName,
               label,
	       fid_.fid(),
               eventTree_.branchNames()); 
  }

  void
  RootFile::close(bool reallyClose) {
    if(reallyClose) {
      // Just to play it safe, zero all pointers to objects in the TFile to be closed.
      eventHistoryTree_ = 0;
      for(RootTreePtrArray::iterator it = treePointers_.begin(), itEnd = treePointers_.end(); it != itEnd; ++it) {
	(*it)->close();
	(*it) = 0;
      }
      filePtr_->Close();
      filePtr_.reset();
    }
    Service<JobReport> reportSvc;
    reportSvc->inputFileClosed(reportToken_);
  }

  void
  RootFile::fillEventAuxiliary() {
    if(fileFormatVersion().newAuxiliary()) {
      EventAuxiliary *pEvAux = &eventAux_;
      eventTree_.fillAux<EventAuxiliary>(pEvAux);
    } else {
      // for backward compatibility.
      EventAux eventAux;
      EventAux *pEvAux = &eventAux;
      eventTree_.fillAux<EventAux>(pEvAux);
      conversion(eventAux, eventAux_);
    }
  }

  void
  RootFile::fillHistory() {
    // We could consider doing delayed reading, but because we have to
    // store this History object in a different tree than the event
    // data tree, this is too hard to do in this first version.

    if(fileFormatVersion().eventHistoryTree()) {
      History* pHistory = history_.get();
      TBranch* eventHistoryBranch = eventHistoryTree_->GetBranch(poolNames::eventHistoryBranchName().c_str());
      if(!eventHistoryBranch)
	throw edm::Exception(errors::EventCorruption)
	  << "Failed to find history branch in event history tree.\n";
      eventHistoryBranch->SetAddress(&pHistory);
      input::getEntry(eventHistoryTree_, eventTree_.entryNumber());
    } else {
      // for backward compatibility.
      if(eventProcessHistoryIDs_.empty()) {
	history_->setProcessHistoryID(eventAux().processHistoryID());
      } else {
	// Lumi block number was not in EventID for the relevant releases.
        EventID id(eventAux().id().run(), 0, eventAux().id().event());        
        if(eventProcessHistoryIter_->eventID() != id) {
          EventProcessHistoryID target(id, ProcessHistoryID());
          eventProcessHistoryIter_ = lower_bound_all(eventProcessHistoryIDs_, target);	
          assert(eventProcessHistoryIter_->eventID() == id);
        }
	history_->setProcessHistoryID(eventProcessHistoryIter_->processHistoryID());
        ++eventProcessHistoryIter_;
      }
    }
    if(provenanceAdaptor_) {
      history_->setProcessHistoryID(provenanceAdaptor_->convertID(history_->processHistoryID()));
      EventSelectionIDVector& esids = history_->eventSelectionIDs(); 
      for(EventSelectionIDVector::iterator i = esids.begin(), e = esids.end(); i != e; ++i) {
	(*i) = provenanceAdaptor_->convertID(*i);
      }
    }
    if(!fileFormatVersion().splitProductIDs()) {
      // old format.  branchListIndexes_ must be filled in from the ProvenanceAdaptor.
      provenanceAdaptor_->branchListIndexes(history_->branchListIndexes());
    }
  }

  boost::shared_ptr<LuminosityBlockAuxiliary>
  RootFile::fillLumiAuxiliary() {
    boost::shared_ptr<LuminosityBlockAuxiliary> lumiAuxiliary(new LuminosityBlockAuxiliary);
    if(fileFormatVersion().newAuxiliary()) {
      LuminosityBlockAuxiliary *pLumiAux = lumiAuxiliary.get();
      lumiTree_.fillAux<LuminosityBlockAuxiliary>(pLumiAux);
    } else {
      LuminosityBlockAux lumiAux;
      LuminosityBlockAux *pLumiAux = &lumiAux;
      lumiTree_.fillAux<LuminosityBlockAux>(pLumiAux);
      conversion(lumiAux, *lumiAuxiliary);
    }
    if(provenanceAdaptor_) {
      lumiAuxiliary->setProcessHistoryID(provenanceAdaptor_->convertID(lumiAuxiliary->processHistoryID()));
    }
    if(lumiAuxiliary->luminosityBlock() == 0 && !fileFormatVersion().runsAndLumis()) {
      lumiAuxiliary->id() = LuminosityBlockID(RunNumber_t(1), LuminosityBlockNumber_t(1));
    }
    return lumiAuxiliary;
  }

  boost::shared_ptr<RunAuxiliary>
  RootFile::fillRunAuxiliary() {
    boost::shared_ptr<RunAuxiliary> runAuxiliary(new RunAuxiliary);
    if(fileFormatVersion().newAuxiliary()) {
      RunAuxiliary *pRunAux = runAuxiliary.get();
      runTree_.fillAux<RunAuxiliary>(pRunAux);
    } else {
      RunAux runAux;
      RunAux *pRunAux = &runAux;
      runTree_.fillAux<RunAux>(pRunAux);
      conversion(runAux, *runAuxiliary);
    }
    if(provenanceAdaptor_) {
      runAuxiliary->setProcessHistoryID(provenanceAdaptor_->convertID(runAuxiliary->processHistoryID()));
    }
    return runAuxiliary;
  }

  bool
  RootFile::skipEvents(int& offset) {
    for(;offset > 0 && indexIntoFileIter_ != indexIntoFileEnd_; ++indexIntoFileIter_) {
      if(indexIntoFileIter_->getEntryType() == IndexIntoFile::kEvent) {
	if(isDuplicateEvent()) {
	  continue;
	}
        --offset;
      }
    }
    while(offset < 0 && indexIntoFileIter_ != indexIntoFileBegin_) {
      --indexIntoFileIter_;
      if(indexIntoFileIter_->getEntryType() == IndexIntoFile::kEvent) {
	if(isDuplicateEvent()) {
	  continue;
	}
        ++offset;
      }
    }
    while(indexIntoFileIter_ != indexIntoFileEnd_ && indexIntoFileIter_->getEntryType() != IndexIntoFile::kEvent) {
      ++indexIntoFileIter_;
    }

    eventTree_.resetTraining();

    return (indexIntoFileIter_ == indexIntoFileEnd_);
  }

  // readEvent() is responsible for creating, and setting up, the
  // EventPrincipal.
  //
  //   1. create an EventPrincipal with a unique EventID
  //   2. For each entry in the provenance, put in one Group,
  //      holding the Provenance for the corresponding EDProduct.
  //   3. set up the caches in the EventPrincipal to know about this
  //      Group.
  //
  // We do *not* create the EDProduct instance (the equivalent of reading
  // the branch containing this EDProduct. That will be done by the Delayed Reader,
  //  when it is asked to do so.
  //
  EventPrincipal*
  RootFile::readEvent(EventPrincipal& cache, boost::shared_ptr<LuminosityBlockPrincipal> lb) {
    assert(indexIntoFileIter_ != indexIntoFileEnd_);
    assert(indexIntoFileIter_->getEntryType() == IndexIntoFile::kEvent);
    // Set the entry in the tree, and read the event at that entry.
    eventTree_.setEntryNumber(indexIntoFileIter_->entry()); 
    EventPrincipal* ep = readCurrentEvent(cache, lb);

    assert(ep != 0);
    assert(eventAux().run() == indexIntoFileIter_->run() + forcedRunOffset_);
    assert(eventAux().luminosityBlock() == indexIntoFileIter_->lumi());
    assert(eventAux().event() == indexIntoFileIter_->event());

    ++indexIntoFileIter_;
    return ep;
  }

  // Reads event at the current entry in the tree.
  // Note: This function neither uses nor sets indexIntoFileIter_.
  EventPrincipal*
  RootFile::readCurrentEvent(EventPrincipal& cache, boost::shared_ptr<LuminosityBlockPrincipal> lb) {
    if(!eventTree_.current()) {
      return 0;
    }
    fillEventAuxiliary();
    if(!fileFormatVersion().lumiInEventID()) {
	//ugly, but will disappear when the backward compatibility is done with schema evolution.
	const_cast<EventID&>(eventAux_.id()).setLuminosityBlockNumber(eventAux_.oldLuminosityBlock());
	eventAux_.resetObsoleteInfo();
    }
    fillHistory();
    overrideRunNumber(eventAux_.id(), eventAux().isRealData());

    std::auto_ptr<EventAuxiliary> aux(new EventAuxiliary(eventAux()));
    // We're not done ... so prepare the EventPrincipal
    cache.fillEventPrincipal(aux,
			     lb,
			     history_,
			     makeBranchMapper(eventTree_, InEvent),
			     eventTree_.makeDelayedReader(fileFormatVersion()));

    // report event read from file
    Service<JobReport> reportSvc;
    reportSvc->eventReadFromFile(reportToken_, eventID().run(), eventID().event());
    return &cache;
  }

  void
  RootFile::setAtEventEntry(IndexIntoFile::EntryNumber_t entry) {
    eventTree_.setEntryNumber(entry);
  }

  boost::shared_ptr<RunAuxiliary>
  RootFile::readRunAuxiliary_() {
    assert(indexIntoFileIter_ != indexIntoFileEnd_);
    assert(indexIntoFileIter_->getEntryType() == IndexIntoFile::kRun);
    // Begin code for backward compatibility before the existence of run trees.
    if(!runTree_.isValid()) {
      // prior to the support of run trees.
      // RunAuxiliary did not contain a valid timestamp.  Take it from the next event.
      if(eventTree_.next()) {
        fillEventAuxiliary();
        // back up, so event will not be skipped.
        eventTree_.previous();
      }
      RunID run = RunID(indexIntoFileIter_->run());
      overrideRunNumber(run);
      return boost::shared_ptr<RunAuxiliary>(new RunAuxiliary(run.run(), eventAux().time(), Timestamp::invalidTimestamp()));
    }
    // End code for backward compatibility before the existence of run trees.
    runTree_.setEntryNumber(indexIntoFileIter_->entry()); 
    boost::shared_ptr<RunAuxiliary> runAuxiliary = fillRunAuxiliary();
    assert(runAuxiliary->run() == indexIntoFileIter_->run());
    overrideRunNumber(runAuxiliary->id());
    Service<JobReport> reportSvc;
    reportSvc->reportInputRunNumber(runAuxiliary->run());
    if(runAuxiliary->beginTime() == Timestamp::invalidTimestamp()) {
      // RunAuxiliary did not contain a valid timestamp.  Take it from the next event.
      if(eventTree_.next()) {
        fillEventAuxiliary();
        // back up, so event will not be skipped.
        eventTree_.previous();
      }
      runAuxiliary->setBeginTime(eventAux().time()); 
      runAuxiliary->setEndTime(Timestamp::invalidTimestamp());
    }
    if(fileFormatVersion().processHistorySameWithinRun()) {
      ProcessHistoryID phid = indexIntoFile_.processHistoryID(indexIntoFileIter_->processHistoryIDIndex());
      assert(runAuxiliary->processHistoryID() == phid);
    } else {
      ProcessHistoryID phid = indexIntoFile_.processHistoryID(indexIntoFileIter_->processHistoryIDIndex());
      runAuxiliary->setProcessHistoryID(phid);
    }
    return runAuxiliary;
  }

  boost::shared_ptr<RunPrincipal>
  RootFile::readRun_(boost::shared_ptr<RunPrincipal> rpCache) {
    assert(indexIntoFileIter_ != indexIntoFileEnd_);
    assert(indexIntoFileIter_->getEntryType() == IndexIntoFile::kRun);
    // Begin code for backward compatibility before the existence of run trees.
    if(!runTree_.isValid()) {
      ++indexIntoFileIter_;
      return rpCache;
    }
    // End code for backward compatibility before the existence of run trees.
    rpCache->fillRunPrincipal(makeBranchMapper(runTree_, InRun), runTree_.makeDelayedReader(fileFormatVersion()));
    // Read in all the products now.
    rpCache->readImmediate();
    ++indexIntoFileIter_;
    return rpCache;
  }

  boost::shared_ptr<LuminosityBlockAuxiliary>
  RootFile::readLuminosityBlockAuxiliary_() {
    assert(indexIntoFileIter_ != indexIntoFileEnd_);
    assert(indexIntoFileIter_->getEntryType() == IndexIntoFile::kLumi);
    // Begin code for backward compatibility before the existence of lumi trees.
    if(!lumiTree_.isValid()) {
      if(eventTree_.next()) {
        fillEventAuxiliary();
        // back up, so event will not be skipped.
        eventTree_.previous();
      }

      LuminosityBlockID lumi = LuminosityBlockID(indexIntoFileIter_->run(), indexIntoFileIter_->lumi());
      overrideRunNumber(lumi);
      return boost::shared_ptr<LuminosityBlockAuxiliary>(new LuminosityBlockAuxiliary(lumi.run(), lumi.luminosityBlock(), eventAux().time(), Timestamp::invalidTimestamp()));
    }
    // End code for backward compatibility before the existence of lumi trees.
    lumiTree_.setEntryNumber(indexIntoFileIter_->entry()); 
    boost::shared_ptr<LuminosityBlockAuxiliary> lumiAuxiliary = fillLumiAuxiliary();
    assert(lumiAuxiliary->run() == indexIntoFileIter_->run());
    assert(lumiAuxiliary->luminosityBlock() == indexIntoFileIter_->lumi());
    overrideRunNumber(lumiAuxiliary->id());
    Service<JobReport> reportSvc;
    reportSvc->reportInputLumiSection(lumiAuxiliary->run(), lumiAuxiliary->luminosityBlock());

    if(lumiAuxiliary->beginTime() == Timestamp::invalidTimestamp()) {
      // LuminosityBlockAuxiliary did not contain a timestamp. Take it from the next event.
      if(eventTree_.next()) {
        fillEventAuxiliary();
        // back up, so event will not be skipped.
        eventTree_.previous();
      }
      lumiAuxiliary->setBeginTime(eventAux().time());
      lumiAuxiliary->setEndTime(Timestamp::invalidTimestamp());
    }
    if(fileFormatVersion().processHistorySameWithinRun()) {
      ProcessHistoryID phid = indexIntoFile_.processHistoryID(indexIntoFileIter_->processHistoryIDIndex());
      assert(lumiAuxiliary->processHistoryID() == phid);
    } else {
      ProcessHistoryID phid = indexIntoFile_.processHistoryID(indexIntoFileIter_->processHistoryIDIndex());
      lumiAuxiliary->setProcessHistoryID(phid);
    }
    return lumiAuxiliary;
  }

  boost::shared_ptr<LuminosityBlockPrincipal>
  RootFile::readLumi(boost::shared_ptr<LuminosityBlockPrincipal> lbCache) {
    assert(indexIntoFileIter_ != indexIntoFileEnd_);
    assert(indexIntoFileIter_->getEntryType() == IndexIntoFile::kLumi);
    // Begin code for backward compatibility before the existence of lumi trees.
    if(!lumiTree_.isValid()) {
      ++indexIntoFileIter_;
      return lbCache;
    }
    // End code for backward compatibility before the existence of lumi trees.
    lumiTree_.setEntryNumber(indexIntoFileIter_->entry()); 
    lbCache->fillLuminosityBlockPrincipal(makeBranchMapper(lumiTree_, InLumi),
					 lumiTree_.makeDelayedReader(fileFormatVersion()));
    // Read in all the products now.
    lbCache->readImmediate();
    ++indexIntoFileIter_;
    return lbCache;
  }

  bool
  RootFile::setEntryAtEvent(RunNumber_t run, LuminosityBlockNumber_t lumi, EventNumber_t event) {
    indexIntoFileIter_ = indexIntoFile_.findEventPosition(run, lumi, event);
    if(indexIntoFileIter_ == indexIntoFileEnd_) return false;
    eventTree_.setEntryNumber(indexIntoFileIter_->entry());
    return true;
  }

  bool
  RootFile::setEntryAtEventEntry(RunNumber_t run, LuminosityBlockNumber_t lumi, EventNumber_t event, IndexIntoFile::EntryNumber_t entry) {
    indexIntoFileIter_ = indexIntoFile_.findEventEntryPosition(run, lumi, event, entry);
    if(indexIntoFileIter_ == indexIntoFileEnd_) return false;
    eventTree_.setEntryNumber(indexIntoFileIter_->entry());
    return true;
  }

  bool
  RootFile::setEntryAtLumi(RunNumber_t run, LuminosityBlockNumber_t lumi) {
    indexIntoFileIter_ = indexIntoFile_.findLumiPosition(run, lumi);
    if(indexIntoFileIter_ == indexIntoFileEnd_) return false;
    lumiTree_.setEntryNumber(indexIntoFileIter_->entry());
    return true;
  }

  bool
  RootFile::setEntryAtRun(RunNumber_t run) {
    indexIntoFileIter_ = indexIntoFile_.findRunPosition(run);
    if(indexIntoFileIter_ == indexIntoFileEnd_) return false;
    runTree_.setEntryNumber(indexIntoFileIter_->entry());
    return true;
  }

  void
  RootFile::overrideRunNumber(RunID& id) {
    if(forcedRunOffset_ != 0) {
      id = RunID(id.run() + forcedRunOffset_);
    } 
    if(id < RunID::firstValidRun()) id = RunID::firstValidRun();
  }

  void
  RootFile::overrideRunNumber(LuminosityBlockID& id) {
    if(forcedRunOffset_ != 0) {
      id = LuminosityBlockID(id.run() + forcedRunOffset_, id.luminosityBlock());
    } 
    if(RunID(id.run()) < RunID::firstValidRun()) id = LuminosityBlockID(RunID::firstValidRun().run(), id.luminosityBlock());
  }

  void
  RootFile::overrideRunNumber(EventID& id, bool isRealData) {
    if(forcedRunOffset_ != 0) {
      if(isRealData) {
        throw edm::Exception(errors::Configuration,"RootFile::RootFile()")
          << "The 'setRunNumber' parameter of PoolSource cannot be used with real data.\n";
      }
      id = EventID(id.run() + forcedRunOffset_, id.luminosityBlock(), id.event());
    } 
    if(RunID(id.run()) < RunID::firstValidRun()) {
      id = EventID(RunID::firstValidRun().run(), LuminosityBlockID::firstValidLuminosityBlock().luminosityBlock(),id.event());
    }
  }

  
  void
  RootFile::readEventHistoryTree() {
    // Read in the event history tree, if we have one...
    if(fileFormatVersion().eventHistoryTree()) {
      eventHistoryTree_ = dynamic_cast<TTree*>(filePtr_->Get(poolNames::eventHistoryTreeName().c_str()));
      if(!eventHistoryTree_) {
        throw edm::Exception(errors::EventCorruption)
	  << "Failed to find the event history tree.\n";
      }
    }
  }

  void
  RootFile::initializeDuplicateChecker(
    std::vector<boost::shared_ptr<IndexIntoFile> > const& indexesIntoFiles,
    std::vector<boost::shared_ptr<IndexIntoFile> >::size_type currentIndexIntoFile) {
    if(duplicateChecker_.get() != 0) {
      if(eventTree_.next()) {
        fillEventAuxiliary();
        duplicateChecker_->inputFileOpened(eventAux().isRealData(),
                                           indexIntoFile_,
                                           indexesIntoFiles,
                                           currentIndexIntoFile);
      }
      eventTree_.setEntryNumber(-1);
    }
  }

  void
  RootFile::dropOnInput (ProductRegistry& reg, GroupSelectorRules const& rules, bool dropDescendants, bool dropMergeable) {
    // This is the selector for drop on input.
    GroupSelector groupSelector;
    groupSelector.initialize(rules, reg.allBranchDescriptions());

    ProductRegistry::ProductList& prodList = reg.productListUpdator();
    // Do drop on input. On the first pass, just fill in a set of branches to be dropped.
    std::set<BranchID> branchesToDrop;
    for(ProductRegistry::ProductList::const_iterator it = prodList.begin(), itEnd = prodList.end();
        it != itEnd; ++it) {
      BranchDescription const& prod = it->second;
      if(!groupSelector.selected(prod)) {
        if(dropDescendants) {
          branchChildren_->appendToDescendants(prod.branchID(), branchesToDrop);
        } else {
          branchesToDrop.insert(prod.branchID());
        }
      }
    }

    // On this pass, actually drop the branches.
    std::set<BranchID>::const_iterator branchesToDropEnd = branchesToDrop.end();
    for(ProductRegistry::ProductList::iterator it = prodList.begin(), itEnd = prodList.end(); it != itEnd;) {
      BranchDescription const& prod = it->second;
      bool drop = branchesToDrop.find(prod.branchID()) != branchesToDropEnd;
      if(drop) {
	if(groupSelector.selected(prod)) {
          LogWarning("RootFile")
            << "Branch '" << prod.branchName() << "' is being dropped from the input\n"
            << "of file '" << file_ << "' because it is dependent on a branch\n" 
            << "that was explicitly dropped.\n";
	}
        treePointers_[prod.branchType()]->dropBranch(newBranchToOldBranch(prod.branchName()));
        ProductRegistry::ProductList::iterator icopy = it;
        ++it;
        prodList.erase(icopy);
      } else {
        ++it;
      }
    }

    // Drop on input mergeable run and lumi products, this needs to be invoked for
    // secondary file input
    if(dropMergeable) {
      for(ProductRegistry::ProductList::iterator it = prodList.begin(), itEnd = prodList.end(); it != itEnd;) {
        BranchDescription const& prod = it->second;
        if(prod.branchType() != InEvent) {
          TClass *cp = gROOT->GetClass(prod.wrappedName().c_str());
          boost::shared_ptr<EDProduct> dummy(static_cast<EDProduct *>(cp->New()));
          if(dummy->isMergeable()) {
            treePointers_[prod.branchType()]->dropBranch(newBranchToOldBranch(prod.branchName()));
            ProductRegistry::ProductList::iterator icopy = it;
            ++it;
            prodList.erase(icopy);
          } else {
            ++it;
          }
        }
        else ++it;
      }
    }
  }

  // backward compatibility

  namespace {
    boost::shared_ptr<BranchMapper>
    makeBranchMapperInRelease180(RootTree& rootTree, BranchType const& type, ProductRegistry const& preg) {
      boost::shared_ptr<BranchMapperWithReader> mapper(new BranchMapperWithReader());
      mapper->setDelayedRead(false);

      for(ProductRegistry::ProductList::const_iterator it = preg.productList().begin(),
          itEnd = preg.productList().end(); it != itEnd; ++it) {
        if(type == it->second.branchType() && !it->second.transient()) {
	  TBranch *br = rootTree.branches().find(it->first)->second.provenanceBranch_;
	  std::auto_ptr<BranchEntryDescription> pb(new BranchEntryDescription);
	  BranchEntryDescription* ppb = pb.get();
	  br->SetAddress(&ppb);
	  input::getEntry(br, rootTree.entryNumber());
	  ProductStatus status = (ppb->creatorStatus() == BranchEntryDescription::Success ? productstatus::present() : productstatus::neverCreated());
	  // Not providing parentage!!!
	  ProductProvenance entry(it->second.branchID(), status, ParentageID());
	  mapper->insert(entry);
	  mapper->insertIntoMap(it->second.oldProductID(), it->second.branchID());
        }
      }
      return mapper;
    }

    boost::shared_ptr<BranchMapper>
    makeBranchMapperInRelease200(RootTree& rootTree, BranchType const& type, ProductRegistry const& preg) {
      rootTree.fillStatus();
      boost::shared_ptr<BranchMapperWithReader> mapper(new BranchMapperWithReader());
      mapper->setDelayedRead(false);
      for(ProductRegistry::ProductList::const_iterator it = preg.productList().begin(),
          itEnd = preg.productList().end(); it != itEnd; ++it) {
        if(type == it->second.branchType() && !it->second.transient()) {
	  std::vector<ProductStatus>::size_type index = it->second.oldProductID().oldID() - 1;
	  // Not providing parentage!!!
	  ProductProvenance entry(it->second.branchID(), rootTree.productStatuses()[index], ParentageID());
	  mapper->insert(entry);
	  mapper->insertIntoMap(it->second.oldProductID(), it->second.branchID());
        }
      }
      return mapper;
    }

    boost::shared_ptr<BranchMapper>
    makeBranchMapperInRelease210(RootTree& rootTree, BranchType const& type) {
      boost::shared_ptr<BranchMapperWithReader> mapper(new BranchMapperWithReader());
      mapper->setDelayedRead(false);
      if(type == InEvent) {
	std::auto_ptr<std::vector<EventEntryInfo> > infoVector(new std::vector<EventEntryInfo>);
	std::vector<EventEntryInfo> *pInfoVector = infoVector.get();
        rootTree.branchEntryInfoBranch()->SetAddress(&pInfoVector);
        setRefCoreStreamer(0, true, false);
        input::getEntry(rootTree.branchEntryInfoBranch(), rootTree.entryNumber());
        setRefCoreStreamer(true);
	for(std::vector<EventEntryInfo>::const_iterator it = infoVector->begin(), itEnd = infoVector->end();
	    it != itEnd; ++it) {
	  EventEntryDescription eed;
	  EntryDescriptionRegistry::instance()->getMapped(it->entryDescriptionID(), eed);
	  Parentage parentage(eed.parents());
	  ProductProvenance entry(it->branchID(), it->productStatus(), parentage.id());
	  mapper->insert(entry);
	  mapper->insertIntoMap(it->productID(), it->branchID());
	}
      } else {
	std::auto_ptr<std::vector<RunLumiEntryInfo> > infoVector(new std::vector<RunLumiEntryInfo>);
	std::vector<RunLumiEntryInfo> *pInfoVector = infoVector.get();
        rootTree.branchEntryInfoBranch()->SetAddress(&pInfoVector);
        setRefCoreStreamer(0, true, false);
        input::getEntry(rootTree.branchEntryInfoBranch(), rootTree.entryNumber());
        setRefCoreStreamer(true);
        for(std::vector<RunLumiEntryInfo>::const_iterator it = infoVector->begin(), itEnd = infoVector->end();
            it != itEnd; ++it) {
	  ProductProvenance entry(it->branchID(), it->productStatus(), ParentageID());
          mapper->insert(entry);
        }
      }
      return mapper;
    }

    boost::shared_ptr<BranchMapper>
    makeBranchMapperInRelease300(RootTree& rootTree) {
      boost::shared_ptr<BranchMapperWithReader> mapper(
	new BranchMapperWithReader(rootTree.branchEntryInfoBranch(), rootTree.entryNumber()));
      mapper->setDelayedRead(true);
      return mapper;
    }
  }

  boost::shared_ptr<BranchMapper>
  RootFile::makeBranchMapper(RootTree& rootTree, BranchType const& type) const {
    if(fileFormatVersion().splitProductIDs()) {
      return makeBranchMapperInRelease300(rootTree);
    } else if(fileFormatVersion().perEventProductIDs()) {
      return makeBranchMapperInRelease210(rootTree, type);
    } else if(fileFormatVersion().eventHistoryTree()) {
      return makeBranchMapperInRelease200(rootTree, type, *productRegistry_);
    } else {
      return makeBranchMapperInRelease180(rootTree, type, *productRegistry_);
    }
  }
  // end backward compatibility
}
