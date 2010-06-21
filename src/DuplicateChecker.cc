
#include "IOPool/Input/src/DuplicateChecker.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/Utilities/interface/Algorithms.h"
#include "FWCore/Utilities/interface/Exception.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include "FWCore/ParameterSet/interface/ParameterSetDescription.h"

#include <cassert>
#include <algorithm>

namespace edm {

/*
namespace {
  struct IsDupAnEvent {
    bool operator()(IndexIntoFile::Element const& e) {
      return e.getEntryType() == IndexIntoFile::kEvent;
    }
    bool operator()(IndexIntoFile::Element const& first, IndexIntoFile::Element const& next) {
      return first == next && first.getEntryType() == IndexIntoFile::kEvent;
    }
  };
}
*/

  DuplicateChecker::DuplicateChecker(ParameterSet const& pset) :
    dataType_(unknown),
    itIsKnownTheFileHasNoDuplicates_(false)
  {
    // The default value provided as the second argument to the getUntrackedParameter function call
    // is not used when the ParameterSet has been validated and the parameters are not optional
    // in the description.  This is currently true when PoolSource is the primary input source.
    // The modules that use PoolSource as a SecSource have not defined their fillDescriptions function
    // yet, so the ParameterSet does not get validated yet.  As soon as all the modules with a SecSource
    // have defined descriptions, the defaults in the getUntrackedParameterSet function calls can
    // and should be deleted from the code.
    std::string duplicateCheckMode =
      pset.getUntrackedParameter<std::string>("duplicateCheckMode", std::string("checkAllFilesOpened"));

    if (duplicateCheckMode == std::string("noDuplicateCheck")) duplicateCheckMode_ = noDuplicateCheck;
    else if (duplicateCheckMode == std::string("checkEachFile")) duplicateCheckMode_ = checkEachFile;
    else if (duplicateCheckMode == std::string("checkEachRealDataFile")) duplicateCheckMode_ = checkEachRealDataFile;
    else if (duplicateCheckMode == std::string("checkAllFilesOpened")) duplicateCheckMode_ = checkAllFilesOpened;
    else {
      throw cms::Exception("Configuration")
        << "Illegal configuration parameter value passed to PoolSource for\n"
        << "the \"duplicateCheckMode\" parameter, legal values are:\n"
        << "\"noDuplicateCheck\", \"checkEachFile\", \"checkEachRealDataFile\", \"checkAllFilesOpened\"\n";
    }
  }

  void DuplicateChecker::inputFileOpened(
      bool realData,
      IndexIntoFile const& indexIntoFile,
      std::vector<boost::shared_ptr<IndexIntoFile> > const& indexesIntoFiles,
      std::vector<boost::shared_ptr<IndexIntoFile> >::size_type currentIndexIntoFile) {
    /*
    if (duplicateCheckMode_ == noDuplicateCheck) return;

    dataType_ = realData ? isRealData : isSimulation;
    if (duplicateCheckMode_ == checkEachRealDataFile) {
      if(dataType_ == isSimulation) return;
    }

    relevantPreviousEvents_.clear();
    itIsKnownTheFileHasNoDuplicates_ = false;

    if (duplicateCheckMode_ == checkAllFilesOpened) {

      std::insert_iterator<std::set<IndexIntoFile::Element> > insertIter(relevantPreviousEvents_, relevantPreviousEvents_.begin());

      // Compares the current IndexIntoFile to all the previous ones and saves any duplicates.
      // One unintended thing, it also saves the duplicate runs and lumis.
      for(std::vector<boost::shared_ptr<IndexIntoFile> >::size_type i = 0; i < currentIndexIntoFile; ++i) {
        if (indexesIntoFiles[i].get() != 0) {
          std::set_intersection(indexIntoFile.begin(), indexIntoFile.end(),
                                indexesIntoFiles[i]->begin(), indexesIntoFiles[i]->end(),
                                insertIter);
        }
      }
    }
    // Check if none of the duplicates are events.
    if (!search_if_in_all(relevantPreviousEvents_, IsDupAnEvent())) {
      // Check for event duplicates within the new file
      IndexIntoFile::const_iterator duplicate = std::adjacent_find(indexIntoFile.begin(), indexIntoFile.end(), IsDupAnEvent());
      if (duplicate == indexIntoFile.end()) {
        itIsKnownTheFileHasNoDuplicates_ = true;
      }
    }
    */
  }

  void DuplicateChecker::inputFileClosed()
  {
    /*
    dataType_ = unknown;
    relevantPreviousEvents_.clear();
    itIsKnownTheFileHasNoDuplicates_ = false;
    */
  }

  bool DuplicateChecker::fastCloningOK() const
  {
    return true;
    /*
    return 
      itIsKnownTheFileHasNoDuplicates_ ||
      duplicateCheckMode_ == noDuplicateCheck ||
      (duplicateCheckMode_ == checkEachRealDataFile && dataType_ == isSimulation);
    */
  }

  bool DuplicateChecker::isDuplicateAndCheckActive(int index,
                                                   EventID const& eventID,
                                                   std::string const& fileName)
  {
    /*
    if (itIsKnownTheFileHasNoDuplicates_) return false;
    if (duplicateCheckMode_ == noDuplicateCheck) return false;
    if (duplicateCheckMode_ == checkEachRealDataFile) {
      assert(dataType_ != unknown);
      if(dataType_ == isSimulation) return false;
    }

    IndexIntoFile::Element newEvent(index, eventID.run(), eventID.luminosityBlock(), eventID.event());
    bool duplicate = !relevantPreviousEvents_.insert(newEvent).second;

    if (duplicate) {
      if (duplicateCheckMode_ == checkAllFilesOpened) {
        LogWarning("DuplicateEvent")
          << "Duplicate Events found in entire set of input files.\n"
          << "Both events were from run " << eventID.run() 
          << " and luminosity block " << eventID.luminosityBlock() 
          << " with event number " << eventID.event() << ".\n"
          << "The duplicate was from file " << fileName << ".\n"
          << "The duplicate will be skipped.\n";
      }
      else {
        LogWarning("DuplicateEvent")
          << "Duplicate Events found in file " << fileName << ".\n"
          << "Both events were from run " << eventID.run() 
          << " and luminosity block " << eventID.luminosityBlock()
          << " with event number " << eventID.event() << ".\n"
          << "The duplicate will be skipped.\n";
      }
      return true;
    }
    */
    return false;
  }

  void
  DuplicateChecker::fillDescription(ParameterSetDescription & desc) {
    std::string defaultString("checkAllFilesOpened");
    desc.addUntracked<std::string>("duplicateCheckMode", defaultString);
  }
}
