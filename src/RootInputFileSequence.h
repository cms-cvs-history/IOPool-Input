#ifndef IOPool_Input_RootInputFileSequence_h
#define IOPool_Input_RootInputFileSequence_h

/*----------------------------------------------------------------------

RootInputFileSequence: This is an InputSource

$Id: RootInputFileSequence.h,v 1.6 2008/03/20 09:39:06 lsexton Exp $

----------------------------------------------------------------------*/

#include <memory>
#include <vector>
#include <string>

#include "Inputfwd.h"

#include "FWCore/Framework/interface/Frameworkfwd.h"
#include "FWCore/Sources/interface/VectorInputSource.h"
#include "DataFormats/Provenance/interface/BranchDescription.h"
#include "DataFormats/Provenance/interface/EventID.h"
#include "DataFormats/Provenance/interface/LuminosityBlockID.h"
#include "DataFormats/Provenance/interface/RunID.h"

#include "boost/shared_ptr.hpp"
#include "boost/utility.hpp"

namespace CLHEP {
  class RandFlat;
}

namespace edm {

  class PoolSource;
  class RootFile;
  class FileCatalogItem;
  class InputFileCatalog;
  class FileIndex;
  class RootInputFileSequence : private boost::noncopyable {
  public:
    explicit RootInputFileSequence(ParameterSet const& pset, PoolSource const& input, InputFileCatalog const& catalog);
    virtual ~RootInputFileSequence();

    typedef VectorInputSource::EventPrincipalVector EventPrincipalVector;
    typedef boost::shared_ptr<RootFile> RootFileSharedPtr;
    typedef input::EntryNumber EntryNumber;
    std::auto_ptr<EventPrincipal> readEvent_(boost::shared_ptr<LuminosityBlockPrincipal> lbp);
    boost::shared_ptr<LuminosityBlockPrincipal> readLuminosityBlock_();
    boost::shared_ptr<RunPrincipal> readRun_();
    boost::shared_ptr<FileBlock> readFile_();
    void closeFile_();
    void endJob();
    InputSource::ItemType getNextItemType();
    std::auto_ptr<EventPrincipal> readIt(EventID const& id, LuminosityBlockNumber_t lumi = 0U, bool exact = false);
    void skip(int offset);
    void rewind_();
    void readMany_(int number, EventPrincipalVector& result);
    void readMany_(int number, EventPrincipalVector& result, EventID const& id, unsigned int fileSeqNumber);
    void readManyRandom_(int number, EventPrincipalVector& result, unsigned int& fileSeqNumber);
    ProductRegistry const& fileProductRegistry() const;
  private:
    void initFile(bool skipBadFiles);
    void updateProductRegistry() const;
    bool nextFile(bool wrapAround);
    bool previousFile();
    void rewindFile();
    std::auto_ptr<EventPrincipal> readCurrentEvent();
    std::vector<FileCatalogItem> const& fileCatalogItems() const;

    boost::shared_ptr<ProductRegistry const> productRegistry() const;
    boost::shared_ptr<RunPrincipal> runPrincipal() const;
    ProcessConfiguration const& processConfiguration() const;
    ProductRegistry & productRegistryUpdate() const;
    int remainingEvents() const;
    bool const primary() const;

    PoolSource const& input_;
    InputFileCatalog const& catalog_;
    bool firstFile_;
    std::vector<FileCatalogItem>::const_iterator fileIterBegin_;
    std::vector<FileCatalogItem>::const_iterator fileIterEnd_;
    std::vector<FileCatalogItem>::const_iterator fileIter_;
    RootFileSharedPtr rootFile_;
    BranchDescription::MatchMode matchMode_;
    CLHEP::RandFlat * flatDistribution_;
    std::vector<boost::shared_ptr<FileIndex> > fileIndexes_;

    int eventsRemainingInFile_;
    RunNumber_t startAtRun_;
    LuminosityBlockNumber_t startAtLumi_;
    EventNumber_t startAtEvent_;
    unsigned int eventsToSkip_;
    std::vector<LuminosityBlockID> whichLumisToSkip_;
    std::vector<EventID> eventsToProcess_;
    bool skipBadFiles_;
    int forcedRunOffset_;
    RunNumber_t setRun_;
    bool dropMetaData_;
  }; // class RootInputFileSequence
}
#endif