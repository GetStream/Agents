//go:build integration

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"

	"github.com/GetStream/Vision-Agents/acceleration/internal/knowledge"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// KnowledgeFilesSuite covers files uploaded to a knowledge base: read in the background into
// passages that say their page, scans read from their pictures, and the files that cannot
// be read refused with why.
type KnowledgeFilesSuite struct {
	RouterSuite
	namespace string
}

func TestKnowledgeFilesSuite(t *testing.T) {
	runSuite(t, new(KnowledgeFilesSuite))
}

func (s *KnowledgeFilesSuite) SetupTest() {
	s.useApp(s.data.createApp())
	s.namespace = "files-" + uuid.NewString()
	s.T().Cleanup(func() {
		s.knowledge.onceBeforeWrite(nil)
		s.knowledge.onceAfterWrite(nil)
		s.knowledge.onceBeforeDelete(nil)
		s.knowledge.onceAfterDelete(nil)
		s.knowledge.onceAfterDrop(nil)
	})
}

func (s *KnowledgeFilesSuite) TestAPDFIsReadIntoPassagesThatSayTheirPage() {
	status, sent, _ := s.upload("manual.pdf", extractFixture(s.T(), "manual.pdf"), "")
	s.Require().Equal(http.StatusAccepted, status)
	s.Equal(KnowledgeDocumentStatePending, sent.State)
	s.Equal("application/pdf", sent.ContentType)

	read := s.settled(sent.Id)

	s.Equal(KnowledgeDocumentStateIndexed, read.State)
	s.Nil(read.Error)
	s.Equal(12, read.Pages)
	s.Positive(read.Passages)
	s.Contains(*read.Text, "<!-- page 7 -->")
	passage := s.passageContaining("Fryer oil is changed every 48 hours")
	s.True(strings.HasPrefix(passage, "manual.pdf > p. 7"), passage)
	stored, err := s.store.KnowledgeDocuments.Get(context.Background(), s.customerID(), sent.Id)
	s.Require().NoError(err)
	s.Empty(stored.ObjectKey, "the file is deleted once read")
}

func (s *KnowledgeFilesSuite) TestAScannedPageIsReadFromItsPictureAndBilledAsOCR() {
	since := time.Now().UTC()
	_, sent, _ := s.upload("scanned.pdf", extractFixture(s.T(), "scanned.pdf"), "")

	read := s.settled(sent.Id)

	s.Equal(KnowledgeDocumentStateIndexed, read.State)
	s.Contains(*read.Text, "This first page has a text layer.")
	s.Contains(*read.Text, "<!-- page 2 -->\n\n# Fryer cleaning\n\nDrain the oil.", "the scan is read by the OCR router")
	rows := s.meteredRows(routing.OCR, since, 1)
	s.Equal("vision", rows[0].Provider, "a model that can summarise its figures reads a scan first")
}

func (s *KnowledgeFilesSuite) TestAChartOnAPageIsSummarisedAfterThePagesText() {
	since := time.Now().UTC()
	_, sent, _ := s.upload("figures.pdf", extractFixture(s.T(), "figures.pdf"), "")

	read := s.settled(sent.Id)

	s.Equal(KnowledgeDocumentStateIndexed, read.State)
	s.Contains(*read.Text, "Week 4\n\n> **Figure:** Oil changes rise each week.\n\n<!-- page 2 -->")
	s.NotContains(*read.Text, "A table\n\nEvery station is cleaned at the end of each shift.\n\n> **Figure:**",
		"a table that is text is not summarised")
	rows := s.meteredRows(routing.OCR, since, 4)
	for _, row := range rows {
		s.Equal("vision", row.Provider)
	}
	s.True(strings.Contains(s.passageContaining("Oil changes rise each week"), "figures.pdf > p. "),
		"what a figure shows is found under its page")
}

func (s *KnowledgeFilesSuite) TestAPictureInAWordFileIsSummarisedWhereItIs() {
	since := time.Now().UTC()
	_, sent, _ := s.upload("figures.docx", extractFixture(s.T(), "figures.docx"), "")

	read := s.settled(sent.Id)

	s.Equal(KnowledgeDocumentStateIndexed, read.State)
	s.Contains(*read.Text, "The fryer is cleaned every night.\n\n> **Figure:** Oil changes rise each week.\n\nAn icon")
	s.Contains(*read.Text, "| 1 | > **Figure:** Oil changes rise each week. |")
	s.NotContains(*read.Text, "<!-- picture")
	s.meteredRows(routing.OCR, since, 2)
}

func (s *KnowledgeFilesSuite) TestAFileWithAPasswordFailsSayingSo() {
	_, sent, _ := s.upload("encrypted.pdf", extractFixture(s.T(), "encrypted.pdf"), "")

	read := s.settled(sent.Id)

	s.Equal(KnowledgeDocumentStateFailed, read.State)
	s.Require().NotNil(read.Error)
	s.Contains(*read.Error, "protected with a password")
	s.Zero(read.Passages)
}

func (s *KnowledgeFilesSuite) TestADamagedFileFailsSayingSo() {
	_, sent, _ := s.upload("corrupt.pdf", extractFixture(s.T(), "corrupt.pdf"), "")

	read := s.settled(sent.Id)

	s.Equal(KnowledgeDocumentStateFailed, read.State)
	s.Contains(*read.Error, "damaged")
}

func (s *KnowledgeFilesSuite) TestAPhotoIsRefusedWhileItsSenderWaits() {
	photo := append([]byte{0xFF, 0xD8, 0xFF, 0xE0, 0, 0x10}, []byte("JFIF\x00 a picture")...)

	status, _, answer := s.upload("menu.jpg", photo, "")

	s.Equal(http.StatusBadRequest, status)
	s.Contains(string(answer), "Upload a PDF")
	s.Empty(s.listed(), "nothing is kept of a file that cannot be read")
}

func (s *KnowledgeFilesSuite) TestAFileOverFiftyMegabytesIsRefused() {
	status, _, answer := s.upload("huge.txt", bytes.Repeat([]byte("a"), 50<<20+1), "")

	s.Equal(http.StatusRequestEntityTooLarge, status, string(answer))
	s.Empty(s.listed())
}

func (s *KnowledgeFilesSuite) TestUploadingTheSameSourceAgainReplacesTheDocument() {
	_, first, _ := s.upload("handbook.docx", extractFixture(s.T(), "handbook.docx"), "handbook")
	s.settled(first.Id)

	_, second, _ := s.upload("manual.pdf", extractFixture(s.T(), "manual.pdf"), "handbook")
	read := s.settled(second.Id)

	s.Equal(first.Id, second.Id, "a source is one document")
	s.Equal(12, read.Pages)
	s.Len(s.listed(), 1)
}

func (s *KnowledgeFilesSuite) TestADocumentRemovedBeforeItsFileIsReadLeavesNothing() {
	_, sent, _ := s.upload("removed.pdf", extractFixture(s.T(), "manual.pdf"), "")

	s.Require().Equal(http.StatusNoContent, s.remove(sent.Id))

	// Removed before the read or after it, the end is the same: no passage of it is found.
	s.everyFileRead()
	s.Zero(s.passagesOf("removed.pdf"))
	s.Empty(s.listed())
}

func (s *KnowledgeFilesSuite) TestAReadThatWritesAfterItsDocumentIsGoneTakesItBack() {
	writing, write := s.holdWrite()
	_, sent, _ := s.upload("late.pdf", extractFixture(s.T(), "manual.pdf"), "")
	s.reached(writing, "the read never wrote")

	s.Require().Equal(http.StatusNoContent, s.remove(sent.Id))
	write()

	s.everyFileRead()
	s.Zero(s.passagesOf("late.pdf"))
	s.Empty(s.listed())
}

func (s *KnowledgeFilesSuite) TestAReadThatWritesBetweenARemovalsPassagesAndItsRowLeavesNothing() {
	writing, write := s.holdWrite()
	_, sent, _ := s.upload("between.pdf", extractFixture(s.T(), "manual.pdf"), "")
	s.reached(writing, "the read never wrote")
	s.knowledge.onceAfterDelete(func() {
		write()
		s.everyFileRead()
	})

	s.Require().Equal(http.StatusNoContent, s.remove(sent.Id))

	s.Zero(s.passagesOf("between.pdf"))
	s.Empty(s.listed())
}

// The removal read the row as uploaded, before the read of its file counted any passage on it.
func (s *KnowledgeFilesSuite) TestAFileReadWhileItsDocumentIsRemovedLeavesNoPassage() {
	_, sent, _ := s.upload("removing.pdf", extractFixture(s.T(), "manual.pdf"), "")
	removing, err := s.store.KnowledgeDocuments.Get(context.Background(), s.customerID(), sent.Id)
	s.Require().NoError(err)
	removing.Passages = 0
	s.Require().Equal(KnowledgeDocumentStateIndexed, s.settled(sent.Id).State)

	asked := routing.WithAttribution(context.Background(), routing.Attribution{APIKey: s.app.key})
	s.Require().NoError(s.router.removeKnowledgeDocument(asked, removing))

	s.Zero(s.passagesOf("removing.pdf"))
	s.Empty(s.listed())
}

func (s *KnowledgeFilesSuite) TestAFileUploadedAgainWhileItsDocumentIsRemovedLeavesNoPassage() {
	_, sent, _ := s.upload("again.pdf", extractFixture(s.T(), "manual.pdf"), "")
	s.Require().Equal(KnowledgeDocumentStateIndexed, s.settled(sent.Id).State)
	s.knowledge.onceAfterDelete(func() {
		_, again, _ := s.upload("again.pdf", extractFixture(s.T(), "manual.pdf"), "")
		s.Equal(sent.Id, again.Id)
		s.settled(again.Id)
	})

	s.Require().Equal(http.StatusNoContent, s.remove(sent.Id))

	s.everyFileRead()
	s.Zero(s.passagesOf("again.pdf"))
	s.Empty(s.listed())
}

// The document made once the row is gone is a new one, and keeps its passages.
func (s *KnowledgeFilesSuite) TestADocumentMadeOnceItsSourceIsRemovedKeepsItsPassages() {
	_, sent, _ := s.upload("new.md", []byte(sections(3)), "")
	s.Require().Equal(KnowledgeDocumentStateIndexed, s.settled(sent.Id).State)
	posted := 0
	post := func() {
		posted++
		s.Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/knowledge", map[string]any{
			"namespace": s.namespace, "documents": []map[string]string{{"source": "new.md", "text": sections(8)}},
		}, nil))
	}
	// A removal that deletes passages again after the row finds the document posted then.
	var onDelete func() error
	onDelete = func() error {
		if len(s.listed()) == 0 {
			post()
			return nil
		}
		s.knowledge.onceBeforeDelete(onDelete)
		return nil
	}
	s.knowledge.onceBeforeDelete(func() error {
		_, _, _ = s.upload("new.md", []byte(sections(8)), "")
		s.settled(sent.Id)
		s.knowledge.onceBeforeDelete(onDelete)
		return nil
	})

	s.Require().Equal(http.StatusNoContent, s.remove(sent.Id))
	s.knowledge.onceBeforeDelete(nil)
	if posted == 0 {
		post()
	}

	s.Equal(1, posted)
	listed := s.listed()
	s.Require().Len(listed, 1)
	s.Equal(8, listed[0].Passages)
	s.Equal(8, s.passagesOf("new.md"))
}

// The first removal's passages fail to go once and its row stays, so the second finds it and
// takes away all the first left.
func (s *KnowledgeFilesSuite) TestARemovalThatFailsIsListedAndCanBeAskedAgain() {
	_, sent, _ := s.upload("fail.md", []byte(sections(3)), "")
	s.Require().Equal(KnowledgeDocumentStateIndexed, s.settled(sent.Id).State)
	s.knowledge.onceBeforeDelete(func() error {
		_, _, _ = s.upload("fail.md", []byte(sections(8)), "")
		s.settled(sent.Id)
		s.knowledge.onceBeforeDelete(func() error { return errors.New("down") })
		return nil
	})

	s.Require().Equal(http.StatusInternalServerError, s.remove(sent.Id))

	listed := s.listed()
	s.Require().Len(listed, 1)
	s.Equal(KnowledgeDocumentStateFailed, listed[0].State)
	s.Require().NotNil(listed[0].Error)
	s.Equal("Its removal did not finish. Delete it again.", *listed[0].Error)
	s.Zero(s.removals(sent.Id), "what was not removed is not recorded as removed")

	s.Require().Equal(http.StatusNoContent, s.remove(sent.Id))

	s.Zero(s.passagesOf("fail.md"))
	s.Empty(s.listed())
	s.Equal(1, s.removals(sent.Id))
}

func (s *KnowledgeFilesSuite) TestARemovalThatFailsKeepsAFileUploadedMeanwhile() {
	_, sent, _ := s.upload("kept.md", []byte(sections(3)), "")
	s.Require().Equal(KnowledgeDocumentStateIndexed, s.settled(sent.Id).State)
	writing, write := s.holdWrite()
	s.knowledge.onceBeforeDelete(func() error {
		status, _, _ := s.upload("kept.md", []byte(sections(8)), "")
		s.Equal(http.StatusAccepted, status)
		s.reached(writing, "the upload was never read")
		return errors.New("down")
	})

	s.Require().Equal(http.StatusInternalServerError, s.remove(sent.Id))
	write()

	read := s.settled(sent.Id)
	s.Equal(KnowledgeDocumentStateIndexed, read.State)
	s.Equal(8, read.Passages)
	s.Equal(8, s.passagesOf("kept.md"))
}

func (s *KnowledgeFilesSuite) TestARemovalWhoseCallerHungUpCanBeAskedAgain() {
	_, sent, _ := s.upload("hungup.md", []byte(sections(3)), "")
	s.Require().Equal(KnowledgeDocumentStateIndexed, s.settled(sent.Id).State)
	removing, err := s.store.KnowledgeDocuments.Get(context.Background(), s.customerID(), sent.Id)
	s.Require().NoError(err)
	ctx, hangUp := context.WithCancel(routing.WithAttribution(context.Background(), routing.Attribution{APIKey: s.app.key}))
	// Uploaded again and read before the caller hung up, so it is found again when it does.
	s.knowledge.onceBeforeDelete(func() error {
		_, _, _ = s.upload("hungup.md", []byte(sections(8)), "")
		s.settled(sent.Id)
		hangUp()
		return ctx.Err()
	})

	s.Require().ErrorIs(s.router.removeKnowledgeDocument(ctx, removing), context.Canceled)
	listed := s.listed()
	s.Require().Len(listed, 1)
	s.Equal(KnowledgeDocumentStateFailed, listed[0].State)

	s.Require().Equal(http.StatusNoContent, s.remove(sent.Id))
	s.Zero(s.passagesOf("hungup.md"))
	s.Empty(s.listed())
}

// Once its passages are gone a removal is finished whether or not its caller still waits.
func (s *KnowledgeFilesSuite) TestARemovalWhoseCallerHungUpAfterThePassagesWentIsFinished() {
	_, sent, _ := s.upload("finished.md", []byte(sections(3)), "")
	s.Require().Equal(KnowledgeDocumentStateIndexed, s.settled(sent.Id).State)
	removing, err := s.store.KnowledgeDocuments.Get(context.Background(), s.customerID(), sent.Id)
	s.Require().NoError(err)
	ctx, hangUp := context.WithCancel(context.WithValue(routing.WithAttribution(context.Background(), routing.Attribution{APIKey: s.app.key}), customerContextKey{}, s.customerID()))
	// The file is put back as if the read had not taken it, to see the removal forget it.
	removing.ObjectKey = s.customerID() + "/kept/finished.md"
	_, err = s.knowledgeBucket.Write(context.Background(), removing.ObjectKey, "text/markdown", strings.NewReader("kept"))
	s.Require().NoError(err)
	s.knowledge.onceAfterDelete(hangUp)

	s.Require().NoError(s.router.removeKnowledgeDocument(ctx, removing))

	s.Zero(s.passagesOf("finished.md"))
	s.Empty(s.listed())
	s.Equal(1, s.removals(sent.Id))
	_, err = s.knowledgeBucket.Read(context.Background(), removing.ObjectKey)
	s.Error(err, "the file is deleted with its document")
}

func (s *KnowledgeFilesSuite) TestTwoRemovalsOfADocumentAtOnceBothFinish() {
	_, sent, _ := s.upload("twice.md", []byte(sections(3)), "")
	s.Require().Equal(KnowledgeDocumentStateIndexed, s.settled(sent.Id).State)
	var second int
	s.knowledge.onceAfterDelete(func() { second = s.remove(sent.Id) })

	first := s.remove(sent.Id)

	s.Equal(http.StatusNoContent, second)
	s.Equal(http.StatusNoContent, first)
	s.Zero(s.passagesOf("twice.md"))
	s.Empty(s.listed())
	s.Equal(1, s.removals(sent.Id), "it was removed once")
}

// Removals that begin during one another must not each keep the other from ending.
func (s *KnowledgeFilesSuite) TestARemovalIsNotHeldUpByAnotherThatBegins() {
	_, sent, _ := s.upload("begun.md", []byte(sections(3)), "")
	s.Require().Equal(KnowledgeDocumentStateIndexed, s.settled(sent.Id).State)
	deletes := 0
	var begin func() error
	begin = func() error {
		deletes++
		_, _, err := s.store.KnowledgeDocuments.Take(context.Background(), s.customerID(), sent.Id)
		s.knowledge.onceBeforeDelete(begin)
		return err
	}
	s.knowledge.onceBeforeDelete(begin)

	s.Equal(http.StatusNoContent, s.remove(sent.Id))
	s.Equal(1, deletes)
	s.Empty(s.listed())
}

// holdTail uploads a shorter version of source and returns once its read wrote and is held in
// deleting what the version before left past its end.
func (s *KnowledgeFilesSuite) holdTail(source string) (finish func()) {
	deleting, released := make(chan struct{}), make(chan struct{})
	finish = sync.OnceFunc(func() { close(released) })
	s.T().Cleanup(finish)
	s.knowledge.onceBeforeDelete(func() error {
		close(deleting)
		<-released
		return nil
	})
	status, _, _ := s.upload(source, []byte(sections(3)), "")
	s.Equal(http.StatusAccepted, status)
	select {
	case <-deleting:
	case <-time.After(30 * time.Second):
		s.Fail("the read never deleted its tail")
	}
	return finish
}

// The last attempt deletes what a read wrote that is not finished, so the read must not say
// the document is found.
func (s *KnowledgeFilesSuite) TestARemovalThatEndsInAConflictIsFailedOnceTheReadItMetEnds() {
	_, sent, _ := s.upload("met.md", []byte(sections(8)), "")
	s.Require().Equal(KnowledgeDocumentStateIndexed, s.settled(sent.Id).State)
	deletes, finish := 0, func() {}
	var again func() error
	again = func() error {
		if deletes++; deletes == 5 {
			finish = s.holdTail("met.md")
			return nil
		}
		_, _, _ = s.upload("met.md", []byte(sections(8)), "")
		s.settled(sent.Id)
		s.knowledge.onceBeforeDelete(again)
		return nil
	}
	s.knowledge.onceBeforeDelete(again)

	s.Require().Equal(http.StatusConflict, s.remove(sent.Id))
	finish()
	s.everyFileRead()

	s.Equal(5, deletes)
	s.Zero(s.passagesOf("met.md"), "the last attempt deleted what the read wrote")
	listed := s.listed()
	s.Require().Len(listed, 1)
	s.Equal(KnowledgeDocumentStateFailed, listed[0].State)
	s.Require().Equal(http.StatusNoContent, s.remove(sent.Id))
	s.Empty(s.listed())
}

func (s *KnowledgeFilesSuite) TestARemovalThatFailsOnALaterAttemptIsFailedOnceTheReadItMetEnds() {
	_, sent, _ := s.upload("later.md", []byte(sections(8)), "")
	s.Require().Equal(KnowledgeDocumentStateIndexed, s.settled(sent.Id).State)
	finish := func() {}
	s.knowledge.onceBeforeDelete(func() error {
		_, _, _ = s.upload("later.md", []byte(sections(8)), "")
		s.settled(sent.Id)
		s.knowledge.onceBeforeDelete(func() error {
			finish = s.holdTail("later.md")
			return errors.New("down")
		})
		return nil
	})

	s.Require().Equal(http.StatusInternalServerError, s.remove(sent.Id))
	finish()
	s.everyFileRead()

	listed := s.listed()
	s.Require().Len(listed, 1)
	s.Equal(KnowledgeDocumentStateFailed, listed[0].State)
	s.Require().Equal(http.StatusNoContent, s.remove(sent.Id))
	s.Zero(s.passagesOf("later.md"))
	s.Empty(s.listed())
}

// A removal and a post of its document each keep to the whole document: what is listed is stored.
func (s *KnowledgeFilesSuite) TestAPostWrittenWhileItsDocumentIsRemovedIsNotListedWithoutPassages() {
	s.Require().Equal(http.StatusOK, s.post("p.md", 6))
	id := s.listed()[0].Id
	atStale, release, posted := make(chan struct{}), make(chan struct{}), make(chan int, 1)
	s.knowledge.onceAfterDelete(func() {
		// The post's delete of [3, 6) is held once its passages are written.
		s.knowledge.onceBeforeDelete(func() error { close(atStale); <-release; return nil })
		go func() { posted <- s.post("p.md", 3) }()
		s.reached(atStale, "the post never wrote")
	})

	s.Require().Equal(http.StatusNoContent, s.remove(id))
	close(release)
	s.Equal(http.StatusOK, s.answer(posted))

	s.indexedWith("p.md", 3)
}

// postHeld begins a post of n sections that holdWrite holds, and returns once it is there.
func (s *KnowledgeFilesSuite) postHeld(source string, n int) (write func(), posted chan int) {
	writing, write := s.holdWrite()
	posted = make(chan int, 1)
	go func() { posted <- s.post(source, n) }()
	s.reached(writing, "the post never wrote")
	return write, posted
}

func (s *KnowledgeFilesSuite) TestAPostThatWritesAfterItsDocumentIsGoneIsADocumentOfItsOwn() {
	s.Require().Equal(http.StatusOK, s.post("own.md", 6))
	id := s.listed()[0].Id
	write, posted := s.postHeld("own.md", 3)

	s.Require().Equal(http.StatusNoContent, s.remove(id))
	s.Empty(s.listed())
	write()

	s.Equal(http.StatusOK, s.answer(posted))
	s.indexedWith("own.md", 3)
}

// The post is done before the removal is, so it is removed with the rest.
func (s *KnowledgeFilesSuite) TestAPostDoneWhileItsDocumentIsRemovedIsRemoved() {
	s.Require().Equal(http.StatusOK, s.post("done.md", 2))
	id := s.listed()[0].Id
	write, posted := s.postHeld("done.md", 8)
	s.knowledge.onceAfterDelete(func() {
		write()
		s.Equal(http.StatusOK, s.answer(posted))
	})

	s.Require().Equal(http.StatusNoContent, s.remove(id))

	s.Empty(s.listed())
	s.Zero(s.passagesOf("done.md"))
}

// The post finished on the row the removal holds before the removal's passages went.
func (s *KnowledgeFilesSuite) TestAPostThatMeetsARemovalsDeleteIsRemoved() {
	s.Require().Equal(http.StatusOK, s.post("meets.md", 2))
	id := s.listed()[0].Id
	s.knowledge.onceBeforeDelete(func() error {
		s.Equal(http.StatusOK, s.post("meets.md", 8))
		return nil
	})

	s.Require().Equal(http.StatusNoContent, s.remove(id))

	s.Empty(s.listed())
	s.Zero(s.passagesOf("meets.md"))
}

// The delete took what the post wrote and then failed, so the post writes it again.
func (s *KnowledgeFilesSuite) TestAPostThatMeetsARemovalThatFailsIsStored() {
	s.Require().Equal(http.StatusOK, s.post("half.md", 6))
	id := s.listed()[0].Id
	atStale, release, posted := make(chan struct{}), make(chan struct{}), make(chan int, 1)
	s.knowledge.onceBeforeDelete(func() error {
		s.knowledge.onceBeforeDelete(func() error { close(atStale); <-release; return nil })
		go func() { posted <- s.post("half.md", 3) }()
		s.reached(atStale, "the post never wrote")
		s.forgetStored("half.md")
		return errors.New("down")
	})

	s.Require().Equal(http.StatusInternalServerError, s.remove(id))
	s.Equal(KnowledgeDocumentStateFailed, s.listed()[0].State)
	close(release)

	s.Equal(http.StatusOK, s.answer(posted))
	s.indexedWith("half.md", 3)
}

// The write that stops part way has written some of its passages after the removal.
func (s *KnowledgeFilesSuite) TestAPostThatFailsOnceItsDocumentIsGoneIsListedAsFailed() {
	s.Require().Equal(http.StatusOK, s.post("part.md", 6))
	id := s.listed()[0].Id
	write, posted := s.postHeld("part.md", 3)
	s.knowledge.onceAfterWrite(func() error { return errors.New("down") })

	s.Require().Equal(http.StatusNoContent, s.remove(id))
	write()

	s.Equal(http.StatusInternalServerError, s.answer(posted))
	s.Equal(3, s.passagesOf("part.md"))
	listed := s.listed()
	s.Require().Len(listed, 1)
	s.Equal(KnowledgeDocumentStateFailed, listed[0].State)
	s.Equal(3, listed[0].Passages)

	s.Require().Equal(http.StatusNoContent, s.remove(listed[0].Id))
	s.Zero(s.passagesOf("part.md"))
	s.Empty(s.listed())
}

// postBoth sends first.md and second.md, of 2 and 3 sections, in one post.
func (s *KnowledgeFilesSuite) postBoth() int {
	return s.serverClient.do(http.MethodPost, "/v1/agents/knowledge", map[string]any{
		"namespace": s.namespace, "documents": []map[string]string{
			{"source": "first.md", "text": sections(2)}, {"source": "second.md", "text": sections(3)},
		},
	}, nil)
}

func (s *KnowledgeFilesSuite) removeSource(source string) {
	for _, document := range s.listed() {
		if document.Source == source {
			s.Require().Equal(http.StatusNoContent, s.remove(document.Id))
		}
	}
}

// bothFailed checks first.md and second.md are listed, each failed.
func (s *KnowledgeFilesSuite) bothFailed() {
	listed := s.listed()
	s.Len(listed, 2)
	for _, document := range listed {
		s.Equal(KnowledgeDocumentStateFailed, document.State, document.Source)
	}
}

// One was removed meanwhile and waited to be written again, the other could not be finished.
func (s *KnowledgeFilesSuite) TestEveryDocumentOfAPostThatCannotFinishIsListedAsFailed() {
	s.trigger("refuse_finish", "knowledge_documents", "UPDATE", "NEW.customer_id = '"+s.customerID()+
		"' AND NEW.source = 'second.md' AND NEW.state = 'indexed'", "RAISE EXCEPTION 'refused by the test';")
	writing, write := s.holdWrite()
	posted := make(chan int, 1)
	go func() { posted <- s.postBoth() }()
	s.reached(writing, "the post never wrote")
	s.removeSource("first.md")
	write()

	s.Equal(http.StatusInternalServerError, s.answer(posted))
	s.bothFailed()
}

func (s *KnowledgeFilesSuite) TestEveryDocumentOfAPostThatCannotDeleteItsTailIsListedAsFailed() {
	s.Require().Equal(http.StatusOK, s.post("second.md", 6))
	writing, write := s.holdWrite()
	posted := make(chan int, 1)
	go func() { posted <- s.postBoth() }()
	s.reached(writing, "the post never wrote")
	s.removeSource("first.md")
	s.knowledge.onceBeforeDelete(func() error { return errors.New("down") })
	write()

	s.Equal(http.StatusInternalServerError, s.answer(posted))
	s.bothFailed()
}

func (s *KnowledgeFilesSuite) TestEveryDocumentOfAPostThatCannotHoldOneIsListedAsFailed() {
	s.Require().Equal(http.StatusOK, s.postBoth())
	s.trigger("refuse_hold", "knowledge_documents", "UPDATE", "NEW.customer_id = '"+s.customerID()+
		"' AND NEW.source = 'second.md' AND NEW.state = 'pending'", "RAISE EXCEPTION 'refused by the test';")

	s.Equal(http.StatusInternalServerError, s.serverClient.do(http.MethodPost, "/v1/agents/knowledge", map[string]any{
		"namespace": s.namespace, "documents": []map[string]string{
			{"source": "first.md", "text": sections(4)}, {"source": "second.md", "text": sections(4)},
		},
	}, nil))
	s.bothFailed()
}

func (s *KnowledgeFilesSuite) TestOnlyTheDocumentOfAPostThatWasRemovedIsWrittenAgain() {
	writing, write := s.holdWrite()
	posted := make(chan int, 1)
	go func() { posted <- s.postBoth() }()
	s.reached(writing, "the post never wrote")
	s.removeSource("second.md")
	write()

	s.Equal(http.StatusOK, s.answer(posted))
	s.Len(s.listed(), 2)
	s.Equal(2, s.passagesOf("first.md"))
	s.Equal(3, s.passagesOf("second.md"))
	s.Len(s.knowledge.lastWrite(), 3, "the document that was not removed is not written again")
	for _, document := range s.listed() {
		s.Equal(KnowledgeDocumentStateIndexed, document.State)
	}
	s.Equal(2, s.recorded(), "each document is recorded once")
}

// Posts of one source at once do not make each other write again: the last to finish is kept.
func (s *KnowledgeFilesSuite) TestTwoPostsOfASourceAtOnceAreEachWrittenOnce() {
	write, posted := s.postHeld("both.md", 6)
	s.Require().Equal(http.StatusOK, s.post("both.md", 3))
	writes := 0
	s.knowledge.onceAfterWrite(func() error {
		s.knowledge.onceBeforeWrite(func() { writes++ })
		return nil
	})
	write()

	s.Equal(http.StatusOK, s.answer(posted))
	s.Zero(writes)
	s.indexedWith("both.md", 6)
}

func (s *KnowledgeFilesSuite) TestTwoPostsOfASourceWrittenAtOnceAreEachWrittenOnce() {
	finish, posted := s.postWritten("once.md", 3)
	finishLast, last := s.postWritten("once.md", 3)
	writes := 0
	s.knowledge.onceBeforeWrite(func() { writes++ })
	finish()
	s.Equal(http.StatusOK, s.answer(posted))
	finishLast()

	s.Equal(http.StatusOK, s.answer(last))
	s.Zero(writes)
	s.indexedWith("once.md", 3)
}

func (s *KnowledgeFilesSuite) TestAPostWrittenOverAnUploadThatBeganMeanwhileIsKept() {
	write, posted := s.postHeld("over.md", 3)
	status, sent, _ := s.upload("over.md", extractFixture(s.T(), "encrypted.pdf"), "")
	s.Require().Equal(http.StatusAccepted, status)
	s.settled(sent.Id)
	write()

	s.Equal(http.StatusOK, s.answer(posted))
	s.everyFileRead()
	s.indexedWith("over.md", 3)
}

// The removal gives up first: it can take the post's document no more often than the post
// writes it.
func (s *KnowledgeFilesSuite) TestAPostOutlastsARemovalThatKeepsMeetingIt() {
	s.Require().Equal(http.StatusOK, s.post("outlasts.md", 2))
	id := s.listed()[0].Id
	writes, held := 0, make(chan func(), 1)
	var hold func()
	hold = func() {
		writes++
		released := make(chan struct{})
		s.knowledge.onceBeforeWrite(hold)
		held <- sync.OnceFunc(func() { close(released) })
		<-released
	}
	s.knowledge.onceBeforeWrite(hold)
	posted := make(chan int, 1)
	go func() { posted <- s.post("outlasts.md", 8) }()
	next := func() func() {
		select {
		case write := <-held:
			return write
		case <-time.After(10 * time.Second):
			s.Fail("the post did not write again")
			return func() {}
		}
	}
	write := next()
	var meet func() error
	meet = func() error {
		s.knowledge.onceBeforeDelete(nil)
		write()
		write = next()
		s.knowledge.onceBeforeDelete(meet)
		return nil
	}
	s.knowledge.onceBeforeDelete(meet)

	s.Equal(http.StatusConflict, s.remove(id))
	s.knowledge.onceBeforeDelete(nil)
	write()
	next()()

	s.Equal(http.StatusOK, s.answer(posted))
	s.Equal(7, writes)
	s.indexedWith("outlasts.md", 8)
}

// Removals one after another keep a post from finishing: it ends, says so, and what it wrote
// is listed to be removed.
func (s *KnowledgeFilesSuite) TestAPostOfADocumentRemovedAgainAndAgainIsAConflict() {
	writes := 0
	var again func()
	again = func() {
		writes++
		listed := s.listed()
		if s.Len(listed, 1) {
			s.Equal(http.StatusNoContent, s.remove(listed[0].Id))
		}
		s.knowledge.onceBeforeWrite(again)
	}
	s.knowledge.onceBeforeWrite(again)

	s.Equal(http.StatusConflict, s.post("never.md", 3))
	s.knowledge.onceBeforeWrite(nil)

	s.Equal(7, writes)
	listed := s.listed()
	s.Require().Len(listed, 1)
	s.Equal(KnowledgeDocumentStateFailed, listed[0].State)
	s.Require().Equal(http.StatusNoContent, s.remove(listed[0].Id))
	s.Zero(s.passagesOf("never.md"))
	s.Empty(s.listed())
}

// The base's rows go in a commit held until the post has written to the dropped base.
func (s *KnowledgeFilesSuite) TestAPostWrittenWhileItsBaseIsDeletedIsADocumentOfItsOwn() {
	s.Require().Equal(http.StatusOK, s.post("base.md", 6))
	ctx := context.Background()
	lock, err := s.store.GlobalDB().Conn(ctx)
	s.Require().NoError(err)
	defer func() {
		_, err := lock.ExecContext(ctx, "SELECT pg_advisory_unlock_all()")
		s.NoError(err)
		s.NoError(lock.Close())
	}()
	_, err = lock.ExecContext(ctx, "SELECT pg_advisory_lock(2380)")
	s.Require().NoError(err)
	s.trigger("hold_base_delete", "knowledge_documents", "DELETE", "OLD.customer_id = '"+s.customerID()+"'",
		"PERFORM pg_advisory_xact_lock(2380);")
	write, posted := s.postHeld("base.md", 3)
	deleted := make(chan int, 1)
	go func() {
		deleted <- s.serverClient.do(http.MethodDelete, "/v1/agents/knowledge?namespace="+s.namespace, nil, nil)
	}()
	// Other tests' backends wait in this database too, so only these two are looked for.
	waiter := func(query string, args ...any) (pid int) {
		s.Require().Eventually(func() bool {
			return lock.QueryRowContext(ctx, query, args...).Scan(&pid) == nil
		}, 30*time.Second, 5*time.Millisecond)
		return pid
	}
	deleting := waiter("SELECT l.pid FROM pg_locks l JOIN pg_database d ON d.oid = l.database " +
		"WHERE d.datname = current_database() AND l.locktype = 'advisory' AND l.objid = 2380 AND NOT l.granted")
	write()
	waiter("SELECT pid FROM pg_stat_activity WHERE ? = ANY(pg_blocking_pids(pid)) LIMIT 1", deleting)
	_, err = lock.ExecContext(ctx, "SELECT pg_advisory_unlock(2380)")
	s.Require().NoError(err)

	s.Equal(http.StatusNoContent, s.answer(deleted))
	s.Equal(http.StatusOK, s.answer(posted))
	s.indexedWith("base.md", 3)
}

func (s *KnowledgeFilesSuite) dropBase() int {
	return s.serverClient.do(http.MethodDelete, "/v1/agents/knowledge?namespace="+s.namespace, nil, nil)
}

// nothingLeft checks no document is listed and no passage of source is stored.
func (s *KnowledgeFilesSuite) nothingLeft(source string) {
	s.Empty(s.listed())
	s.Zero(s.passagesOf(source))
}

func (s *KnowledgeFilesSuite) TestAPostDoneWhileItsBaseIsDeletedIsDeletedWithIt() {
	s.Require().Equal(http.StatusOK, s.post("done.md", 2))
	s.knowledge.onceAfterDrop(func() { s.Equal(http.StatusOK, s.post("done.md", 8)) })

	s.Require().Equal(http.StatusNoContent, s.dropBase())

	s.nothingLeft("done.md")
	s.Equal(1, s.removals(s.namespace))
	var removal store.AuditEntry
	s.Require().NoError(s.store.GlobalDB().NewSelect().Model(&removal).
		Where("customer_id = ? AND resource_id = ?", s.customerID(), s.namespace).Scan(context.Background()))
	s.Contains(removal.Changes, store.AuditChange{Field: "documents", Before: float64(2)})
}

func (s *KnowledgeFilesSuite) TestAFileReadWhileItsBaseIsDeletedIsDeletedWithIt() {
	writing, write := s.holdWrite()
	_, _, _ = s.upload("read.md", []byte(sections(8)), "")
	s.reached(writing, "the read never wrote")
	s.knowledge.onceAfterDrop(func() {
		write()
		s.everyFileRead()
	})

	s.Require().Equal(http.StatusNoContent, s.dropBase())

	s.nothingLeft("read.md")
}

// A base written over and over keeps its delete from finishing: it ends, and says so.
func (s *KnowledgeFilesSuite) TestDeletingABaseWrittenAgainAndAgainIsAConflict() {
	s.Require().Equal(http.StatusOK, s.post("busy.md", 2))
	drops := 0
	var again func()
	again = func() {
		drops++
		s.Equal(http.StatusOK, s.post("busy.md", 2))
		s.knowledge.onceAfterDrop(again)
	}
	s.knowledge.onceAfterDrop(again)

	s.Require().Equal(http.StatusConflict, s.dropBase())
	s.knowledge.onceAfterDrop(nil)

	s.Equal(5, drops)
	listed := s.listed()
	s.Require().Len(listed, 1)
	s.Equal(KnowledgeDocumentStateFailed, listed[0].State, "its passages may be gone, so it is not shown as found")
	s.Require().Equal(http.StatusNoContent, s.dropBase())
	s.nothingLeft("busy.md")
}

// holdAfterWrite holds the next write to the base once it is kept, until done is called, and
// closes wrote once it is there.
func (s *KnowledgeFilesSuite) holdAfterWrite() (wrote chan struct{}, done func()) {
	wrote, released := make(chan struct{}), make(chan struct{})
	done = sync.OnceFunc(func() { close(released) })
	s.T().Cleanup(done)
	s.knowledge.onceAfterWrite(func() error {
		close(wrote)
		<-released
		return nil
	})
	return wrote, done
}

// postWritten begins a post of n sections that is held once its passages are written.
func (s *KnowledgeFilesSuite) postWritten(source string, n int) (finish func(), posted chan int) {
	wrote, finish := s.holdAfterWrite()
	posted = make(chan int, 1)
	go func() { posted <- s.post(source, n) }()
	s.reached(wrote, "the post never wrote")
	return finish, posted
}

// The upload was read while the post's write was held, so the post takes its tail away.
func (s *KnowledgeFilesSuite) TestAPostWrittenOverALongerUploadLeavesNoneOfItsPassages() {
	write, posted := s.postHeld("longer.md", 2)
	_, sent, _ := s.upload("longer.md", []byte(sections(8)), "")
	s.Require().Equal(KnowledgeDocumentStateIndexed, s.settled(sent.Id).State)
	write()

	s.Equal(http.StatusOK, s.answer(posted))
	s.indexedWith("longer.md", 2)
}

// The shorter post takes the upload's tail away, and with it some of what the longer one wrote.
func (s *KnowledgeFilesSuite) TestAPostWhosePassagesAnotherTookForAnUploadsIsWrittenAgain() {
	write, posted := s.postHeld("shared.md", 2)
	_, sent, _ := s.upload("shared.md", []byte(sections(8)), "")
	s.Require().Equal(KnowledgeDocumentStateIndexed, s.settled(sent.Id).State)
	atTail, release := make(chan struct{}), make(chan struct{})
	s.knowledge.onceAfterWrite(func() error {
		s.knowledge.onceBeforeDelete(func() error { close(atTail); <-release; return nil })
		return nil
	})
	write()
	s.reached(atTail, "the post never took the tail")
	finish, longer := s.postWritten("shared.md", 6)
	close(release)
	s.Equal(http.StatusOK, s.answer(posted))
	finish()

	s.Equal(http.StatusOK, s.answer(longer))
	s.indexedWith("shared.md", 6)
}

func (s *KnowledgeFilesSuite) TestAPostAnOlderReadWroteOverIsWrittenAgain() {
	for _, removed := range []bool{false, true} {
		writing, write := s.holdWrite()
		_, sent, _ := s.upload("older.md", []byte("# Section 0\n\nThe file's.\n\n"+sections(8)), "")
		s.reached(writing, "the read never wrote")
		if removed {
			s.Require().Equal(http.StatusNoContent, s.remove(sent.Id))
		}
		finish, posted := s.postWritten("older.md", 2)
		write()
		s.everyFileRead()
		s.Positive(s.passagesOf("older.md"))
		finish()

		s.Equal(http.StatusOK, s.answer(posted))
		s.indexedWith("older.md", 2)
		s.passageContaining("What section 0 says.")
		s.Require().Equal(http.StatusNoContent, s.remove(s.listed()[0].Id))
	}
}

// The document of a read was removed and its source posted before the read wrote.
func (s *KnowledgeFilesSuite) TestAReadLeavesADocumentMadeOfItsSourceSinceItsPassages() {
	writing, write := s.holdWrite()
	_, sent, _ := s.upload("since.md", []byte(sections(3)), "")
	s.reached(writing, "the read never wrote")
	s.Require().Equal(http.StatusNoContent, s.remove(sent.Id))
	s.Require().Equal(http.StatusOK, s.post("since.md", 2))
	write()

	s.everyFileRead()
	s.indexedWith("since.md", 3)
	s.Require().Equal(http.StatusNoContent, s.remove(s.listed()[0].Id))
	s.nothingLeft("since.md")
}

func (s *KnowledgeFilesSuite) TestAPostThatMeetsWhatAReadTakesBackIsStored() {
	writing, write := s.holdWrite()
	_, sent, _ := s.upload("back.md", []byte(sections(8)), "")
	s.reached(writing, "the read never wrote")
	s.Require().Equal(http.StatusNoContent, s.remove(sent.Id))
	var finish func()
	var posted chan int
	s.knowledge.onceBeforeDelete(func() error {
		finish, posted = s.postWritten("back.md", 2)
		return nil
	})
	write()
	s.everyFileRead()
	s.Zero(s.passagesOf("back.md"))
	finish()

	s.Equal(http.StatusOK, s.answer(posted))
	s.indexedWith("back.md", 2)
}

func (s *KnowledgeFilesSuite) TestAnUploadAnOlderReadWroteOverIsWrittenAgain() {
	writing, write := s.holdWrite()
	_, _, _ = s.upload("newer.md", []byte("# Section 0\n\nThe older file's.\n\n"), "")
	s.reached(writing, "the read never wrote")
	wrote, finish := s.holdAfterWrite()
	_, sent, _ := s.upload("newer.md", []byte(sections(1)), "")
	s.reached(wrote, "the newer file was never read")
	write()
	s.Eventually(func() bool {
		document, err := s.store.KnowledgeDocuments.Get(context.Background(), s.customerID(), sent.Id)
		return err == nil && document.Error != ""
	}, 30*time.Second, 10*time.Millisecond, "the older read did not say it wrote")
	finish()

	s.Nil(s.settled(sent.Id).Error)
	s.everyFileRead()
	s.indexedWith("newer.md", 1)
	s.passageContaining("What section 0 says.")
}

// The delete took what the read wrote and then failed, so the read writes it again.
func (s *KnowledgeFilesSuite) TestAnUploadThatMeetsARemovalThatFailsIsStored() {
	_, sent, _ := s.upload("half.md", []byte(sections(8)), "")
	s.Require().Equal(KnowledgeDocumentStateIndexed, s.settled(sent.Id).State)
	finish := func() {}
	s.knowledge.onceBeforeDelete(func() error {
		finish = s.holdTail("half.md")
		s.forgetStored("half.md")
		return errors.New("down")
	})

	s.Require().Equal(http.StatusInternalServerError, s.remove(sent.Id))
	finish()

	s.settled(sent.Id)
	s.indexedWith("half.md", 3)
}

// A read met each time it writes ends and is tried again.
func (s *KnowledgeFilesSuite) TestAReadMetAgainAndAgainIsTriedAgain() {
	_, sent, _ := s.upload("met.md", []byte(sections(3)), "")
	s.Require().Equal(KnowledgeDocumentStateIndexed, s.settled(sent.Id).State)
	writes := 0
	var meet func() error
	meet = func() error {
		if writes++; writes <= 4 {
			s.forgetStored("met.md")
			s.NoError(s.store.KnowledgeDocuments.FailRemoval(context.Background(), s.customerID(), sent.Id))
			s.knowledge.onceAfterWrite(meet)
		}
		return nil
	}
	s.knowledge.onceAfterWrite(meet)
	_, _, _ = s.upload("met.md", []byte(sections(2)), "")

	s.settled(sent.Id)
	s.Equal(5, writes)
	s.indexedWith("met.md", 2)
}

func (s *KnowledgeFilesSuite) TestARemovalThatDidNotBeginLeavesItsDocumentAsItWas() {
	s.Require().Equal(http.StatusOK, s.post("intact.md", 2))
	document, err := s.store.KnowledgeDocuments.Get(context.Background(), s.customerID(), s.listed()[0].Id)
	s.Require().NoError(err)
	ctx, hangUp := context.WithCancel(context.Background())
	hangUp()

	s.Require().ErrorIs(s.router.removeKnowledgeDocument(ctx, document), context.Canceled)

	s.indexedWith("intact.md", 2)
}

func (s *KnowledgeFilesSuite) TestAPostThatCannotFinishItsSecondDocumentKeepsTheFirstIndexed() {
	s.trigger("refuse_finish", "knowledge_documents", "UPDATE", "NEW.customer_id = '"+s.customerID()+
		"' AND NEW.source = 'second.md' AND NEW.state = 'indexed'", "RAISE EXCEPTION 'refused by the test';")

	s.Equal(http.StatusInternalServerError, s.postBoth())

	states := map[string]KnowledgeDocumentState{}
	for _, document := range s.listed() {
		states[document.Source] = document.State
	}
	s.Equal(map[string]KnowledgeDocumentState{"first.md": "indexed", "second.md": "failed"}, states)
	s.Equal(2, s.passagesOf("first.md"))
}

// refuseInterrupts fails a read's word to the removal that it wrote after it, and nothing else.
func (s *KnowledgeFilesSuite) refuseInterrupts() {
	s.trigger("refuse_interrupts", "knowledge_documents", "UPDATE", "OLD.customer_id = '"+s.customerID()+
		"' AND OLD.object_key LIKE 'removing:%' AND NEW.object_key LIKE 'removing:%' AND NEW.object_key <> OLD.object_key",
		"RAISE EXCEPTION 'refused by the test';")
}

func (s *KnowledgeFilesSuite) TestAReadThatCannotTellTheRemovalItWroteTakesItBack() {
	s.refuseInterrupts()
	writing, write := s.holdWrite()
	_, sent, _ := s.upload("untold.md", []byte(sections(8)), "")
	s.reached(writing, "the read never wrote")
	s.knowledge.onceAfterDelete(func() {
		write()
		s.everyFileRead()
	})

	s.Require().Equal(http.StatusNoContent, s.remove(sent.Id))

	s.Zero(s.passagesOf("untold.md"))
	s.Empty(s.listed())
}

// The removal deleted the read's count, the post's shorter version took the row, and then the
// read wrote.
func (s *KnowledgeFilesSuite) TestAReadOvertakenByAPostWhileItsDocumentIsRemovedLeavesNothing() {
	writing, write := s.holdWrite()
	_, sent, _ := s.upload("between.md", []byte(sections(8)), "")
	s.reached(writing, "the read never wrote")
	s.knowledge.onceAfterDelete(func() {
		s.Equal(http.StatusOK, s.post("between.md", 2))
		write()
		s.everyFileRead()
	})

	s.Require().Equal(http.StatusNoContent, s.remove(sent.Id))

	s.Zero(s.passagesOf("between.md"))
	s.Empty(s.listed())
}

// A read overtaken by a post of fewer passages leaves more, which go with the document.
func (s *KnowledgeFilesSuite) TestAReadOvertakenByAShorterPostIsRemovedWhole() {
	writing, write := s.holdWrite()
	_, sent, _ := s.upload("shorter.md", []byte(sections(8)), "")
	s.reached(writing, "the read never wrote")
	s.Require().Equal(http.StatusOK, s.post("shorter.md", 2))
	write()
	s.everyFileRead()
	s.indexedWith("shorter.md", 8)

	s.Require().Equal(http.StatusNoContent, s.remove(sent.Id))
	s.Zero(s.passagesOf("shorter.md"))
}

// Not told whether it is removed, the read leaves the post's passages alone.
func (s *KnowledgeFilesSuite) TestAReadThatCannotCountItsPassagesLeavesAPostsAlone() {
	writing, write := s.holdWrite()
	_, _, _ = s.upload("uncounted.md", []byte(sections(3)), "")
	s.reached(writing, "the read never wrote")
	s.Require().Equal(http.StatusOK, s.post("uncounted.md", 3))
	s.trigger("refuse_counts", "knowledge_documents", "UPDATE", "OLD.customer_id = '"+s.customerID()+
		"' AND OLD.updated_at = NEW.updated_at", "RAISE EXCEPTION 'refused by the test';")
	write()

	s.everyFileRead()
	s.indexedWith("uncounted.md", 3)
}

// A key that names no file is not asked of the bucket.
func (s *KnowledgeFilesSuite) TestRemovingPostedTextAsksNothingOfTheBucket() {
	s.Require().Equal(http.StatusOK, s.post("text.md", 2))
	id := s.listed()[0].Id
	document, err := s.store.KnowledgeDocuments.Get(context.Background(), s.customerID(), id)
	s.Require().NoError(err)
	_, err = s.knowledgeBucket.Write(context.Background(), document.ObjectKey, "text/plain", strings.NewReader("kept"))
	s.Require().NoError(err)

	s.Require().Equal(http.StatusNoContent, s.remove(id))

	kept, err := s.knowledgeBucket.Read(context.Background(), document.ObjectKey)
	s.Require().NoError(err)
	s.Equal("kept", string(kept))
}

// The upload was read while the first read's write was held, and is not the first's to take.
func (s *KnowledgeFilesSuite) TestAReadOvertakenByALaterUploadLeavesItsPassages() {
	writing, write := s.holdWrite()
	_, sent, _ := s.upload("later-upload.md", []byte(sections(3)), "")
	s.reached(writing, "the read never wrote")
	status, _, _ := s.upload("later-upload.md", []byte(sections(3)), "")
	s.Require().Equal(http.StatusAccepted, status)
	s.Equal(KnowledgeDocumentStateIndexed, s.settled(sent.Id).State)
	write()

	s.everyFileRead()
	s.indexedWith("later-upload.md", 3)
}

// A count an overtaken read raised makes the same text be written again, once.
func (s *KnowledgeFilesSuite) TestAPostAfterAnOvertakenReadIsWrittenAgainOnce() {
	writing, write := s.holdWrite()
	_, _, _ = s.upload("counted.md", []byte(sections(8)), "")
	s.reached(writing, "the read never wrote")
	s.Require().Equal(http.StatusOK, s.post("counted.md", 2))
	write()
	s.everyFileRead()
	writes := 0
	s.knowledge.onceBeforeWrite(func() { writes++ })

	s.Equal(http.StatusOK, s.post("counted.md", 2))
	s.indexedWith("counted.md", 2)
	s.Equal(1, writes)
	s.knowledge.onceBeforeWrite(func() { writes++ })
	s.Equal(http.StatusOK, s.post("counted.md", 2))
	s.Equal(1, writes, "unchanged: not written again")
}

// What it was overtaken by is a post, whose passages are not the read's to take.
func (s *KnowledgeFilesSuite) TestAReadOvertakenByAPostLeavesItsPassages() {
	writing, write := s.holdWrite()
	_, _, _ = s.upload("posted.md", []byte(sections(3)), "")
	s.reached(writing, "the read never wrote")
	s.Require().Equal(http.StatusOK, s.post("posted.md", 3))
	write()

	s.everyFileRead()
	s.indexedWith("posted.md", 3)
}

// A write that answers an error may have been kept.
func (s *KnowledgeFilesSuite) TestAReadWhoseWriteFailedAfterItsDocumentIsGoneTakesItBack() {
	writing, write := s.holdWrite()
	s.knowledge.onceAfterWrite(func() error { return errors.New("down") })
	_, sent, _ := s.upload("errs.md", []byte(sections(8)), "")
	s.reached(writing, "the read never wrote")

	s.Require().Equal(http.StatusNoContent, s.remove(sent.Id))
	write()

	s.everyFileRead()
	s.Zero(s.passagesOf("errs.md"))
	s.Empty(s.listed())
}

// The read is cancelled as it is when its time runs out.
func (s *KnowledgeFilesSuite) TestAReadCancelledInItsWriteTakesItBackOnceItsDocumentIsGone() {
	writing, write := s.holdWrite()
	_, sent, _ := s.upload("cancelled.md", []byte(sections(8)), "")
	s.reached(writing, "the read never wrote")
	document, err := s.store.KnowledgeDocuments.Get(context.Background(), s.customerID(), sent.Id)
	s.Require().NoError(err)
	inspector := asynq.NewInspector(s.knowledgeRedis)
	defer func() { s.NoError(inspector.Close()) }()
	s.Require().NoError(inspector.CancelProcessing(document.ObjectKey))
	s.reached(s.knowledge.writeCancelled(), "the read was never cancelled")

	s.Require().Equal(http.StatusNoContent, s.remove(sent.Id))
	write()

	s.everyFileRead()
	s.Zero(s.passagesOf("cancelled.md"))
	s.Empty(s.listed())
}

func (s *KnowledgeFilesSuite) TestAReadWhoseWriteFailsIsTriedAgain() {
	s.knowledge.onceAfterWrite(func() error { return errors.New("down") })
	_, sent, _ := s.upload("tried.md", []byte(sections(3)), "")

	s.Equal(KnowledgeDocumentStateIndexed, s.settled(sent.Id).State)
	s.Equal(3, s.passagesOf("tried.md"))
}

// A source written over and over keeps its removal from finishing: it ends, and says so.
func (s *KnowledgeFilesSuite) TestARemovalOfADocumentWrittenAgainAndAgainIsAConflict() {
	_, sent, _ := s.upload("busy.md", []byte(sections(3)), "")
	s.Require().Equal(KnowledgeDocumentStateIndexed, s.settled(sent.Id).State)
	deletes := 0
	var again func() error
	again = func() error {
		deletes++
		_, _, _ = s.upload("busy.md", []byte(sections(3)), "")
		s.settled(sent.Id)
		s.knowledge.onceBeforeDelete(again)
		return nil
	}
	s.knowledge.onceBeforeDelete(again)

	s.Require().Equal(http.StatusConflict, s.remove(sent.Id))
	s.knowledge.onceBeforeDelete(nil)

	s.Equal(5, deletes)
	listed := s.listed()
	s.Require().Len(listed, 1)
	s.Equal(KnowledgeDocumentStateFailed, listed[0].State, "its passages may be gone, so it is not shown as found")
	s.Require().Equal(http.StatusNoContent, s.remove(sent.Id))
	s.Zero(s.passagesOf("busy.md"))
	s.Empty(s.listed())
}

func (s *KnowledgeFilesSuite) TestAFileWhoseWriteFailedPartWayIsDeletedWithEveryPassageItWrote() {
	s.knowledge.refuseWritesAfter(1)
	s.T().Cleanup(func() { s.knowledge.failWrites(false) })
	_, sent, _ := s.upload("long.md", []byte(sections(knowledge.WriteBatch+10)), "")

	read := s.settled(sent.Id)
	s.knowledge.failWrites(false)
	s.Require().Equal(KnowledgeDocumentStateFailed, read.State)
	s.Require().NotZero(s.passagesOf("long.md"), "the first batch was written")

	s.Require().Equal(http.StatusNoContent, s.serverClient.do(http.MethodDelete, "/v1/agents/knowledge/documents/"+sent.Id, nil, nil))

	s.Zero(s.passagesOf("long.md"))
}

func (s *KnowledgeFilesSuite) TestAFileThatGrewAndFailedPartWayLeavesNoPassageBehindOnceDeleted() {
	_, first, _ := s.upload("grown.md", []byte(sections(5)), "grown")
	s.Require().Equal(KnowledgeDocumentStateIndexed, s.settled(first.Id).State)

	s.knowledge.refuseWritesAfter(1)
	s.T().Cleanup(func() { s.knowledge.failWrites(false) })
	_, second, _ := s.upload("grown.md", []byte(sections(knowledge.WriteBatch+10)), "grown")
	read := s.settled(second.Id)
	s.knowledge.failWrites(false)
	s.Require().Equal(KnowledgeDocumentStateFailed, read.State)

	s.Require().Equal(http.StatusNoContent, s.serverClient.do(http.MethodDelete, "/v1/agents/knowledge/documents/"+second.Id, nil, nil))

	s.Zero(s.passagesOf("grown"))
}

func (s *KnowledgeFilesSuite) TestTextPostedIsIndexedAtOnce() {
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost, "/v1/agents/knowledge", map[string]any{
		"namespace": s.namespace,
		"documents": []map[string]string{{"source": "hours.md", "text": "# Hours\n\nNine to five, every day but Sunday."}},
	}, nil))

	listed := s.listed()

	s.Require().Len(listed, 1)
	s.Equal(KnowledgeDocumentStateIndexed, listed[0].State)
	s.Equal("text/markdown", listed[0].ContentType)
	s.EqualValues(len("# Hours\n\nNine to five, every day but Sunday."), listed[0].Bytes)
}

func (s *KnowledgeFilesSuite) TestAFormWithoutAFileIsRefused() {
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	s.Require().NoError(form.WriteField("note", "no file here"))
	s.Require().NoError(form.Close())

	status, answer := s.postKnowledgeFile(s.serverClient, body.Bytes(), form.FormDataContentType(), url.Values{"namespace": {s.namespace}})

	s.Equal(http.StatusBadRequest, status)
	s.Contains(string(answer), "no part named file")
}

func (s *KnowledgeFilesSuite) TestANamespaceIsRequired() {
	body, contentType := formWith("notes.md", []byte("# Notes"))

	status, _ := s.postKnowledgeFile(s.serverClient, body, contentType, url.Values{})

	s.Equal(http.StatusBadRequest, status)
}

func (s *KnowledgeFilesSuite) TestOnlyTheCustomersOwnBackendMayUpload() {
	s.assertPosture(serverOnly, func(as *testClient) int {
		body, contentType := formWith("notes.md", []byte("# Notes"))
		status, _ := s.postKnowledgeFile(as, body, contentType, url.Values{"namespace": {s.namespace}})
		return status
	})
}

// upload sends a file as the customer's backend, under source when it is given.
func (s *KnowledgeFilesSuite) upload(name string, data []byte, source string) (int, IndexedKnowledgeDocument, []byte) {
	body, contentType := formWith(name, data)
	query := url.Values{"namespace": {s.namespace}}
	if source != "" {
		query.Set("source", source)
	}
	status, answer := s.postKnowledgeFile(s.serverClient, body, contentType, query)
	var sent IndexedKnowledgeDocument
	if status == http.StatusAccepted {
		s.Require().NoError(json.Unmarshal(answer, &sent), string(answer))
	}
	return status, sent, answer
}

// postKnowledgeFile sends body to the upload endpoint as the client as, and returns the status
// and what it answered.
func (s *RouterSuite) postKnowledgeFile(as *testClient, body []byte, contentType string, query url.Values) (int, []byte) {
	request, err := http.NewRequest(http.MethodPost, as.base()+knowledgeFilesPath+"?"+query.Encode(), bytes.NewReader(body))
	s.Require().NoError(err)
	request.Header = as.header.Clone()
	request.Header.Set("Content-Type", contentType)
	response, err := s.server.Client().Do(request)
	s.Require().NoError(err)
	defer response.Body.Close()
	answer, err := readAll(response)
	s.Require().NoError(err)
	return response.StatusCode, answer
}

// settled is the document once its file has been read, or has failed to be.
func (s *KnowledgeFilesSuite) settled(id string) IndexedKnowledgeDocument {
	var read IndexedKnowledgeDocument
	s.Require().Eventually(func() bool {
		read = IndexedKnowledgeDocument{}
		status := s.serverClient.do(http.MethodGet, "/v1/agents/knowledge/documents/"+id, nil, &read)
		return status == http.StatusOK && read.State != KnowledgeDocumentStatePending
	}, 30*time.Second, 20*time.Millisecond, "the file was never read")
	return read
}

func (s *KnowledgeFilesSuite) remove(id string) int {
	return s.serverClient.do(http.MethodDelete, "/v1/agents/knowledge/documents/"+id, nil, nil)
}

// post sends source as text of n sections, and returns the status.
func (s *KnowledgeFilesSuite) post(source string, n int) int {
	return s.serverClient.do(http.MethodPost, "/v1/agents/knowledge", map[string]any{
		"namespace": s.namespace, "documents": []map[string]string{{"source": source, "text": sections(n)}},
	}, nil)
}

// indexedWith checks source is the one document listed, indexed with n passages that are stored.
func (s *KnowledgeFilesSuite) indexedWith(source string, n int) {
	listed := s.listed()
	s.Require().Len(listed, 1)
	s.Equal(KnowledgeDocumentStateIndexed, listed[0].State)
	s.Equal(n, listed[0].Passages)
	s.Equal(n, s.passagesOf(source))
}

// forgetStored takes the passages of source out of the base behind the router's back.
func (s *KnowledgeFilesSuite) forgetStored(source string) {
	s.knowledge.mu.Lock()
	defer s.knowledge.mu.Unlock()
	passages := s.knowledge.passages[knowledge.Scoped(s.customerID(), s.namespace)]
	for id, passage := range passages {
		if strings.HasPrefix(passage.Source, source+" > ") {
			delete(passages, id)
		}
	}
}

// recorded is how many of the app's documents' writes are on its history.
func (s *KnowledgeFilesSuite) recorded() int {
	count, err := s.store.GlobalDB().NewSelect().Model((*store.AuditEntry)(nil)).
		Where("customer_id = ? AND resource_type = ? AND action = ?", s.customerID(), store.AuditKnowledge, store.AuditCreated).
		Count(context.Background())
	s.Require().NoError(err)
	return count
}

// answer is the status a request made meanwhile came back with.
func (s *KnowledgeFilesSuite) answer(status <-chan int) int {
	select {
	case answered := <-status:
		return answered
	case <-time.After(30 * time.Second):
		s.Fail("the request was never answered")
		return 0
	}
}

// holdWrite holds the next write to the base until write is called, and closes writing once
// it is there.
func (s *KnowledgeFilesSuite) holdWrite() (writing chan struct{}, write func()) {
	writing, released := make(chan struct{}), make(chan struct{})
	write = sync.OnceFunc(func() { close(released) })
	s.T().Cleanup(write)
	s.knowledge.onceBeforeWrite(func() {
		close(writing)
		<-released
	})
	return writing, write
}

func (s *KnowledgeFilesSuite) reached(point <-chan struct{}, otherwise string) {
	select {
	case <-point:
	case <-time.After(30 * time.Second):
		s.FailNow(otherwise)
	}
}

// everyFileRead waits until no uploaded file waits to be read or is being read.
func (s *KnowledgeFilesSuite) everyFileRead() {
	inspector := asynq.NewInspector(s.knowledgeRedis)
	defer func() { s.NoError(inspector.Close()) }()
	s.Eventually(func() bool {
		queue, err := inspector.GetQueueInfo(s.knowledgeQueue)
		return err == nil && queue.Pending+queue.Active+queue.Scheduled+queue.Retry == 0
	}, 30*time.Second, 20*time.Millisecond, "a file is still being read")
}

// removals is how many times the document's removal is on the app's history.
func (s *KnowledgeFilesSuite) removals(id string) int {
	count, err := s.store.GlobalDB().NewSelect().Model((*store.AuditEntry)(nil)).
		Where("customer_id = ? AND resource_id = ? AND action = ?", s.customerID(), id, store.AuditDeleted).
		Count(context.Background())
	s.Require().NoError(err)
	return count
}

func (s *KnowledgeFilesSuite) listed() []IndexedKnowledgeDocument {
	var listed []IndexedKnowledgeDocument
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet,
		"/v1/agents/knowledge/documents?namespace="+s.namespace, nil, &listed))
	return listed
}

// passagesOf is how many stored passages belong to source.
func (s *KnowledgeFilesSuite) passagesOf(source string) int {
	s.knowledge.mu.Lock()
	defer s.knowledge.mu.Unlock()
	count := 0
	for _, passage := range s.knowledge.passages[knowledge.Scoped(s.customerID(), s.namespace)] {
		if passage.Source == source || strings.HasPrefix(passage.Source, source+" > ") {
			count++
		}
	}
	return count
}

// passageContaining is the source of the stored passage that holds text.
func (s *KnowledgeFilesSuite) passageContaining(text string) string {
	_, passages := s.knowledge.stored()
	for _, passage := range passages {
		if strings.Contains(passage.Text, text) {
			return passage.Source
		}
	}
	s.FailNow("no passage holds the text", text)
	return ""
}

// formWith is a multipart form holding one file, as a browser's FormData sends it.
func formWith(name string, data []byte) ([]byte, string) {
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	part, err := form.CreateFormFile("file", name)
	if err != nil {
		panic(err)
	}
	if _, err := part.Write(data); err != nil {
		panic(err)
	}
	if err := form.Close(); err != nil {
		panic(err)
	}
	return body.Bytes(), form.FormDataContentType()
}

// extractFixture is one of the files extract is tested against.
func extractFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "knowledge", "extract", "testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}
