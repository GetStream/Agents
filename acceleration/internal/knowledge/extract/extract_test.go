package extract

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/xml"
	"errors"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/klippa-app/go-pdfium"
	"github.com/klippa-app/go-pdfium/requests"
	"github.com/klippa-app/go-pdfium/responses"
	"github.com/stretchr/testify/suite"
	"github.com/xuri/excelize/v2"
)

type ExtractSuite struct {
	suite.Suite
	extractor *Extractor
}

func TestExtractSuite(t *testing.T) {
	suite.Run(t, new(ExtractSuite))
}

func (s *ExtractSuite) SetupSuite() {
	s.extractor = New(Options{})
}

func (s *ExtractSuite) TearDownSuite() {
	s.Require().NoError(s.extractor.Close())
}

func (s *ExtractSuite) fixture(name string) []byte {
	data, err := os.ReadFile(filepath.Join("testdata", name))
	s.Require().NoError(err)
	return data
}

func (s *ExtractSuite) extract(name string) Document {
	document, err := s.extractor.Extract(context.Background(), name, s.fixture(name))
	s.Require().NoError(err)
	return document
}

func (s *ExtractSuite) page(document Document, number int) string {
	for _, page := range document.Pages {
		if page.Number == number {
			return page.Text
		}
	}
	s.FailNow("no such page", "page %d of %d", number, len(document.Pages))
	return ""
}

func (s *ExtractSuite) requireReason(err error, reason Reason) {
	s.Require().Error(err)
	got, ok := ReasonOf(err)
	s.Require().True(ok, "%v says no reason", err)
	s.Equal(reason, got, err.Error())
}

// rezip is a copy of an archive with some of its parts replaced or added.
func (s *ExtractSuite) rezip(data []byte, parts map[string][]byte) []byte {
	archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	s.Require().NoError(err)
	var out bytes.Buffer
	writer := zip.NewWriter(&out)
	for _, file := range archive.File {
		if _, replaced := parts[file.Name]; replaced {
			continue
		}
		s.Require().NoError(writer.Copy(file))
	}
	for name, content := range parts {
		part, err := writer.Create(name)
		s.Require().NoError(err)
		_, err = part.Write(content)
		s.Require().NoError(err)
	}
	s.Require().NoError(writer.Close())
	return out.Bytes()
}

func (s *ExtractSuite) TestAPDFPageSaysWhatIsOnItUnderItsNumber() {
	document := s.extract("manual.pdf")

	s.Equal(PDF, document.Format)
	s.Len(document.Pages, 12)
	s.Contains(s.page(document, 7), "Fryer oil is changed every 48 hours, or sooner when it smokes or darkens.",
		"the lines of a paragraph are one paragraph")
	s.NotContains(s.page(document, 6), "Fryer oil")
}

func (s *ExtractSuite) TestAPDFRunningHeadAndPageNumbersAreLeftOutButItsChapterTitlesAreNot() {
	document := s.extract("manual.pdf")

	for _, page := range document.Pages {
		s.NotContains(page.Text, "Kitchen Operations", "page %d repeats the running head", page.Number)
		lines := strings.Split(page.Text, "\n")
		s.NotEqual(strconv.Itoa(page.Number), lines[len(lines)-1], "page %d ends with its number", page.Number)
	}
	s.True(strings.HasPrefix(s.page(document, 3), "# Chapter 3: Station 3\n\n"), s.page(document, 3))
}

func (s *ExtractSuite) TestAPDFTableIsATable() {
	text := s.page(s.extract("manual.pdf"), 5)

	s.Contains(text, "| Equipment | Frequency | Owner |\n| --- | --- | --- |\n| Fryer | Daily | Shift lead |\n| Grill | Every shift | Cook |")
	s.Contains(text, "| Ice machine | Weekly | Manager |\n\nLog every cleaning", "the paragraph under the table is not a row of it")
}

func (s *ExtractSuite) TestAPDFBulletedListIsAListUnderItsHeading() {
	text := s.page(s.extract("manual.pdf"), 3)

	s.Contains(text, "## Before you start\n\n- Wash your hands\n- Put on gloves\n- Check the thermometer")
}

func (s *ExtractSuite) TestTwoColumnsAreReadOneAfterTheOther() {
	// The file draws the columns a line of each at a time, so read in its own order they
	// interleave.
	text := s.page(s.extract("columns.pdf"), 1)

	s.Contains(text, "# Newsletter\n\nThe left column starts the story about the new menu, which launches in every store on the first of the month with three new items.")
	s.Contains(text, "The right column carries on with the training plan: every team member watches the video and passes the quiz before the launch day.")
	s.Less(strings.Index(text, "left column"), strings.Index(text, "right column"))
}

func (s *ExtractSuite) TestAScannedPageIsMarkedAndCanBeDrawn() {
	document := s.extract("scanned.pdf")

	s.Equal([]int{2}, document.Scanned(), "the page with a text layer is read, the picture of one is not")
	s.Contains(s.page(document, 1), "This first page has a text layer.")

	drawn, err := document.Render(context.Background(), 2)
	s.Require().NoError(err)
	s.Equal("image/jpeg", drawn.MIME)
	picture, err := jpeg.Decode(bytes.NewReader(drawn.Data))
	s.Require().NoError(err)
	s.Greater(picture.Bounds().Dy(), 1500, "an A4 page drawn finely enough to read")
}

func (s *ExtractSuite) TestAPageWithAChartOrAPictureOnItIsMarkedForItsFigures() {
	document := s.extract("figures.pdf")

	s.Equal([]int{1, 3, 5, 7}, document.Figured(),
		"a bar chart, a pie chart, a photo and a chart drawn as a form are figures; "+
			"a ruled and shaded table, a logo and a tinted page are not")
	s.Empty(document.Scanned())
	s.Contains(s.page(document, 1), "Week 1", "a figured page's own text is read as ever")
	s.Empty(s.extract("manual.pdf").Figured(), "a manual of text, lists and tables has nothing to summarise")
}

func (s *ExtractSuite) TestAScannedPageIsNotAlsoMarkedForItsFigures() {
	document := s.extract("scanned.pdf")

	s.Empty(document.Figured(), "reading a scan reads its figures")
}

func (s *ExtractSuite) TestAPageReadsWithWhatItsFiguresSaidAfterItsText() {
	document := Document{Pages: []Page{
		{Number: 1, Text: "Oil is changed every 48 hours.", Figures: "> **Figure:** Oil changes by week."},
		{Number: 2, Figures: "> **Figure:** A fryer's parts."},
	}}

	s.Equal("<!-- page 1 -->\n\nOil is changed every 48 hours.\n\n> **Figure:** Oil changes by week.\n\n"+
		"<!-- page 2 -->\n\n> **Figure:** A fryer's parts.", document.Markdown())
}

func (s *ExtractSuite) TestAPDFWithAPasswordIsRefusedAsEncrypted() {
	_, err := s.extractor.Extract(context.Background(), "encrypted.pdf", s.fixture("encrypted.pdf"))

	s.requireReason(err, Encrypted)
}

func (s *ExtractSuite) TestABrokenPDFIsRefusedAsCorrupt() {
	_, err := s.extractor.Extract(context.Background(), "corrupt.pdf", s.fixture("corrupt.pdf"))

	s.requireReason(err, Corrupt)
}

func (s *ExtractSuite) TestAPDFWithMorePagesThanAllowedIsRefused() {
	small := New(Options{MaxPages: 5})
	defer small.Close()

	_, err := small.Extract(context.Background(), "manual.pdf", s.fixture("manual.pdf"))

	s.requireReason(err, TooLarge)
}

func (s *ExtractSuite) TestReadingStopsAtTheDeadline() {
	hurried := New(Options{Timeout: time.Nanosecond})
	defer hurried.Close()

	_, err := hurried.Extract(context.Background(), "manual.pdf", s.fixture("manual.pdf"))

	s.Require().ErrorIs(err, context.DeadlineExceeded)
}

// A context cancelled before the read starts kills the worker while the read opens the
// document, and the open that fails then closes the worker. Run under -race, as CI does, a
// close while the kill is still going on is a data race in PDFium's instance. The kill runs in
// a goroutine of its own, so a read can also finish before it and return no error.
func (s *ExtractSuite) TestAReadCancelledBeforeItStartsKillsItsWorkerBeforeClosingIt() {
	data := s.fixture("manual.pdf")
	for range 200 {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := s.extractor.Extract(ctx, "manual.pdf", data)

		if err != nil {
			s.Require().ErrorIs(err, context.Canceled)
		}
	}
}

// A deadline that passes after the document opened kills the worker while the read closes it.
// Run under -race, a close that does not wait for the kill is a data race in PDFium's
// instance. Such a close can also leave the pool's lock held, so the next open never returns:
// each open and close gets half a minute, on an extractor of the test's own whose engine has
// started already.
func (s *ExtractSuite) TestClosingAReadWaitsForAKillThatBeganAfterTheDocumentOpened() {
	extractor := New(Options{})
	_, err := extractor.engine()
	s.Require().NoError(err)
	data := s.fixture("manual.pdf")
	for range 200 {
		closed := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			session, err := extractor.open(ctx, data)
			if err == nil {
				cancel()
				session.close()
			}
			closed <- err
		}()

		select {
		case err := <-closed:
			s.Require().NoError(err)
		case <-time.After(30 * time.Second):
			s.FailNow("an open has waited half a minute for a worker")
		}
	}
	s.Require().NoError(extractor.Close())
}

// A deadline that passes while a page's objects are walked kills the worker between two of
// its calls. The read then fails on the next call, whichever of the walk's calls it is.
func (s *ExtractSuite) TestAWorkerKilledWhileAPageIsWalkedFailsTheRead() {
	data := s.fixture("figures.pdf")
	for _, call := range []string{"FPDFPage_CountObjects", "FPDFPage_GetObject", "FPDFFormObj_CountObjects", "FPDFFormObj_GetObject"} {
		session, err := s.extractor.open(context.Background(), data)
		s.Require().NoError(err, call)
		killed := &killedAt{Pdfium: session.instance, call: call}
		session.instance = killed

		var readErr error
		s.Require().NotPanics(func() {
			for index := 0; readErr == nil; index++ {
				_, readErr = session.read(index)
			}
		}, call)
		session.close()

		s.Require().True(killed.done, "%s: the read reached the call", call)
		s.Require().ErrorContains(readErr, "instance is closed", call)
	}
}

func (s *ExtractSuite) TestReadingEveryFormatStopsAtTheDeadline() {
	hurried := New(Options{Timeout: time.Nanosecond})
	defer hurried.Close()

	for _, name := range []string{"handbook.docx", "handbook.odt", "handbook.rtf", "policies.doc", "menu.xlsx"} {
		_, err := hurried.Extract(context.Background(), name, s.fixture(name))

		s.Require().ErrorIs(err, context.DeadlineExceeded, name)
		_, ok := ReasonOf(err)
		s.False(ok, "%s: a file stopped at the deadline is not called corrupt", name)
	}
}

// paddedJPEG is a JPEG a model could be shown, followed by padding to size bytes. Only its
// header is read, and the padding compresses a thousand times.
func (s *ExtractSuite) paddedJPEG(size int, fill byte) []byte {
	var b bytes.Buffer
	s.Require().NoError(jpeg.Encode(&b, image.NewRGBA(image.Rect(0, 0, 200, 200)), nil))
	return append(b.Bytes(), bytes.Repeat([]byte{fill}, size-b.Len())...)
}

// wordWithPictures is figures.docx with a body that places each of ids in turn, and
// with the parts given, each named in its relationships by its file name.
func (s *ExtractSuite) wordWithPictures(ids []string, parts map[string][]byte) []byte {
	data := s.fixture("figures.docx")
	archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	s.Require().NoError(err)
	rels, err := zipFile(archive, "word/_rels/document.xml.rels")
	s.Require().NoError(err)
	var added strings.Builder
	replaced := map[string][]byte{}
	for name, part := range parts {
		id := strings.TrimSuffix(name, ".jpeg")
		added.WriteString(`<Relationship Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/image" Id="` + id + `" Target="media/` + name + `" />`)
		replaced["word/media/"+name] = part
	}
	replaced["word/_rels/document.xml.rels"] = bytes.Replace(rels, []byte("</Relationships>"), []byte(added.String()+"</Relationships>"), 1)
	var body strings.Builder
	body.WriteString(`<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main" xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><w:body><w:p><w:r><w:t>Photos.</w:t></w:r>`)
	for _, id := range ids {
		body.WriteString(`<w:r><a:blip r:embed="` + id + `"/></w:r>`)
	}
	body.WriteString(`</w:p></w:body></w:document>`)
	replaced["word/document.xml"] = []byte(body.String())
	return s.rezip(data, replaced)
}

func (s *ExtractSuite) TestTheKeptPicturesOfADocumentStopAtTheirByteBudget() {
	parts := map[string][]byte{}
	var ids []string
	for n := range 8 {
		id := "big" + strconv.Itoa(n)
		parts[id+".jpeg"] = s.paddedJPEG(9<<20, byte(n))
		ids = append(ids, id)
	}
	data := s.wordWithPictures(ids, parts)
	s.Less(len(data), 1<<20, "eight 9 MiB pictures pack into a small file")

	document, err := s.extractor.Extract(context.Background(), "photos.docx", data)

	s.Require().NoError(err)
	s.Equal(7, len(document.Pictures), "the eighth would take the pictures past the budget")
	size := 0
	for _, picture := range document.Pictures {
		size += len(picture.Data)
	}
	s.LessOrEqual(size, maxKeptBytes)
}

func (s *ExtractSuite) TestAPictureNamedOverAndOverIsUnpackedOnce() {
	// Too large to send, so it is never kept, and each time it is named it would be
	// unpacked again: a body naming it thousands of times would read for minutes.
	ids := make([]string, 5000)
	for i := range ids {
		ids[i] = "huge"
	}
	data := s.wordWithPictures(ids, map[string][]byte{"huge.jpeg": s.paddedJPEG(60<<20, 0)})
	hurried := New(Options{Timeout: 10 * time.Second})
	defer hurried.Close()

	document, err := hurried.Extract(context.Background(), "named.docx", data)

	s.Require().NoError(err)
	s.Empty(document.Pictures)
	s.Contains(document.Pages[0].Text, "Photos.")
}

func (s *ExtractSuite) TestAWordDocumentKeepsItsHeadingsListsTablesAndPages() {
	document := s.extract("handbook.docx")

	s.Equal(DOCX, document.Format)
	s.Require().Len(document.Pages, 2)
	first := s.page(document, 1)
	s.Contains(first, "# Employee Handbook\n\nWelcome to the team.")
	s.Contains(first, "## Uniforms\n\n- Wear a clean shirt\n- Wear non-slip shoes\n  - Black or brown\n1. Clock in\n1. Wash your hands")
	second := s.page(document, 2)
	s.True(strings.HasPrefix(second, "Page two starts with the break policy."), second)
	s.Contains(second, "| Shift length | Break |\n| --- | --- |\n| 6 hours | 30 minutes |\n| 8 hours | 45 minutes |")
}

func (s *ExtractSuite) TestAWordDocumentThatRecordsNoPagesIsOnePageWithNoNumber() {
	// A break put in by hand is the only one such a file says anything about, and
	// numbering by those would call the thirtieth page the second.
	data := s.fixture("handbook.docx")
	archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	s.Require().NoError(err)
	body, err := zipFile(archive, "word/document.xml")
	s.Require().NoError(err)
	body = bytes.ReplaceAll(body, []byte("<w:lastRenderedPageBreak/>"), nil)

	document, err := s.extractor.Extract(context.Background(), "handbook.docx", s.rezip(data, map[string][]byte{"word/document.xml": body}))

	s.Require().NoError(err)
	s.Require().Len(document.Pages, 1)
	s.Zero(document.Pages[0].Number)
	s.Contains(document.Pages[0].Text, "Page two starts with the break policy.")
	s.NotContains(document.Markdown(), "<!-- page")
}

func (s *ExtractSuite) TestAWordDocumentWithAPasswordIsRefusedAsEncrypted() {
	_, err := s.extractor.Extract(context.Background(), "encrypted.docx", s.fixture("encrypted.docx"))

	s.requireReason(err, Encrypted)
}

func (s *ExtractSuite) TestAPartThatUnpacksPastTheLimitIsRefused() {
	// Zeros compress a thousand times over, so this archive is small and would unpack to
	// more than the limit.
	bomb := bytes.Repeat([]byte{' '}, maxUnzipped+1)
	data := s.rezip(s.fixture("handbook.docx"), map[string][]byte{"word/document.xml": bomb})
	s.Less(len(data), 1<<20)

	_, err := s.extractor.Extract(context.Background(), "bomb.docx", data)

	s.requireReason(err, TooLarge)
}

func (s *ExtractSuite) TestAnOpenDocumentKeepsItsHeadingsListsTablesAndPages() {
	document := s.extract("handbook.odt")

	s.Equal(ODT, document.Format)
	s.Require().Len(document.Pages, 2)
	s.Contains(s.page(document, 1), "## Uniforms\n\n- Wear a clean shirt\n- Wear non-slip shoes\n  - Black or brown\n1. Clock in\n1. Wash your hands")
	s.Contains(s.page(document, 2), "## Breaks\n\n| Shift length | Break |\n| --- | --- |\n| 6 hours | 30 minutes |")
}

func (s *ExtractSuite) TestAnOpenDocumentWithAPasswordIsRefusedAsEncrypted() {
	manifest := []byte(`<manifest:manifest><manifest:file-entry manifest:full-path="content.xml"><manifest:encryption-data/></manifest:file-entry></manifest:manifest>`)
	data := s.rezip(s.fixture("handbook.odt"), map[string][]byte{"META-INF/manifest.xml": manifest})

	_, err := s.extractor.Extract(context.Background(), "locked.odt", data)

	s.requireReason(err, Encrypted)
}

func (s *ExtractSuite) TestRichTextKeepsItsHeadingsListsAndTables() {
	document := s.extract("handbook.rtf")

	s.Equal(RTF, document.Format)
	s.Require().Len(document.Pages, 1)
	s.Zero(document.Pages[0].Number, "RTF records no pages")
	text := document.Pages[0].Text
	s.True(strings.HasPrefix(text, "# Employee Handbook\n\nWelcome to the team."), text)
	s.Contains(text, "- Wear non-slip shoes\n  - Black or brown\n1. Clock in")
	s.Contains(text, "## Breaks\n\n| Shift length | Break |\n| --- | --- |\n| 6 hours | 30 minutes |\n| 8 hours | 45 minutes |\n\nBreaks are taken in the back room.")
	s.NotContains(text, "Helvetica", "the font table is not text")
}

func (s *ExtractSuite) TestRichTextReadsItsCodePageAndUnicode() {
	// \'e9 is é in Windows-1252, and the ? after \u8364 is what a reader without Unicode
	// would show instead of the euro sign.
	data := []byte(`{\rtf1\ansi\ansicpg1252{\fonttbl{\f0 Arial;}}\pard Caf\'e9 prices in \u8364?.\par}`)

	document, err := s.extractor.Extract(context.Background(), "prices.rtf", data)

	s.Require().NoError(err)
	s.Equal("Café prices in €.", document.Pages[0].Text)
}

func (s *ExtractSuite) TestALegacyWordDocumentKeepsItsParagraphsListsAndTables() {
	document := s.extract("policies.doc")

	s.Equal(DOC, document.Format)
	text := document.Pages[0].Text
	s.Contains(text, "Store Policies\n\nOpening hours are 6am to 10pm.")
	s.Contains(text, "- Wash hands\n- Wear gloves")
	s.Contains(text, "| Item | Price |\n| --- | --- |\n| Sandwich | 4.50 |\n| Lemonade | 2.25 |\n\nRefunds are given within 30 days with a receipt.")
}

func (s *ExtractSuite) TestALegacyWordDocumentReadsItsPiecesInOrderOnce() {
	word, table := s.wordStream("Opening hours are 6am to 10pm.", []uint32{0, 13, 30})

	text, err := docText(context.Background(), word, table)

	s.Require().NoError(err)
	s.Equal("Opening hours are 6am to 10pm.", string(text))
}

// A piece table that goes back to the start a thousand times would hold the text a
// thousand times over.
func (s *ExtractSuite) TestALegacyWordDocumentWhosePiecesGoBackIsRefused() {
	cps := []uint32{0}
	for range 1000 {
		cps = append(cps, 30, 0)
	}
	word, table := s.wordStream("Opening hours are 6am to 10pm.", cps)

	text, err := docText(context.Background(), word, table)

	s.Require().Error(err)
	s.Empty(text)
}

// wordStream is the WordDocument and table streams of a .doc holding text as one-byte
// characters, its piece table cut at the character positions cps, every piece reading
// from the start of the text.
func (s *ExtractSuite) wordStream(text string, cps []uint32) (word, table []byte) {
	const lw = fibCsw + 4
	word = make([]byte, lw+4*4+2+(clxIndex+1)*8)
	binary.LittleEndian.PutUint16(word[fibCsw+2:], 4)
	binary.LittleEndian.PutUint32(word[lw+12:], uint32(len(text)))
	textAt := len(word)
	word = append(word, text...)

	pieces := len(cps) - 1
	table = []byte{0x02}
	table = binary.LittleEndian.AppendUint32(table, uint32(12*pieces+4))
	for _, cp := range cps {
		table = binary.LittleEndian.AppendUint32(table, cp)
	}
	for _, cp := range cps[:pieces] {
		table = binary.LittleEndian.AppendUint16(table, 0)
		table = binary.LittleEndian.AppendUint32(table, 0x40000000|uint32(textAt+int(cp))*2)
		table = binary.LittleEndian.AppendUint16(table, 0)
	}
	pair := lw + 4*4 + 2 + clxIndex*8
	binary.LittleEndian.PutUint32(word[pair+4:], uint32(len(table)))
	return word, table
}

func (s *ExtractSuite) TestAWorkbookIsATablePerSheetWithItsValuesAsTheSheetShowsThem() {
	document := s.extract("menu.xlsx")

	s.Equal(XLSX, document.Format)
	text := document.Pages[0].Text
	s.Contains(text, "## Menu\n\n| Item | Price | Calories |\n| --- | --- | --- |\n| Chicken sandwich | $4.50 | 440 |")
	s.Contains(text, "## Hours\n\n| Day | Opens | Closes |")
	s.NotContains(text, "Empty", "a sheet with nothing on it is left out")
}

func (s *ExtractSuite) TestAPresentationIsRefusedAsUnsupported() {
	var out bytes.Buffer
	writer := zip.NewWriter(&out)
	part, err := writer.Create("ppt/presentation.xml")
	s.Require().NoError(err)
	_, err = part.Write([]byte("<p:presentation/>"))
	s.Require().NoError(err)
	s.Require().NoError(writer.Close())

	_, err = s.extractor.Extract(context.Background(), "deck.pptx", out.Bytes())

	s.requireReason(err, Unsupported)
}

func (s *ExtractSuite) TestTextIsReadAsItIsWithoutItsByteOrderMark() {
	document, err := s.extractor.Extract(context.Background(), "notes.md", []byte("\xEF\xBB\xBF# Notes\n\nOpen at six."))

	s.Require().NoError(err)
	s.Equal(Text, document.Format)
	s.Equal([]Page{{Text: "# Notes\n\nOpen at six."}}, document.Pages)
}

func (s *ExtractSuite) TestBytesThatAreNoFormatAreRefusedAsUnsupported() {
	_, err := s.extractor.Extract(context.Background(), "photo.png", []byte("\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"))

	s.requireReason(err, Unsupported)
}

func (s *ExtractSuite) TestAPageMarkerNamesThePageItOpens() {
	document := Document{Pages: []Page{{Number: 1, Text: "One."}, {Number: 2, Text: ""}, {Number: 3, Text: "Three."}}}

	s.Equal("<!-- page 1 -->\n\nOne.\n\n<!-- page 2 -->\n\n<!-- page 3 -->\n\nThree.", document.Markdown())
	number, ok := PageOf("  <!-- page 12 -->  ")
	s.True(ok)
	s.Equal(12, number)
	_, ok = PageOf("<!-- page twelve -->")
	s.False(ok)
}

func (s *ExtractSuite) TestAClosedExtractorRefusesAPDF() {
	closed := New(Options{})
	s.Require().NoError(closed.Close())

	_, err := closed.Extract(context.Background(), "manual.pdf", s.fixture("manual.pdf"))

	s.Require().Error(err)
	_, ok := ReasonOf(err)
	s.False(ok, "a closed extractor is not a fault of the file")
	s.False(errors.Is(err, context.DeadlineExceeded))
}

func (s *ExtractSuite) TestEveryFormatThatCanBeReadPassesASniff() {
	for _, name := range []string{"manual.pdf", "handbook.docx", "handbook.odt", "handbook.rtf", "menu.xlsx", "policies.doc"} {
		s.NoError(Sniff(s.fixture(name)[:SniffBytes]), name)
	}
	s.NoError(Sniff([]byte("# Opening hours\n\nNine to five.")))
}

func (s *ExtractSuite) TestAPhotoIsRefusedFromItsFirstBytes() {
	photo := append([]byte{0xFF, 0xD8, 0xFF, 0xE0, 0, 0x10}, []byte("JFIF\x00")...)

	s.requireReason(Sniff(photo), Unsupported)
}

func (s *ExtractSuite) TestTextCutPartWayThroughACharacterPassesASniff() {
	text := bytes.Repeat([]byte("é"), SniffBytes)

	s.NoError(Sniff(text[:SniffBytes-1]))
}

func (s *ExtractSuite) TestAWordOrOpenDocumentPictureStandsWhereItIsUntilItIsRead() {
	for _, name := range []string{"figures.docx", "figures.odt"} {
		document := s.extract(name)

		s.Require().Len(document.Pictures, 2, "%s: the chart once, though it is placed twice, and the picture in the table; not the icon", name)
		s.Equal("image/png", document.Pictures[0].MIME, name)
		s.Equal("image/png", document.Pictures[1].MIME, "%s: a GIF is shown to a model as a PNG", name)
		s.Contains(document.Pages[0].Text, "The fryer is cleaned every night.\n\n"+PictureMarker(1), name)
		s.Contains(document.Pages[0].Text, "| 1 | "+PictureMarker(2)+" |", name)
		s.NotContains(document.Markdown(), "<!-- picture", "%s: a picture not read is left out", name)
	}
}

func (s *ExtractSuite) TestAPictureReadIsWrittenWhereItIs() {
	document := s.extract("figures.docx")
	document.Pictures[0].Text = "Fryer oil changes\n\n> **Figure:** Changes rise from week 1 to week 4."
	document.Pictures[1].Text = "> **Figure:** A map\nof the kitchen."

	text := document.Markdown()

	s.Contains(text, "every night.\n\nFryer oil changes\n\n> **Figure:** Changes rise from week 1 to week 4.\n\nAn icon")
	s.Contains(text, "| 1 | > **Figure:** A map of the kitchen. |", "a table's cell is one line")
}

func (s *ExtractSuite) TestAnExcelPictureFollowsItsSheetsTable() {
	document := s.extract("figures.xlsx")

	s.Require().Len(document.Pictures, 1)
	s.Equal("## Oil\n\n| Week | Changes |\n| --- | --- |\n| 1 | 4 |\n\n"+PictureMarker(1), document.Pages[0].Text)
}

func (s *ExtractSuite) TestAPictureTooSmallToBeAFigureOrInNoFormatAModelReadsIsNotKept() {
	var kept pictures
	icon := image.NewRGBA(image.Rect(0, 0, 40, 40))
	var small bytes.Buffer
	s.Require().NoError(png.Encode(&small, icon))

	s.Empty(kept.keep(small.Bytes()))
	s.Empty(kept.keep([]byte("\x01\x00\x00\x00 an EMF drawing")))
	s.Empty(kept.kept)
}

// killedAt is a PDF worker that is killed as a read reaches call, the way a deadline that
// passes during the read kills it.
type killedAt struct {
	pdfium.Pdfium
	call string
	done bool
}

func (k *killedAt) reach(call string) {
	if call == k.call && !k.done {
		k.done = true
		_ = k.Kill()
	}
}

func (k *killedAt) FPDFPage_CountObjects(request *requests.FPDFPage_CountObjects) (*responses.FPDFPage_CountObjects, error) {
	k.reach("FPDFPage_CountObjects")
	return k.Pdfium.FPDFPage_CountObjects(request)
}

func (k *killedAt) FPDFPage_GetObject(request *requests.FPDFPage_GetObject) (*responses.FPDFPage_GetObject, error) {
	k.reach("FPDFPage_GetObject")
	return k.Pdfium.FPDFPage_GetObject(request)
}

func (k *killedAt) FPDFFormObj_CountObjects(request *requests.FPDFFormObj_CountObjects) (*responses.FPDFFormObj_CountObjects, error) {
	k.reach("FPDFFormObj_CountObjects")
	return k.Pdfium.FPDFFormObj_CountObjects(request)
}

func (k *killedAt) FPDFFormObj_GetObject(request *requests.FPDFFormObj_GetObject) (*responses.FPDFFormObj_GetObject, error) {
	k.reach("FPDFFormObj_GetObject")
	return k.Pdfium.FPDFFormObj_GetObject(request)
}

// workbook is a workbook whose rows each hold one value, in the given column.
func (s *ExtractSuite) workbook(rows, column int) []byte {
	book := excelize.NewFile()
	defer book.Close()
	for row := 1; row <= rows; row++ {
		cell, err := excelize.CoordinatesToCellName(column, row)
		s.Require().NoError(err)
		s.Require().NoError(book.SetCellValue("Sheet1", cell, "v"+strconv.Itoa(row)))
	}
	var out bytes.Buffer
	s.Require().NoError(book.Write(&out))
	return out.Bytes()
}

// A row is held as wide as its last value, so a workbook of a few kilobytes with a value in the
// last column of every row would be millions of cells. The read is refused before it holds them.
func (s *ExtractSuite) TestAWorkbookWithAValueInItsLastColumnOfEveryRowIsRefusedWithinABudget() {
	const lastColumn = 16384
	data := s.workbook(1500, lastColumn)
	s.Less(len(data), 32<<10, "the file is a few kilobytes")

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := s.extractor.Extract(context.Background(), "wide.xlsx", data)
	runtime.ReadMemStats(&after)

	s.requireReason(err, TooLarge)
	// The refused row is the first, at most 16,384 cells of 16 bytes. Reading every row as wide
	// allocates gigabytes.
	s.Less(after.TotalAlloc-before.TotalAlloc, uint64(16<<20))
}

func (s *ExtractSuite) TestARowMayBeAsWideAsTheLimitAndNoWider() {
	for _, test := range []struct {
		column int
		reason Reason
	}{
		{maxSheetColumns, ""},
		{maxSheetColumns + 1, TooLarge},
	} {
		_, err := s.extractor.Extract(context.Background(), "wide.xlsx", s.workbook(3, test.column))

		if test.reason == "" {
			s.Require().NoError(err, "column %d", test.column)
			continue
		}
		s.requireReason(err, test.reason)
	}
}

func (s *ExtractSuite) TestAWorkbookMayHoldAsManyCellsAsTheLimitAndNoMore() {
	const column = 1000
	rows := maxWorkbookCells / column

	read, err := s.extractor.Extract(context.Background(), "tall.xlsx", s.workbook(rows, column))
	s.Require().NoError(err)
	s.Contains(read.Pages[0].Text, "v"+strconv.Itoa(rows))

	_, err = s.extractor.Extract(context.Background(), "tall.xlsx", s.workbook(rows+1, column))
	s.requireReason(err, TooLarge)
}

// Cells are counted for the whole workbook: two sheets that each hold half the limit are read,
// and one more cell in either is refused.
func (s *ExtractSuite) TestTheCellLimitIsOfTheWholeWorkbook() {
	const column = 1000
	half := maxWorkbookCells / 2 / column
	for _, test := range []struct {
		second int
		reason Reason
	}{
		{half, ""},
		{half + 1, TooLarge},
	} {
		book := excelize.NewFile()
		_, err := book.NewSheet("Second")
		s.Require().NoError(err)
		for sheet, rows := range map[string]int{"Sheet1": half, "Second": test.second} {
			for row := 1; row <= rows; row++ {
				cell, err := excelize.CoordinatesToCellName(column, row)
				s.Require().NoError(err)
				s.Require().NoError(book.SetCellValue(sheet, cell, "v"))
			}
		}
		var out bytes.Buffer
		s.Require().NoError(book.Write(&out))
		s.Require().NoError(book.Close())

		read, err := s.extractor.Extract(context.Background(), "two.xlsx", out.Bytes())

		if test.reason == "" {
			s.Require().NoError(err)
			s.Contains(read.Pages[0].Text, "## Second")
			continue
		}
		s.requireReason(err, test.reason)
	}
}

// The rows between two rows of values are read as rows with no cells, and each is a line of the
// table, so each counts as a cell.
func (s *ExtractSuite) TestARowWithNoCellsCountsAsACell() {
	row := func(number int) string {
		cell := "A" + strconv.Itoa(number)
		return `<row r="` + strconv.Itoa(number) + `"><c r="` + cell + `" t="inlineStr"><is><t>` + cell + `</t></is></c></row>`
	}

	read, err := s.extractor.Extract(context.Background(), "gaps.xlsx", s.sheet(row(1)+row(maxWorkbookCells)))
	s.Require().NoError(err)
	s.True(strings.HasSuffix(read.Pages[0].Text, "| A"+strconv.Itoa(maxWorkbookCells)+" |"))

	_, err = s.extractor.Extract(context.Background(), "gaps.xlsx", s.sheet(row(1)+row(maxWorkbookCells+1)))
	s.requireReason(err, TooLarge)
}

// A sheet with a value in its last row is a million rows from a few hundred bytes, and a workbook
// of many such sheets is refused before it holds them: reading them all allocates gigabytes, and
// the read is refused at the first sheet's millionth row.
func (s *ExtractSuite) TestAWorkbookOfSheetsReachingTheirLastRowIsRefusedWithinABudget() {
	last := `<row r="1048576"><c r="A1048576" t="inlineStr"><is><t>x</t></is></c></row>`
	for name, test := range map[string]struct {
		sheets    int
		sheetData string
	}{
		"100 sheets with a value in their last row":          {100, last},
		"40 sheets with a value in their first and last row": {40, `<row r="1"><c r="A1" t="inlineStr"><is><t>x</t></is></c></row>` + last},
	} {
		data := s.sheets(test.sheets, test.sheetData)
		s.Less(len(data), 64<<10, "%s: the file is a few kilobytes", name)

		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		_, err := s.extractor.Extract(context.Background(), "far.xlsx", data)
		runtime.ReadMemStats(&after)

		s.requireReason(err, TooLarge)
		s.Less(after.TotalAlloc-before.TotalAlloc, uint64(256<<20), name)
	}
}

// sharedText is a workbook of the given sheets, each of rows × columns cells that all hold one
// shared string of text.
func (s *ExtractSuite) sharedText(sheets, rows, columns int, text string) []byte {
	book := excelize.NewFile()
	defer book.Close()
	for i := 1; i <= sheets; i++ {
		sheet := "S" + strconv.Itoa(i)
		_, err := book.NewSheet(sheet)
		s.Require().NoError(err)
		for row := 1; row <= rows; row++ {
			for column := 1; column <= columns; column++ {
				cell, err := excelize.CoordinatesToCellName(column, row)
				s.Require().NoError(err)
				s.Require().NoError(book.SetCellValue(sheet, cell, "shared"))
			}
		}
	}
	s.Require().NoError(book.DeleteSheet("Sheet1"))
	var out bytes.Buffer
	s.Require().NoError(book.Write(&out))
	// excelize cuts a value to the 32,767 characters a cell may hold, so the one shared string is
	// replaced in the file.
	return s.rezip(out.Bytes(), map[string][]byte{"xl/sharedStrings.xml": []byte(
		`<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" count="1" uniqueCount="1"><si><t>` + text + `</t></si></sst>`)})
}

// One long shared string is the text of every cell it is in. A workbook whose cells are within
// the cell limit but whose text is not is refused, before its cells' copies of it are all held.
func (s *ExtractSuite) TestAWorkbookIsRefusedPastItsTextBudget() {
	long := strings.Repeat("y", 100<<10)
	data := s.sharedText(1, 3, 1000, long)
	s.Less(len(data), 64<<10, "the file is a few kilobytes")

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := s.extractor.Extract(context.Background(), "shared.xlsx", data)
	runtime.ReadMemStats(&after)

	s.requireReason(err, TooLarge)
	// The cells are refused at the budget, twice maxUnzipped. Holding all three thousand copies
	// is more than twice that.
	s.Less(after.TotalAlloc-before.TotalAlloc, uint64(4*maxUnzipped))
}

// A row's cells are each a copy of their text, built at once: one long shared string in every cell
// of one row is that string a thousand times over before the row could be counted. The cells are
// counted as they are stored before any row is built, so the read is refused within the text budget.
func (s *ExtractSuite) TestARowOfOneLongSharedStringIsRefusedBeforeItIsBuilt() {
	data := s.sharedText(1, 1, maxSheetColumns, strings.Repeat("y", 2<<20))
	s.Less(len(data), 64<<10, "the file is a few kilobytes")

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := s.extractor.Extract(context.Background(), "row.xlsx", data)
	runtime.ReadMemStats(&after)

	s.requireReason(err, TooLarge)
	// Building the row allocates two gigabytes, sixteen times the budget.
	s.Less(after.TotalAlloc-before.TotalAlloc, uint64(maxWorkbookText))
}

// cellsNaming is a workbook of one row of cells, each the given XML, and the given shared strings,
// in a part of the given name.
func (s *ExtractSuite) cellsNaming(cells int, cell, part, shared string) []byte {
	sheet := s.sheet(`<row r="1">` + strings.Repeat(cell, cells) + `</row>`)
	return s.rezip(s.withoutPart(sheet, "xl/sharedStrings.xml"), map[string][]byte{part: []byte(
		`<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">` + shared + `</sst>`)})
}

// A cell is counted as the shared string excelize reads for it, at the index it reads it at, or
// one long string in every cell of a row is counted as a short one and built before it is
// refused. excelize reads the strings that are the root's children when their part is small
// enough to hold, and every one in it when it is not, so a string nested one level down moves the
// strings after it to other indices in one way and not the other.
func (s *ExtractSuite) TestASharedStringIsCountedAtTheIndexItIsReadAt() {
	long := `<si><t>` + strings.Repeat("y", 2<<20) + `</t></si>`
	short := `<si><t>short</t></si>`
	first := `<c t="s"><v>0</v></c>`
	for name, test := range map[string]struct {
		data []byte
		// budget is what the read may allocate. Counting a string allocates a few times its
		// length, as the XML decoder grows its buffer, for each of the two ways it is read.
		budget uint64
	}{
		"a part small enough to hold, the first string the root's child": {
			data:   s.cellsNaming(128, first, "xl/sharedStrings.xml", `<x>`+short+`</x>`+long),
			budget: maxWorkbookText,
		},
		"a part too large to hold, the first string nested": {
			data:   s.cellsNaming(32, first, "xl/sharedStrings.xml", `<x><si><t>`+strings.Repeat("y", 17<<20)+`</t></si></x>`+short),
			budget: 2 * maxWorkbookText,
		},
		"a cell whose formula holds another index": {
			data:   s.cellsNaming(128, `<c t="s"><v>0</v><f><v>1</v></f></c>`, "xl/sharedStrings.xml", long+short),
			budget: maxWorkbookText,
		},
		"an index between spaces": {
			data:   s.cellsNaming(128, `<c t="s"><v> 1 </v></c>`, "xl/sharedStrings.xml", short+long),
			budget: maxWorkbookText,
		},
		"a part named in other letters": {
			data:   s.cellsNaming(128, first, "XL/SharedStrings.xml", long),
			budget: maxWorkbookText,
		},
		"a part named with a backslash": {
			data:   s.cellsNaming(128, first, `xl\sharedStrings.xml`, long),
			budget: maxWorkbookText,
		},
		"a string of runs": {
			data:   s.cellsNaming(maxSheetColumns, first, "xl/sharedStrings.xml", `<si><r><t>`+strings.Repeat("y", 1<<20)+`</t></r></si>`),
			budget: maxWorkbookText,
		},
		"a cell of two values, the last naming the long string": {
			data:   s.cellsNaming(128, `<c t="s"><v>1</v><v>0</v></c>`, "xl/sharedStrings.xml", long+short),
			budget: maxWorkbookText,
		},
	} {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		_, err := s.extractor.Extract(context.Background(), "named.xlsx", test.data)
		runtime.ReadMemStats(&after)

		s.requireReason(err, TooLarge)
		s.Contains(err.Error(), "shared strings", "%s: refused before a row is built", name)
		// The row is two to eight times the text budget.
		s.Less(after.TotalAlloc-before.TotalAlloc, test.budget, name)
	}
}

// The text budget may be spent to the byte, and no further. Cells of spaces make no table, so the
// cells alone spend it. Stored, the cells are counted before they are read, and a byte past the
// budget is refused then; a number format that writes a string twice ("@@") makes them twice as
// long as they are read, and they are counted so.
func (s *ExtractSuite) TestTheTextBudgetMayBeSpentToTheByte() {
	spaces := strings.Repeat(" ", 1<<20)
	columns := maxWorkbookText / len(spaces)

	stored := `<si><t>` + spaces + `</t></si><si><t> </t></si>`
	read, err := s.extractor.Extract(context.Background(), "spaces.xlsx",
		s.cellsNaming(columns, `<c t="s"><v>0</v></c>`, "xl/sharedStrings.xml", stored))
	s.Require().NoError(err, "stored, the cells are the budget")
	s.Empty(read.Pages[0].Text)

	_, err = s.extractor.Extract(context.Background(), "spaces.xlsx",
		s.cellsNaming(1, strings.Repeat(`<c t="s"><v>0</v></c>`, columns)+`<c t="s"><v>1</v></c>`, "xl/sharedStrings.xml", stored))
	s.requireReason(err, TooLarge)
	s.Contains(err.Error(), "shared strings", "stored, the cells are the budget and a byte, refused before they are read")

	book := excelize.NewFile()
	defer book.Close()
	twice := "@@"
	style, err := book.NewStyle(&excelize.Style{CustomNumFmt: &twice})
	s.Require().NoError(err)
	for column := 1; column <= columns/2; column++ {
		cell, err := excelize.CoordinatesToCellName(column, 1)
		s.Require().NoError(err)
		s.Require().NoError(book.SetCellValue("Sheet1", cell, "twice"))
		s.Require().NoError(book.SetCellStyle("Sheet1", cell, cell, style))
	}
	cell, err := excelize.CoordinatesToCellName(columns/2+1, 1)
	s.Require().NoError(err)
	s.Require().NoError(book.SetCellValue("Sheet1", cell, "once"))
	var out bytes.Buffer
	s.Require().NoError(book.Write(&out))
	data := s.rezip(out.Bytes(), map[string][]byte{"xl/sharedStrings.xml": []byte(
		`<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><si><t>` + spaces + `</t></si><si><t> </t></si></sst>`)})

	_, err = s.extractor.Extract(context.Background(), "twice.xlsx", data)

	// Stored, the cells are half the budget and a byte; read, the budget and a byte.
	s.requireReason(err, TooLarge)
}

// formatted is a workbook of one row of cells, each the given XML, with the given shared strings,
// and with one number format, of the given code, which a cell of style 1 is written in.
func (s *ExtractSuite) formatted(code string, cells int, cell, shared string) []byte {
	return s.formattedBy(`formatCode="`+s.escaped(code)+`"`, "xl/styles.xml", cells, cell, shared)
}

// formattedBy is formatted with a number format of the given attributes, in a part of the given
// name.
func (s *ExtractSuite) formattedBy(numFmt, part string, cells int, cell, shared string) []byte {
	data := s.withoutPart(s.cellsNaming(cells, cell, "xl/sharedStrings.xml", shared), "xl/styles.xml")
	return s.rezip(data, map[string][]byte{part: []byte(
		`<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:x16r2="http://schemas.microsoft.com/office/spreadsheetml/2015/02/main">` +
			`<numFmts count="1"><numFmt numFmtId="164" ` + numFmt + `/></numFmts>` +
			`<fonts count="1"><font/></fonts><fills count="1"><fill/></fills><borders count="1"><border/></borders><cellStyleXfs count="1"><xf/></cellStyleXfs>` +
			`<cellXfs count="2"><xf numFmtId="0"/><xf numFmtId="164" applyNumberFormat="1"/></cellXfs></styleSheet>`)})
}

func (s *ExtractSuite) escaped(text string) string {
	var out bytes.Buffer
	s.Require().NoError(xml.EscapeText(&out, []byte(text)))
	return out.String()
}

// A number format writes a cell's text once for each text placeholder of its text section, so a
// cell's text may be many times longer read than stored, and a row of such cells is built at once.
// The cells are counted as their formats write them before any row is built.
func (s *ExtractSuite) TestACellIsCountedAsItsNumberFormatWritesIt() {
	text := strings.Repeat("y", 32<<10)
	ats := strings.Repeat("@", 127)
	// A fourth section is the text section, and its zero placeholders write the text as "@" does.
	zeros := "0;0;0;" + strings.Repeat("0 ", 120)
	shared := `<si><t>` + text + `</t></si>`
	sharedCell := `<c t="s" s="1"><v>0</v></c>`
	for name, data := range map[string][]byte{
		"a shared string, written by 127 text placeholders":           s.formatted(ats, 1000, sharedCell, shared),
		"an inline string, written by 127 text placeholders":          s.formatted(ats, 1000, `<c t="inlineStr" s="1"><is><t>`+text+`</t></is></c>`, ""),
		"an inline string of runs, written by 127 text placeholders":  s.formatted(ats, 1000, `<c t="inlineStr" s="1"><is><r><t>`+text+`</t></r></is></c>`, ""),
		"a value that is no number, written by 127 text placeholders": s.formatted(ats, 1000, `<c s="1"><v>`+text+`</v></c>`, ""),
		"a shared string, written by 120 zero placeholders":           s.formatted(zeros, 1000, sharedCell, shared),
		"a format whose code is replaced by its formatCode16":         s.formattedBy(`formatCode="@" x16r2:formatCode16="`+ats+`"`, "xl/styles.xml", 1000, sharedCell, shared),
		"a styles part named with a backslash":                        s.formattedBy(`formatCode="`+ats+`"`, `xl\styles.xml`, 1000, sharedCell, shared),
	} {

		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		_, err := s.extractor.Extract(context.Background(), "formatted.xlsx", data)
		runtime.ReadMemStats(&after)

		s.requireReason(err, TooLarge)
		s.Contains(err.Error(), "shared strings", "%s: refused before a row is built", name)
		// Read, the row is gigabytes, many times the budget.
		s.Less(after.TotalAlloc-before.TotalAlloc, uint64(2*maxWorkbookText), name)
	}
}

// A number format may be as long as Excel allows one to be, and no longer: excelize writes a long
// one's quoted text into each cell in time that grows with its square.
func (s *ExtractSuite) TestANumberFormatMayBeAsLongAsExcelAllows() {
	// A character of two bytes, since the limit is of characters.
	literal := func(length int) string { return `"` + strings.Repeat("é", length-3) + `"0` }
	number := `<c s="1"><v>1</v></c>`

	read, err := s.extractor.Extract(context.Background(), "format.xlsx", s.formatted(literal(maxFormatCode), 2, number, ""))
	s.Require().NoError(err)
	s.Contains(read.Pages[0].Text, strings.Repeat("é", maxFormatCode-3)+"1")

	_, err = s.extractor.Extract(context.Background(), "format.xlsx", s.formatted(literal(maxFormatCode+1), 2, number, ""))
	s.requireReason(err, TooLarge)
	s.Contains(err.Error(), "number format")

	// Read, a format of a megabyte of quoted text takes a minute for each cell.
	started := time.Now()
	_, err = s.extractor.Extract(context.Background(), "format.xlsx", s.formatted(literal(1<<20), 2, number, ""))
	s.requireReason(err, TooLarge)
	s.Less(time.Since(started), 5*time.Second)
}

// A workbook in the formats people use reads as it did before formats were counted: text, a code
// padded with zeros, a weight with its unit, a price, a date, and text in a number's format.
func (s *ExtractSuite) TestAWorkbookOfOrdinaryFormatsReadsAsBefore() {
	book := excelize.NewFile()
	defer book.Close()
	styles := map[string]int{}
	for _, code := range []string{"@", "0000", `0 "kg"`, "0.00", "yyyy-mm-dd"} {
		custom := code
		style, err := book.NewStyle(&excelize.Style{CustomNumFmt: &custom})
		s.Require().NoError(err)
		styles[code] = style
	}
	header := []any{"Item", "Code", "Weight", "Price", "Date", "Note"}
	s.Require().NoError(book.SetSheetRow("Sheet1", "A1", &header))
	var want strings.Builder
	want.WriteString("## Sheet1\n\n| Item | Code | Weight | Price | Date | Note |\n| --- | --- | --- | --- | --- | --- |")
	for row := 2; row <= 40; row++ {
		date := time.Date(2026, 1, row, 0, 0, 0, 0, time.UTC)
		values := []any{"item " + strconv.Itoa(row), row, row * 3, float64(row) + 0.5, date, "note " + strconv.Itoa(row)}
		cell, err := excelize.CoordinatesToCellName(1, row)
		s.Require().NoError(err)
		s.Require().NoError(book.SetSheetRow("Sheet1", cell, &values))
		for column, code := range []string{"@", "0000", `0 "kg"`, "0.00", "yyyy-mm-dd", `0 "kg"`} {
			cell, err := excelize.CoordinatesToCellName(column+1, row)
			s.Require().NoError(err)
			s.Require().NoError(book.SetCellStyle("Sheet1", cell, cell, styles[code]))
		}
		fmt.Fprintf(&want, "\n| item %d | %04d | %d kg | %d.50 | %s | note %d |", row, row, row*3, row, date.Format("2006-01-02"), row)
	}
	var out bytes.Buffer
	s.Require().NoError(book.Write(&out))

	read, err := s.extractor.Extract(context.Background(), "ordinary.xlsx", out.Bytes())

	s.Require().NoError(err)
	s.Equal(want.String(), read.Pages[0].Text)
}

// A table may be as long as its limit and no longer, its last row counted as well as the others.
func (s *ExtractSuite) TestATableMayBeAsLongAsItsLimit() {
	rows := [][]string{{"h"}, {"a"}, {"b"}}
	whole := table(rows)

	text, err := tableWithin(context.Background(), rows, len(whole))
	s.Require().NoError(err)
	s.Equal(whole, text)

	_, err = tableWithin(context.Background(), rows, len(whole)-1)
	s.Require().ErrorIs(err, errTableTooLong)
}

// The text budget is of the whole workbook: two sheets that each take less than half of it are
// read, and a third is refused.
func (s *ExtractSuite) TestTheTextBudgetIsOfTheWholeWorkbook() {
	long := strings.Repeat("y", 100<<10)

	read, err := s.extractor.Extract(context.Background(), "shared.xlsx", s.sharedText(2, 3, 100, long))
	s.Require().NoError(err)
	s.Contains(read.Pages[0].Text, "## S2")
	s.Contains(read.Pages[0].Text, long)

	_, err = s.extractor.Extract(context.Background(), "shared.xlsx", s.sharedText(3, 3, 100, long))
	s.requireReason(err, TooLarge)
}

// A table pads every row to its widest, so one row of values in every column over rows with no
// cells is a long table from few cells. It is refused when it is longer than the text budget.
func (s *ExtractSuite) TestATableLongerThanTheTextBudgetIsRefused() {
	var header strings.Builder
	header.WriteString(`<row r="1">`)
	for column := 1; column <= maxSheetColumns; column++ {
		cell, err := excelize.CoordinatesToCellName(column, 1)
		s.Require().NoError(err)
		header.WriteString(`<c r="` + cell + `" t="inlineStr"><is><t>h</t></is></c>`)
	}
	header.WriteString(`</row>`)
	last := `<row r="100000"><c r="A100000" t="inlineStr"><is><t>x</t></is></c></row>`

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := s.extractor.Extract(context.Background(), "padded.xlsx", s.sheet(header.String()+last))
	runtime.ReadMemStats(&after)

	s.requireReason(err, TooLarge)
	// Writing a table allocates several times its length, so the budget is a gigabyte, a few
	// times the text budget. Writing the whole table allocates several gigabytes.
	s.Less(after.TotalAlloc-before.TotalAlloc, uint64(1<<30))
}

// The deadline is checked between the rows of a table as it is written, not only as it is read.
func (s *ExtractSuite) TestATableIsWrittenOnlyUntilTheDeadline() {
	const rows = 500
	data := s.workbook(rows, 1)

	// Err is asked as the cells are counted, then once for the sheet and once for each row read:
	// the deadline passes a few rows into writing the table.
	_, err := readXLSX(&stopsAfter{Context: context.Background(), left: s.countingCalls(data) + 1 + rows + 5}, data)

	s.Require().ErrorIs(err, context.DeadlineExceeded)
}

// sheets is a workbook of n sheets, each the given rows of inline strings.
func (s *ExtractSuite) sheets(n int, sheetData string) []byte {
	var out bytes.Buffer
	writer := zip.NewWriter(&out)
	add := func(name, body string) {
		part, err := writer.Create(name)
		s.Require().NoError(err)
		_, err = part.Write([]byte(body))
		s.Require().NoError(err)
	}
	types := `<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/><Default Extension="xml" ContentType="application/xml"/><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>`
	list := `<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>`
	rels := `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">`
	for i := 1; i <= n; i++ {
		types += fmt.Sprintf(`<Override PartName="/xl/worksheets/sheet%d.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>`, i)
		list += fmt.Sprintf(`<sheet name="S%d" sheetId="%d" r:id="rId%d"/>`, i, i, i)
		rels += fmt.Sprintf(`<Relationship Id="rId%d" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet%d.xml"/>`, i, i)
		add(fmt.Sprintf("xl/worksheets/sheet%d.xml", i), `<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>`+sheetData+`</sheetData></worksheet>`)
	}
	add("[Content_Types].xml", types+`</Types>`)
	add("xl/workbook.xml", list+`</sheets></workbook>`)
	add("xl/_rels/workbook.xml.rels", rels+`</Relationships>`)
	add("_rels/.rels", `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>`)
	s.Require().NoError(writer.Close())
	return out.Bytes()
}

// A table of many rows of an ordinary width reads as it did before the limits. Its last column is
// the tenth, so each row holds ten cells for three values.
func (s *ExtractSuite) TestAWorkbookOfManyRowsOfOrdinaryWidthIsReadInFull() {
	book := excelize.NewFile()
	defer book.Close()
	var want strings.Builder
	want.WriteString("## Sheet1\n\n| A | B | C |\n| --- | --- | --- |")
	for row := 1; row <= 20000; row++ {
		number := strconv.Itoa(row)
		values := []any{"A", "B", nil, nil, nil, nil, nil, nil, nil, "C"}
		if row > 1 {
			values = []any{"a" + number, row, nil, nil, nil, nil, nil, nil, nil, "c" + number}
			want.WriteString("\n| a" + number + " | " + number + " | c" + number + " |")
		}
		cell, err := excelize.CoordinatesToCellName(1, row)
		s.Require().NoError(err)
		s.Require().NoError(book.SetSheetRow("Sheet1", cell, &values))
	}
	var out bytes.Buffer
	s.Require().NoError(book.Write(&out))

	read, err := s.extractor.Extract(context.Background(), "table.xlsx", out.Bytes())

	s.Require().NoError(err)
	s.Equal(want.String(), read.Pages[0].Text)
}

// stopsAfter is a context that is done once Err has been asked this many times.
type stopsAfter struct {
	context.Context
	left int
}

func (c *stopsAfter) Err() error {
	if c.left--; c.left < 0 {
		return context.DeadlineExceeded
	}
	return nil
}

// The deadline is checked between the rows of a sheet, not only between sheets.
func (s *ExtractSuite) TestASheetIsReadOnlyUntilTheDeadline() {
	data := s.workbook(500, 1)
	_, err := readXLSX(context.Background(), data)
	s.Require().NoError(err)

	_, err = readXLSX(&stopsAfter{Context: context.Background(), left: s.countingCalls(data) + 10}, data)

	s.Require().ErrorIs(err, context.DeadlineExceeded)
	_, ok := ReasonOf(err)
	s.False(ok, "a workbook stopped at the deadline is not called corrupt or too large")
}

// countingCalls is how many times counting a workbook's cells asks for the deadline.
func (s *ExtractSuite) countingCalls(data []byte) int {
	counted := &stopsAfter{Context: context.Background(), left: math.MaxInt}
	count := cellCount{ctx: counted}
	s.Require().NoError(count.workbook(data))
	return math.MaxInt - counted.left
}

// The deadline is checked as the cells are counted: at each row of every part, and at each shared
// string. A workbook with a part that is no sheet, or with more shared strings than its cells
// name, is counted for longer than it is read.
func (s *ExtractSuite) TestTheCellsAreCountedOnlyUntilTheDeadline() {
	var rows, shared strings.Builder
	for i := 1; i <= 500; i++ {
		rows.WriteString(`<row r="` + strconv.Itoa(i) + `"><c r="A` + strconv.Itoa(i) + `"><v>1</v></c></row>`)
		shared.WriteString(`<si><t>s</t></si>`)
	}
	for name, data := range map[string][]byte{
		"rows of a part that is no sheet": s.rezip(s.workbook(1, 1), map[string][]byte{"xl/rows.xml": []byte(`<sheetData>` + rows.String() + `</sheetData>`)}),
		"shared strings":                  s.rezip(s.workbook(1, 1), map[string][]byte{"xl/sharedStrings.xml": []byte(`<sst>` + shared.String() + `</sst>`)}),
	} {
		_, err := readXLSX(context.Background(), data)
		s.Require().NoError(err, name)

		_, err = readXLSX(&stopsAfter{Context: context.Background(), left: 10}, data)

		s.Require().ErrorIs(err, context.DeadlineExceeded, name)
	}
}

// sheet is a workbook whose only sheet is the given XML, rows of inline strings.
func (s *ExtractSuite) sheet(sheetData string) []byte {
	return s.rezip(s.workbook(1, 1), map[string][]byte{"xl/worksheets/sheet1.xml": []byte(
		`<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>` + sheetData + `</sheetData></worksheet>`)})
}

func (s *ExtractSuite) TestASheetWhoseRowsCannotBeReadIsRefusedAsCorrupt() {
	row := func(number, cell string) string {
		return `<row r="` + number + `"><c r="` + cell + `" t="inlineStr"><is><t>` + cell + `</t></is></c></row>`
	}
	for name, test := range map[string]struct {
		data   []byte
		reason Reason
		// says is what the error says, when the rows are what cannot be read.
		says string
	}{
		"a row past the last row a sheet has": {data: s.sheet(row("1", "A1") + row("1048577", "A1048577")), reason: Corrupt, says: `sheet "Sheet1" cannot be read`},
		"a sheet whose part is missing":       {data: s.withoutPart(s.workbook(1, 1), "xl/worksheets/sheet1.xml"), reason: Corrupt},
		"a cell named by no column":           {data: s.sheet(row("1", "A1") + row("2", "ZZZZ2")), reason: Corrupt},
	} {
		_, err := s.extractor.Extract(context.Background(), name+".xlsx", test.data)

		s.requireReason(err, test.reason)
		if test.says != "" {
			s.Contains(err.Error(), test.says, name)
			s.NotContains(err.Error(), "pictures", name)
		}
	}
}

// A row that cannot be read ends its sheet there, as in excelize's GetRows: a workbook whose
// shared strings are cut short reads as a sheet with no text, not as a corrupt file.
func (s *ExtractSuite) TestAWorkbookWhoseSharedStringsAreCutShortReadsWithNoText() {
	data := s.rezip(s.workbook(3, 1), map[string][]byte{"xl/sharedStrings.xml": []byte(`<sst><si><t>cut`)})

	read, err := s.extractor.Extract(context.Background(), "cut.xlsx", data)

	s.Require().NoError(err)
	s.Empty(read.Pages[0].Text)
}

// withoutPart is a copy of an archive without one of its parts.
func (s *ExtractSuite) withoutPart(data []byte, name string) []byte {
	archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	s.Require().NoError(err)
	var out bytes.Buffer
	writer := zip.NewWriter(&out)
	for _, file := range archive.File {
		if file.Name != name {
			s.Require().NoError(writer.Copy(file))
		}
	}
	s.Require().NoError(writer.Close())
	return out.Bytes()
}
