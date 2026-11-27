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
	"io"
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

// The deadline has passed before the read starts, so the answer cannot depend on how fast the
// read is: it fails, and it does not even start the engine. A timeout of a nanosecond is not
// that: its timer may not have fired by the time the read ends.
func (s *ExtractSuite) TestReadingStopsAtTheDeadline() {
	extractor := New(Options{})
	defer extractor.Close()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	_, err := extractor.Extract(ctx, "manual.pdf", s.fixture("manual.pdf"))

	s.Require().ErrorIs(err, context.DeadlineExceeded)
	_, ok := ReasonOf(err)
	s.False(ok, "a file stopped at the deadline is not called corrupt")
	s.Nil(extractor.pdf, "a read past its deadline starts no engine")
}

// Drawing a page is a read of its own: a deadline that has passed fails it before it takes a
// worker. The extractor is closed, so a draw that did take the engine would fail with that
// instead.
func (s *ExtractSuite) TestDrawingAPageStopsAtTheDeadline() {
	extractor := New(Options{})
	document, err := extractor.Extract(context.Background(), "manual.pdf", s.fixture("manual.pdf"))
	s.Require().NoError(err)
	s.Require().NoError(extractor.Close())
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	_, err = document.Render(ctx, 1)

	s.Require().ErrorIs(err, context.DeadlineExceeded)
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
	extractor := New(Options{})
	defer extractor.Close()
	// The deadline has passed already: a timeout of a nanosecond is done only once its timer runs.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	for _, name := range []string{"handbook.docx", "handbook.odt", "handbook.rtf", "policies.doc", "menu.xlsx"} {
		_, err := extractor.Extract(ctx, name, s.fixture(name))

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

// workbook is a workbook whose rows each hold one value, in the given column. Its sheet and shared
// strings are written as excelize saves them, not set cell by cell: excelize holds a row it sets as
// wide as its last value, so a workbook of rows that reach a far column takes gigabytes to build.
func (s *ExtractSuite) workbook(rows, column int) []byte {
	book := excelize.NewFile()
	defer book.Close()
	s.Require().NoError(book.SetCellValue("Sheet1", "A1", "v1"))
	var out bytes.Buffer
	s.Require().NoError(book.Write(&out))

	name, err := excelize.ColumnNumberToName(column)
	s.Require().NoError(err)
	var sheetData, shared strings.Builder
	for row := 1; row <= rows; row++ {
		number := strconv.Itoa(row)
		sheetData.WriteString(`<row r="` + number + `"><c r="` + name + number + `" t="s"><v>` + strconv.Itoa(row-1) + `</v></c></row>`)
		shared.WriteString(`<si><t>v` + number + `</t></si>`)
	}
	return s.rezip(out.Bytes(), map[string][]byte{
		"xl/worksheets/sheet1.xml": []byte(`<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>` + sheetData.String() + `</sheetData></worksheet>`),
		"xl/sharedStrings.xml":     []byte(`<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">` + shared.String() + `</sst>`),
	})
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
		// Streamed, so each row is written as its one value, not held as wide as its last column.
		for sheet, rows := range map[string]int{"Sheet1": half, "Second": test.second} {
			writer, err := book.NewStreamWriter(sheet)
			s.Require().NoError(err)
			for row := 1; row <= rows; row++ {
				cell, err := excelize.CoordinatesToCellName(column, row)
				s.Require().NoError(err)
				s.Require().NoError(writer.SetRow(cell, []any{"v"}))
			}
			s.Require().NoError(writer.Flush())
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
	const length = 2 << 20
	long := `<si><t>` + strings.Repeat("y", length) + `</t></si>`
	short := `<si><t>short</t></si>`
	first := `<c t="s"><v>0</v></c>`
	// excelize holds a shared strings part of up to 16 MiB in memory, so a longer string makes
	// it read the part as it reads one too large to hold.
	const nested = 17 << 20
	for name, test := range map[string]struct {
		data []byte
		// row is the text of the row as it is built: the long string once in every cell.
		row int
		// budget overrides row/2 where the legit allocation is far under half the row.
		budget uint64
	}{
		"a part small enough to hold, the first string the root's child": {
			data: s.cellsNaming(128, first, "xl/sharedStrings.xml", `<x>`+short+`</x>`+long),
			row:  128 * length,
		},
		"a part too large to hold, the first string nested": {
			data: s.cellsNaming(128, first, "xl/sharedStrings.xml", `<x><si><t>`+strings.Repeat("y", nested)+`</t></si></x>`+short),
			row:  128 * nested,
		},
		"a cell whose formula holds another index": {
			data: s.cellsNaming(128, `<c t="s"><v>0</v><f><v>1</v></f></c>`, "xl/sharedStrings.xml", long+short),
			row:  128 * length,
		},
		"an index between spaces": {
			data: s.cellsNaming(128, `<c t="s"><v> 1 </v></c>`, "xl/sharedStrings.xml", short+long),
			row:  128 * length,
		},
		"a part named in other letters": {
			data: s.cellsNaming(128, first, "XL/SharedStrings.xml", long),
			row:  128 * length,
		},
		"a part named with a backslash": {
			data: s.cellsNaming(128, first, `xl\sharedStrings.xml`, long),
			row:  128 * length,
		},
		"a string of runs": {
			data: s.cellsNaming(maxSheetColumns, first, "xl/sharedStrings.xml", `<si><r><t>`+strings.Repeat("y", 1<<20)+`</t></r></si>`),
			row:  maxSheetColumns << 20,
			// Legit allocation is about 23 MB under -race, so 128 MiB is about 5.8 times it.
			budget: maxWorkbookText,
		},
		"a cell of two values, the last naming the long string": {
			data: s.cellsNaming(128, `<c t="s"><v>1</v><v>0</v></c>`, "xl/sharedStrings.xml", long+short),
			row:  128 * length,
		},
	} {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		_, err := s.extractor.Extract(context.Background(), "named.xlsx", test.data)
		runtime.ReadMemStats(&after)

		s.requireReason(err, TooLarge)
		s.Contains(err.Error(), "shared strings", "%s: refused before a row is built", name)
		// Counting the long string allocates a few times its length for each of the two ways it
		// is read, more under the race detector: at most a quarter of the row here. Building the
		// row allocates more than the row. Half the row is between the two.
		budget := uint64(test.row / 2)
		if test.budget != 0 {
			budget = test.budget
		}
		s.Less(after.TotalAlloc-before.TotalAlloc, budget, name)
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
	// Stored, the cells are 8 MiB of text, within the budget; read, a gigabyte, eight times it.
	const cells = 256
	ats := strings.Repeat("@", 127)
	// A fourth section is the text section, and its zero placeholders write the text as "@" does.
	zeros := "0;0;0;" + strings.Repeat("0 ", 120)
	shared := `<si><t>` + text + `</t></si>`
	sharedCell := `<c t="s" s="1"><v>0</v></c>`
	for name, data := range map[string][]byte{
		"a shared string, written by 127 text placeholders":           s.formatted(ats, cells, sharedCell, shared),
		"an inline string, written by 127 text placeholders":          s.formatted(ats, cells, `<c t="inlineStr" s="1"><is><t>`+text+`</t></is></c>`, ""),
		"an inline string of runs, written by 127 text placeholders":  s.formatted(ats, cells, `<c t="inlineStr" s="1"><is><r><t>`+text+`</t></r></is></c>`, ""),
		"a value that is no number, written by 127 text placeholders": s.formatted(ats, cells, `<c s="1"><v>`+text+`</v></c>`, ""),
		"a shared string, written by 120 zero placeholders":           s.formatted(zeros, cells, sharedCell, shared),
		"a format whose code is replaced by its formatCode16":         s.formattedBy(`formatCode="@" x16r2:formatCode16="`+ats+`"`, "xl/styles.xml", cells, sharedCell, shared),
		"a styles part named with a backslash":                        s.formattedBy(`formatCode="`+ats+`"`, `xl\styles.xml`, cells, sharedCell, shared),
	} {

		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		_, err := s.extractor.Extract(context.Background(), "formatted.xlsx", data)
		runtime.ReadMemStats(&after)

		s.requireReason(err, TooLarge)
		s.Contains(err.Error(), "shared strings", "%s: refused before a row is built", name)
		// Building the row allocates at least its gigabyte, four times this budget.
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
	// Each line of the table is its 1,024 cells of three bytes, a pipe and a line break. The
	// table is four times the text budget.
	rows := 4 * maxWorkbookText / (3*maxSheetColumns + 2)
	last := `<row r="` + strconv.Itoa(rows) + `"><c r="A` + strconv.Itoa(rows) + `" t="inlineStr"><is><t>x</t></is></c></row>`

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := s.extractor.Extract(context.Background(), "padded.xlsx", s.sheet(header.String()+last))
	runtime.ReadMemStats(&after)

	s.requireReason(err, TooLarge)
	// Writing a table allocates five to six times its length: refused at the text budget, about
	// six times the budget; written whole, over twenty times it. Twelve times is about twice the
	// first and half the second.
	s.Less(after.TotalAlloc-before.TotalAlloc, uint64(12*maxWorkbookText))
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
		// The count itself stops there, not only the read of the sheets after it.
		count := cellCount{ctx: &stopsAfter{Context: context.Background(), left: 10}}
		s.ErrorIs(count.workbook(data), context.DeadlineExceeded, name)
	}
}

// The deadline is checked every few thousand XML tokens as the cells are counted, not only at a
// row or a string: a part of many elements that are none of them is not read to its end past it.
func (s *ExtractSuite) TestAPartOfOtherElementsIsCountedOnlyUntilTheDeadline() {
	junk := strings.Repeat(`<a/>`, 100_000)
	for name, data := range map[string][]byte{
		"a part that is no sheet": s.rezip(s.workbook(1, 1), map[string][]byte{"xl/junk.xml": []byte(`<r>` + junk + `</r>`)}),
		// Inside a formula, which the count of a part's cells skips whole, so the elements are
		// read one by one only as the shared strings are read.
		"the shared strings": s.rezip(s.workbook(1, 1), map[string][]byte{"xl/sharedStrings.xml": []byte(`<sst><si><t>s</t></si><c><f>` + junk + `</f></c></sst>`)}),
		"a cell":             s.sheet(`<row r="1"><c r="A1" t="s"><v>0</v>` + junk + `</c></row>`),
		"the styles":         s.rezip(s.workbook(1, 1), map[string][]byte{"xl/styles.xml": []byte(`<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">` + junk + `</styleSheet>`)}),
		// Inside a value and an inline string, which the count decodes whole.
		"a value":          s.rezip(s.workbook(1, 1), map[string][]byte{"xl/junk.xml": []byte(`<r><c><v>` + junk + `</v></c></r>`)}),
		"an inline string": s.rezip(s.workbook(1, 1), map[string][]byte{"xl/junk.xml": []byte(`<r><c><is>` + junk + `</is></c></r>`)}),
	} {
		_, err := readXLSX(context.Background(), data)
		s.Require().NoError(err, name)

		_, err = readXLSX(&stopsAfter{Context: context.Background(), left: 10}, data)

		s.Require().ErrorIs(err, context.DeadlineExceeded, name)
		// The count stops at the first check past the deadline, not at the end of the element it
		// is in.
		count := cellCount{ctx: &stopsAfter{Context: context.Background()}}
		s.Require().ErrorIs(count.workbook(data), context.DeadlineExceeded, name)
		s.LessOrEqual(count.tokens, tokensPerDeadlineCheck, name)
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

// A string of many runs with no text in them is a small file that excelize decodes into a struct
// for each run. The read is refused when a string has more runs than a cell can show, before any of
// them is decoded.
func (s *ExtractSuite) TestAStringOfMoreRunsThanACellShowsIsRefusedBeforeItIsDecoded() {
	const runs = 4 << 20
	for name, part := range map[string]string{
		"a shared string":           `<si>` + strings.Repeat(`<r></r>`, runs) + `</si>`,
		"an inline string":          `<is>` + strings.Repeat(`<r></r>`, runs) + `</is>`,
		"phonetic runs of a string": `<si><t>x</t>` + strings.Repeat(`<rPh/>`, runs) + `</si>`,
	} {
		data := s.cellsNaming(1, `<c t="s"><v>0</v></c>`, "xl/sharedStrings.xml", part)
		if strings.HasPrefix(part, "<is>") {
			data = s.sheet(`<row r="1"><c t="inlineStr">` + part + `</c></row>`)
		}
		s.Less(len(data), 128<<10, "%s: the file is small", name)

		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		_, err := s.extractor.Extract(context.Background(), "runs.xlsx", data)
		runtime.ReadMemStats(&after)

		s.requireReason(err, TooLarge)
		s.Contains(err.Error(), "runs", name)
		// The refused read allocates a few hundred bytes for each run it reads up to the limit, a
		// few MiB, and decoding every run allocates hundreds of MiB. The length of the string's part
		// is several times the first and a small part of the second.
		s.Less(after.TotalAlloc-before.TotalAlloc, uint64(len(part)), name)
	}
}

// A string may hold as many runs as a cell shows characters, and no more, and as many phonetic runs.
// Only the runs that are the string's own children are counted, as excelize reads them, and each
// string's runs are counted on their own.
func (s *ExtractSuite) TestAStringMayHoldAsManyRunsAsACellShows() {
	runs := func(n int) string { return strings.Repeat(`<r></r>`, n) }
	for name, test := range map[string]struct {
		sheet  string
		shared string
		// refused is whether the workbook is refused as too large; one that is not reads as x.
		refused bool
	}{
		"a shared string at the limit":          {shared: `<si>` + runs(maxStringRuns-1) + `<r><t>x</t></r></si>`},
		"a shared string past the limit":        {shared: `<si>` + runs(maxStringRuns) + `<r><t>x</t></r></si>`, refused: true},
		"an inline string at the limit":         {sheet: `<is>` + runs(maxStringRuns-1) + `<r><t>x</t></r></is>`},
		"an inline string past the limit":       {sheet: `<is>` + runs(maxStringRuns) + `<r><t>x</t></r></is>`, refused: true},
		"phonetic runs at the limit":            {shared: `<si><t>x</t>` + strings.Repeat(`<rPh/>`, maxStringRuns) + `</si>`},
		"phonetic runs past the limit":          {shared: `<si><t>x</t>` + strings.Repeat(`<rPh/>`, maxStringRuns+1) + `</si>`, refused: true},
		"runs and phonetic runs at the limit":   {shared: `<si>` + runs(maxStringRuns-1) + `<r><t>x</t></r>` + strings.Repeat(`<rPh/>`, maxStringRuns) + `</si>`},
		"runs on both sides of a string inside": {shared: `<si>` + runs(maxStringRuns/2+1) + `<si/>` + runs(maxStringRuns/2+1) + `<r><t>x</t></r></si>`, refused: true},
		"runs inside another element":           {shared: `<si><x>` + runs(maxStringRuns+1) + `</x><t>x</t></si>`},
		"two strings, each under the limit":     {shared: `<si>` + runs(maxStringRuns/2+1) + `</si><si>` + runs(maxStringRuns/2+1) + `<r><t>x</t></r></si>`},
		"two strings of phonetic runs":          {shared: `<si><t>a</t>` + strings.Repeat(`<rPh/>`, maxStringRuns/2+1) + `</si><si><t>x</t>` + strings.Repeat(`<rPh/>`, maxStringRuns/2+1) + `</si>`},
	} {
		data := s.sheet(`<row r="1"><c r="A1" t="inlineStr">` + test.sheet + `</c></row>`)
		if test.shared != "" {
			// The cell names the last string, the one with the x.
			last := strconv.Itoa(strings.Count(test.shared, "<si>") - 1)
			data = s.cellsNaming(1, `<c t="s"><v>`+last+`</v></c>`, "xl/sharedStrings.xml", test.shared)
		}

		read, err := s.extractor.Extract(context.Background(), "runs.xlsx", data)

		if test.refused {
			s.requireReason(err, TooLarge)
			s.Contains(err.Error(), "runs", name)
			continue
		}
		s.Require().NoError(err, name)
		s.Contains(read.Pages[0].Text, "| x", name)
	}
}

// deep is an element nested the given number of levels deep, under its parent.
func (s *ExtractSuite) deep(levels int) string {
	return strings.Repeat(`<x>`, levels) + strings.Repeat(`</x>`, levels)
}

// An element nested far deeper than the limit is a small file, and encoding/xml keeps every level
// that is still open as it reads it. The workbook is refused before any part of it is decoded,
// whichever part holds the element: the ones excelize decodes as it opens the workbook, the ones
// it decodes as its sheets are read, and the ones it never reads.
func (s *ExtractSuite) TestAPartNestedPastTheLimitIsRefusedBeforeItIsDecoded() {
	const levels = 1 << 18
	nested := s.deep(levels)
	ns := `xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"`
	for name, part := range map[string]struct{ name, xml string }{
		"the workbook":          {"xl/workbook.xml", `<workbook ` + ns + ` xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Sheet1" sheetId="1" r:id="rId1"/></sheets>` + nested + `</workbook>`},
		"the styles":            {"xl/styles.xml", `<styleSheet ` + ns + `>` + nested + `</styleSheet>`},
		"the theme":             {"xl/theme/theme1.xml", `<a:theme xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main">` + nested + `</a:theme>`},
		"the calculation chain": {"xl/calcChain.xml", `<calcChain ` + ns + `>` + nested + `</calcChain>`},
		"the relationships":     {"xl/_rels/workbook.xml.rels", `<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>` + nested + `</Relationships>`},
		"the sheet":             {"xl/worksheets/sheet1.xml", `<worksheet ` + ns + `><sheetData><row r="1"><c r="A1"><v>1</v></c></row></sheetData>` + nested + `</worksheet>`},
		"the shared strings":    {"xl/sharedStrings.xml", `<sst ` + ns + `><si><t>v1</t></si>` + nested + `</sst>`},
		"a part never read":     {"docProps/custom.xml", nested},
		// excelize decodes a part in the encoding it declares, so the count does as well.
		"the styles in another encoding": {"xl/styles.xml", `<?xml version="1.0" encoding="windows-1252"?><styleSheet ` + ns + `>` + nested + `</styleSheet>`},
		"the sheet in another encoding":  {"xl/worksheets/sheet1.xml", `<?xml version="1.0" encoding="windows-1252"?><worksheet ` + ns + `><sheetData><row r="1"><c r="A1"><v>1</v></c></row></sheetData>` + nested + `</worksheet>`},
	} {
		data := s.rezip(s.workbook(1, 1), map[string][]byte{part.name: []byte(part.xml)})
		s.Less(len(data), 128<<10, "%s: the file is small", name)

		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		_, err := s.extractor.Extract(context.Background(), "nested.xlsx", data)
		runtime.ReadMemStats(&after)

		s.requireReason(err, TooLarge)
		s.Contains(err.Error(), "nested", name)
		// The refused read allocates for the levels it reads up to the limit, a few KiB, and
		// reading every level allocates more than a hundred bytes for each. The length of the part
		// is far between the two.
		s.Less(after.TotalAlloc-before.TotalAlloc, uint64(len(part.xml)), name)
	}
}

// An element may be nested as deep as the limit, counted from the part's root, and no deeper.
func (s *ExtractSuite) TestAnElementMayBeNestedAsDeepAsTheLimit() {
	for _, test := range []struct {
		depth   int
		refused bool
	}{
		{maxXMLDepth, false},
		{maxXMLDepth + 1, true},
	} {
		// The worksheet is the first level.
		data := s.rezip(s.workbook(1, 1), map[string][]byte{"xl/worksheets/sheet1.xml": []byte(
			`<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>x</t></is></c></row></sheetData>` +
				s.deep(test.depth-1) + `</worksheet>`)})

		read, err := s.extractor.Extract(context.Background(), "nested.xlsx", data)

		if test.refused {
			s.requireReason(err, TooLarge)
			continue
		}
		s.Require().NoError(err, "depth %d", test.depth)
		s.Contains(read.Pages[0].Text, "| x |")
	}
}

// withSheet is a workbook whose only sheet is one cell of 1, then the given XML: elements after the
// sheet's cells, which the sheet's reader skips.
func (s *ExtractSuite) withSheet(prolog, rootAttributes, after string) []byte {
	return s.rezip(s.workbook(1, 1), map[string][]byte{"xl/worksheets/sheet1.xml": []byte(prolog +
		`<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"` + rootAttributes +
		`><sheetData><row r="1"><c r="A1"><v>1</v></c></row></sheetData>` + after + `</worksheet>`)})
}

// A tag of many attributes is a small file, and encoding/xml builds a value for each attribute of a
// tag as it reads the tag, before anything can count them. The workbook is refused before such a
// tag is read whole, whichever element or part it is, and in a part of another encoding, whose bytes
// may read as a quote and a > where its text has neither, as well.
func (s *ExtractSuite) TestATagOfMoreAttributesThanTheLimitIsRefusedBeforeItIsRead() {
	attributes := strings.Repeat(` a="1"`, 100_000)
	// One character of ISO-2022-JP, whose two bytes are a quote and a >.
	const quoteAndBracket = "\x1b$B\">\x1b(B"
	for name, data := range map[string][]byte{
		"an element of the sheet":    s.withSheet("", "", `<x`+attributes+`/>`),
		"the root of the sheet":      s.withSheet("", attributes, ""),
		"namespace declarations":     s.withSheet("", "", `<x`+strings.Repeat(` xmlns:p="u"`, 100_000)+`/>`),
		"the styles":                 s.rezip(s.workbook(1, 1), map[string][]byte{"xl/styles.xml": []byte(`<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"` + attributes + `/>`)}),
		"a part in another encoding": s.withSheet(`<?xml version="1.0" encoding="iso-2022-jp"?>`, "", `<x b="`+quoteAndBracket+`"`+attributes+`/>`),
	} {
		s.Less(len(data), 128<<10, "%s: the file is small", name)

		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		_, err := s.extractor.Extract(context.Background(), "attributes.xlsx", data)
		runtime.ReadMemStats(&after)

		s.requireReason(err, TooLarge)
		s.Contains(err.Error(), "attributes", name)
		// The refused read allocates for the attributes up to the limit, a few KiB, and reading
		// every one allocates many times its length. The length of its attributes is far between.
		s.Less(after.TotalAlloc-before.TotalAlloc, uint64(len(attributes)), name)
	}
}

// A tag may be as long as the limit, and an element may have as many attributes as the limit,
// counting its namespace declarations, and no more. A quote or a = inside a value is no attribute,
// a > inside one is no end of the tag, and neither is one inside a comment, which a quote may be in.
// A part that is not XML is no tag either.
func (s *ExtractSuite) TestATagMayBeAsLongAndHaveAsManyAttributesAsTheLimit() {
	// long is a tag of n bytes, counted from after its <.
	long := func(n int) string { return `<x a="` + strings.Repeat("y", n-len(`x a=""/>`)) + `"/>` }
	attributes := func(n int, value string) string {
		var tag strings.Builder
		tag.WriteString(`<x xmlns:p="u"`)
		for i := 1; i < n; i++ {
			tag.WriteString(` a` + strconv.Itoa(i) + `="` + value + `"`)
		}
		return tag.String() + `/>`
	}
	for name, test := range map[string]struct {
		after string
		// refused is what the error of a refused workbook says; one that is not reads as 1.
		refused string
	}{
		"a tag as long as the limit":  {after: long(maxTagBytes)},
		"a tag longer than the limit": {after: long(maxTagBytes + 1), refused: "bytes"},
		// The limit is past in the middle of a character of two bytes.
		"a tag longer than the limit inside a character": {after: `<x a="` + strings.Repeat("é", maxTagBytes/2) + `"/>`, refused: "bytes"},
		"as many attributes as the limit":                {after: attributes(maxAttributes, "1")},
		"more attributes than the limit":                 {after: attributes(maxAttributes+1, "1"), refused: "attributes"},
		"values that hold a quote and an =":              {after: attributes(maxAttributes, `'=`)},
		"a > in a value, which ends no tag":              {after: attributes(maxAttributes+1, `>`), refused: "attributes"},
		"a comment with a quote and an = in it":          {after: `<!-- it's ` + strings.Repeat(`a=b `, maxAttributes+1) + `-->` + long(maxTagBytes)},
		"a processing instruction with a quote in it":    {after: `<?x it's ` + strings.Repeat(`a=b `, maxAttributes+1) + `?>` + long(maxTagBytes)},
		// A > ends none of these, and the < and quote after it open no tag or value.
		"a comment holding > < \"":                                {after: `<!-- > < " -->` + attributes(maxAttributes+1, `>`), refused: "attributes"},
		"a processing instruction holding > < \"":                 {after: `<?p > < " ?>` + attributes(maxAttributes+1, `>`), refused: "attributes"},
		"a CDATA section holding > < \"":                          {after: `<y><![CDATA[> < "]]></y>` + attributes(maxAttributes+1, `>`), refused: "attributes"},
		"a declaration holding > < \"":                            {after: `<!DOCTYPE x [<!ENTITY e "> < '"><!-- > < " -->]>` + attributes(maxAttributes+1, `>`), refused: "attributes"},
		"a comment holding > < \" before a tag at the limits":     {after: `<!-- > < " -->` + attributes(maxAttributes, `>`) + long(maxTagBytes)},
		"a declaration holding > < \" before a tag at the limits": {after: `<!DOCTYPE x [<!ENTITY e "> < '"><!-- > < " -->]>` + attributes(maxAttributes, `>`) + long(maxTagBytes)},
	} {
		data := s.withSheet("", "", test.after)

		read, err := s.extractor.Extract(context.Background(), "tags.xlsx", data)

		if test.refused != "" {
			s.requireReason(err, TooLarge)
			s.Contains(err.Error(), test.refused, name)
			continue
		}
		s.Require().NoError(err, name)
		s.Contains(read.Pages[0].Text, "| 1 |", name)
	}

	// A part that is not XML is read only to its first error, and the bytes after it are no tag.
	picture := append([]byte("\x89PNG\r\n\x1a\n<"), bytes.Repeat([]byte("="), maxAttributes+1)...)
	_, err := s.extractor.Extract(context.Background(), "picture.xlsx", s.rezip(s.workbook(1, 1), map[string][]byte{"xl/media/image1.png": picture}))
	s.Require().NoError(err)
}

// The guard reads as a tag the bytes encoding/xml reads as one, and no others: each byte it reads
// as part of a tag is part of a token the decoder reads from a <, and each other byte part of the
// decoder's text.
func (s *ExtractSuite) TestTheGuardEndsEachTagWhereTheDecoderDoes() {
	for name, part := range map[string]string{
		"elements":                                     `<a b=">" c='"' d="'"><e/>text > " '</a >`,
		"a declaration of the XML":                     `<?xml version="1.0" encoding="UTF-8"?><a/>`,
		"a comment":                                    `<a><!-- > < " ' = - -> --></a>`,
		"an empty comment":                             `<a><!----><b c=">"/></a>`,
		"a processing instruction":                     `<a><?p > < " ' = ? >?></a>`,
		"a processing instruction of a ?":              `<a><?p??><b c=">"/></a>`,
		"a CDATA section":                              `<a><![CDATA[> < " ' = ] ]] ]>]]></a>`,
		"two CDATA sections":                           `<a><![CDATA[x]]><![CDATA[> " ]]><b c=">"/></a>`,
		"a CDATA section ending in ]":                  `<a><![CDATA[]]]]><b c=">"/></a>`,
		"a declaration of quoted > and <":              `<!DOCTYPE a SYSTEM "> <" '" >'><a/>`,
		"a declaration of entities and a comment":      `<!DOCTYPE a [<!ENTITY e "> < '"><!-- > < " --><!ATTLIST a b CDATA '>'>]><a/>`,
		"a declaration of nested <>":                   `<!x <<y> <z <>> > ><a/>`,
		"a declaration whose first byte is a >":        `<!> ><a b=">"/>`,
		"a declaration whose first byte is a quote":    `<!" ><a b=">"/>`,
		"a declaration of a < before !-":               `<!x <!- > ><a/>`,
		"a declaration of a < before !- and a comment": `<!x <!- > <!-- > --> ><a b=">"/>`,
		"a declaration of a comment ending ->":         `<!x <!-- -> > --> ><a/>`,
	} {
		decoder := xml.NewDecoder(strings.NewReader(part))
		var decoded []bool
		for {
			start := decoder.InputOffset()
			_, err := decoder.RawToken()
			if err == io.EOF {
				break
			}
			s.Require().NoError(err, name)
			for range decoder.InputOffset() - start {
				decoded = append(decoded, part[start] == '<')
			}
		}
		s.Require().Len(decoded, len(part), name)

		guard := newTagGuard(strings.NewReader(part))
		var guarded []bool
		for range part {
			before := guard.state != inText
			_, err := guard.ReadByte()
			s.Require().NoError(err, name)
			guarded = append(guarded, before || guard.state != inText)
		}

		s.Equal(decoded, guarded, name)
	}
}

// A part in another encoding is read by the guard as the text it decodes to, not as its bytes: a
// character whose bytes are a < and a quote opens no tag and no value. The part reads, and is
// counted to its end: an element nested past the limit after the character is refused.
func (s *ExtractSuite) TestAPartInAnotherEncodingIsGuardedAsItsText() {
	// One character of ISO-2022-JP, whose two bytes are a < and a quote, then a text longer than a
	// tag may be, with no quote in it.
	text := "\x1b$B<\"\x1b(B" + strings.Repeat("y", maxTagBytes)
	sheet := func(after string) []byte {
		return s.rezip(s.workbook(1, 1), map[string][]byte{"xl/worksheets/sheet1.xml": []byte(
			`<?xml version="1.0" encoding="iso-2022-jp"?><worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>` + text + `</t></is></c></row></sheetData>` + after + `</worksheet>`)})
	}

	read, err := s.extractor.Extract(context.Background(), "encoded.xlsx", sheet(""))
	s.Require().NoError(err)
	s.Contains(read.Pages[0].Text, "| 滋y")

	_, err = s.extractor.Extract(context.Background(), "encoded.xlsx", sheet(s.deep(maxXMLDepth)))
	s.requireReason(err, TooLarge)
	s.Contains(err.Error(), "nested")
}

// elementsIn is how many XML elements the parts of an archive hold: the tags that open an element,
// or are one.
func (s *ExtractSuite) elementsIn(data []byte) int {
	archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	s.Require().NoError(err)
	elements := 0
	for _, file := range archive.File {
		part, err := readZipFile(file)
		s.Require().NoError(err)
		elements += bytes.Count(part, []byte("<")) - bytes.Count(part, []byte("</")) - bytes.Count(part, []byte("<?"))
	}
	return elements
}

// The elements of every part are counted, for the whole workbook, and a workbook may hold as many
// as the limit and no more. The shared strings are read more than once as they are counted, and
// their elements count once. A workbook is counted at a few elements here and the limit checked on
// its own, since counting one to the limit takes long under the race detector.
func (s *ExtractSuite) TestAWorkbookMayHoldAsManyElementsAsTheLimitAndNoMore() {
	data := s.rezip(s.cellsNaming(1, `<c t="s"><v>0</v></c>`, "xl/sharedStrings.xml", `<si><t>x</t></si>`), map[string][]byte{
		"docProps/first.xml":  []byte(`<a><b/></a>`),
		"docProps/second.xml": []byte(`<a><b/><b/></a>`),
	})

	count := cellCount{ctx: context.Background()}
	s.Require().NoError(count.workbook(data))
	s.Equal(s.elementsIn(data), count.counts.elements)

	s.checksLimit(tally{elements: maxWorkbookElements}, tally{elements: maxWorkbookElements + 1}, "XML elements allowed.")
}

// checksLimit checks that counts at a limit are within it, and counts past it are refused with the
// given words.
func (s *ExtractSuite) checksLimit(at, past tally, says string) {
	within := limitedTokens{counts: &at}
	s.Require().NoError(within.limit())
	over := limitedTokens{counts: &past}
	s.Require().ErrorAs(over.limit(), new(overLimit))
	s.Contains(over.limit().Error(), says)
}

// A row or a cell may repeat a position: the iterator reads one of them, but excelize decodes each
// one as it reads a sheet's pictures. The row elements of a workbook and the cell elements of its
// rows are counted, a cell of the calculation chain or of any other element no cell of a row, and
// a workbook may hold as many of either as it may hold cells, and no more. One of more is refused
// before any of them is decoded.
func (s *ExtractSuite) TestAWorkbookMayHoldAsManyRowsAndCellsAsTheCellLimitAndNoMore() {
	data := s.rezip(s.sheet(`<row r="1"><c r="A1"/><c r="B1"/><x><c r="C1"/></x></row><c r="D1"/><row r="2"/>`), map[string][]byte{
		"xl/calcChain.xml": []byte(`<calcChain xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><c r="A1" i="1"/></calcChain>`),
	})
	count := cellCount{ctx: context.Background()}
	s.Require().NoError(count.workbook(data))
	s.Equal(2, count.counts.rows)
	s.Equal(2, count.counts.cells)

	s.checksLimit(tally{rows: maxWorkbookCells, cells: maxWorkbookCells}, tally{rows: maxWorkbookCells + 1}, "rows or cells")
	s.checksLimit(tally{rows: maxWorkbookCells, cells: maxWorkbookCells}, tally{cells: maxWorkbookCells + 1}, "rows or cells")

	// One cell more than the limit, all of them at one position.
	data = s.sheet(`<row r="1">` + strings.Repeat(`<c r="A1"/>`, maxWorkbookCells+1) + `</row>`)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := s.extractor.Extract(context.Background(), "repeated.xlsx", data)
	runtime.ReadMemStats(&after)

	s.requireReason(err, TooLarge)
	s.Contains(err.Error(), "rows or cells")
	// The refused read allocates a few hundred bytes for each element it reads up to the limit,
	// and decoding every one allocates more than a thousand for each. 600 bytes for each is about
	// twice the first and half the second.
	s.Less(after.TotalAlloc-before.TotalAlloc, uint64(600*maxWorkbookCells))
}

// others is how many elements of a part count toward maxPartElements.
func (s *ExtractSuite) others(part string) int {
	tokens := &limitedTokens{guard: newTagGuard(strings.NewReader(part)), counts: &tally{}, tick: func() error { return nil }}
	tokens.raw = xml.NewDecoder(tokens.guard)
	for {
		if _, err := tokens.Token(); err != nil {
			return tokens.others
		}
	}
}

// A part may hold as many elements under its root, outside a sheet's cells, the shared strings and
// a pivot table's copy of its cells, as the limit, and no more. The styles are decoded as the
// workbook is opened, so styles of more are refused before they are decoded.
func (s *ExtractSuite) TestAPartMayHoldAsManyElementsOutsideItsCellsAsTheLimitAndNoMore() {
	for part, want := range map[string]int{
		`<styleSheet><cellXfs><xf/><xf/></cellXfs></styleSheet>`:                                             3,
		`<worksheet><sheetData><row><c/></row></sheetData><mergeCells><mergeCell/></mergeCells></worksheet>`: 2,
		`<sst><si><t>x</t></si></sst>`:                                                                       0,
		`<pivotCacheRecords><r><n/></r></pivotCacheRecords>`:                                                 0,
		`<x><sst><si/></sst><y/></x>`:                                                                        1,
	} {
		s.Equal(want, s.others(part), part)
	}
	within := limitedTokens{counts: &tally{}, others: maxPartElements}
	s.Require().NoError(within.limit())
	over := limitedTokens{counts: &tally{}, others: maxPartElements + 1}
	s.Require().ErrorAs(over.limit(), new(overLimit))

	styles := `<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><cellXfs>` + strings.Repeat(`<xf/>`, maxPartElements) + `</cellXfs></styleSheet>`
	data := s.rezip(s.workbook(1, 1), map[string][]byte{"xl/styles.xml": []byte(styles)})
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := s.extractor.Extract(context.Background(), "styles.xlsx", data)
	runtime.ReadMemStats(&after)

	s.requireReason(err, TooLarge)
	s.Contains(err.Error(), "outside its cells")
	// As for repeated cells: a few hundred bytes for each element read up to the limit, more than
	// a thousand for each decoded.
	s.Less(after.TotalAlloc-before.TotalAlloc, uint64(600*maxPartElements))
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// A workbook whose parts say they unpack to more than a workbook may is refused before any of them
// is read, as excelize refuses it when it opens it. The part here says it is longer than it is, so
// the file stays small. One that says it unpacks to the limit is only counted, not read: excelize
// sets aside as much memory as each part says it holds.
func (s *ExtractSuite) TestAWorkbookThatSaysItUnpacksPastTheLimitIsRefusedBeforeItIsRead() {
	book := s.workbook(1, 1)
	archive, err := zip.NewReader(bytes.NewReader(book), int64(len(book)))
	s.Require().NoError(err)
	size := uint64(0)
	for _, file := range archive.File {
		size += file.UncompressedSize64
	}
	for _, test := range []struct {
		says    uint64
		refused bool
	}{
		{maxWorkbook - size, false},
		{maxWorkbook - size + 1, true},
	} {
		var out bytes.Buffer
		writer := zip.NewWriter(&out)
		for _, file := range archive.File {
			s.Require().NoError(writer.Copy(file))
		}
		content := []byte(`<a>long</a>`)
		part, err := writer.CreateRaw(&zip.FileHeader{Name: "docProps/long.xml", Method: zip.Store,
			CompressedSize64: uint64(len(content)), UncompressedSize64: test.says})
		s.Require().NoError(err)
		_, err = part.Write(content)
		s.Require().NoError(err)
		s.Require().NoError(writer.Close())

		if !test.refused {
			count := cellCount{ctx: context.Background()}
			s.Require().NoError(count.workbook(out.Bytes()))
			continue
		}

		_, err = s.extractor.Extract(context.Background(), "long.xlsx", out.Bytes())

		s.requireReason(err, TooLarge)
		s.Contains(err.Error(), "unpacks to more than")
	}
}
