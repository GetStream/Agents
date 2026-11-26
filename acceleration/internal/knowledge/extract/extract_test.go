package extract

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"image"
	"image/jpeg"
	"image/png"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/klippa-app/go-pdfium"
	"github.com/klippa-app/go-pdfium/requests"
	"github.com/klippa-app/go-pdfium/responses"
	"github.com/stretchr/testify/suite"
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
