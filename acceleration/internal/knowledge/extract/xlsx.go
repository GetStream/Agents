package extract

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/xuri/excelize/v2"
	"github.com/xuri/nfp"
	"golang.org/x/net/html/charset"
)

// maxWorkbook is the most a workbook may unpack to in all. A sheet's XML is several
// times the size of the values in it, so this is far more than a sheet anybody reads.
const maxWorkbook = 4 * maxUnzipped

// maxSheetColumns is the widest a row may be, counted to its last value. Excel's sheet is
// 16,384 columns wide, but a table a person reads is far narrower, and a row is held as wide
// as its last value's column: one value in the last column of a few thousand rows is millions
// of cells from a file of a few kilobytes. 1,024 is sixteen times the 64 columns the ODT
// reader (odt.go) keeps of a repeated or spanned cell.
const maxSheetColumns = 1024

// maxWorkbookCells is the most cells one workbook may hold, the empty ones in a row counted and
// a row with no cells counted as one. The iterator returns every row up to a sheet's last, so
// one value in a sheet's last row is a million rows. A cell is a 16-byte string header, so a
// million is 16 MiB before its text, and a table of that many cells is more than a conversation
// or a knowledge passage can take in. It is counted for the whole workbook, not for each sheet:
// excelize keeps every sheet it has read for its pictures (GetPictureCells) until the workbook
// is closed, padded as the iterator pads it.
const maxWorkbookCells = 1_000_000

// maxWorkbookText is the most text one workbook may be read to, counted once as its cells are
// read and once more as its tables are written, since a sheet's cells and its table are held at
// once. A cell's text is its own copy, so one long shared string in many cells is that string
// many times over. Twice maxUnzipped, so a workbook's tables may be about as long as one part of
// an Office file may unpack to.
const maxWorkbookText = 2 * maxUnzipped

// maxFormatCode is the longest number format a workbook may define, in characters: Excel's own
// limit for one ([MS-XLS] 2.4.126 Format: stFormat "MUST be greater than or equal to 1 character
// and less than or equal to 255 characters"). excelize writes a format's quoted text into a cell's
// value one character at a time, each a new copy of the value so far, so a longer one takes time
// that grows with the square of its length for each cell it is on.
const maxFormatCode = 255

// maxXMLDepth is the deepest an element of a workbook part may be nested. encoding/xml keeps every
// element that is still open, so a part nested deep enough holds far more memory than it is long.
// 256 is libxml2's default limit (xmlParserMaxDepth), set for the same reason.
const maxXMLDepth = 256

// maxWorkbookElements is the most XML elements all the parts of a workbook may hold. excelize decodes
// a part it reads into a struct for each element it knows, which takes many times the bytes of an
// empty element, so the count of elements bounds the memory and not the size of the part. A cell of an
// ordinary workbook is up to five elements: its row when the row holds only it, the cell and its
// value, and its shared string and the string's text. Six for each of the maxWorkbookCells cells a
// workbook may hold leaves room for its formulas, styles and other parts.
const maxWorkbookElements = 6 * maxWorkbookCells

// maxPartElements is the most XML elements one part may hold under its root, outside a sheet's cells
// (sheetData), the shared strings (sst) and a pivot table's copy of its source cells
// (pivotCacheRecords, which excelize never decodes), all of which maxWorkbookElements bounds. excelize decodes such a
// part, the styles, the workbook, or a sheet's merged cells and formats, into structs far larger
// than an element of a sheet's cells, as it opens the workbook or reads a sheet. A part a
// spreadsheet program writes there has far fewer, but the calculation chain, which holds an element
// for each cell with a formula: so as many as maxWorkbookCells.
const maxPartElements = maxWorkbookCells

// maxStringRuns is the most runs one shared or inline string may hold, and the most phonetic runs.
// excelize keeps each run of a string it reads, with no text in it as well. A cell shows at most
// 32,767 characters (Excel's specifications and limits), and a run with no character adds nothing to
// it, so a string that Excel writes has no more runs than that. Its phonetic runs are a reading of
// that text, and as many at most, so they are counted apart.
const maxStringRuns = 32767

// budget is what is left of a workbook's limits as its sheets are read.
type budget struct {
	cells int
	text  int
}

// readXLSX reads a workbook as a table per sheet, each under its sheet's name, with its
// values as the sheet shows them: a price formatted as money reads as money. A hidden
// sheet is left out, since nobody reading the workbook sees it. A sheet's pictures follow
// its table. Its charts are Excel's own drawings, which nothing here draws.
func readXLSX(ctx context.Context, data []byte) (Document, error) {
	// The workbook is counted before excelize opens it, since excelize decodes the workbook, styles,
	// theme, calculation chain and relationships parts as it opens a file (excelize.go, openReaderAt).
	// The count reads XML the way excelize does (excelize.go, newFile).
	count := cellCount{ctx: ctx}
	if err := count.workbook(data); err != nil {
		var over overLimit
		switch {
		case ctx.Err() != nil:
			return Document{}, unreadable(ctx, XLSX, err)
		case errors.As(err, &over):
			return Document{}, fail(TooLarge, XLSX, err)
		}
		return Document{}, fail(Corrupt, XLSX, fmt.Errorf("the workbook cannot be read: %w", err))
	}
	if count.longest > maxFormatCode {
		return Document{}, fail(TooLarge, XLSX, fmt.Errorf("the workbook has a number format of %d characters, more than the %d allowed", count.longest, maxFormatCode))
	}
	if count.overBudget() {
		return Document{}, fail(TooLarge, XLSX, fmt.Errorf("the text of the workbook's cells, its shared strings and the rest as its number formats write them, is more than the %d bytes of text allowed", maxWorkbookText))
	}

	book, err := excelize.OpenReader(bytes.NewReader(data), excelize.Options{UnzipSizeLimit: maxWorkbook})
	if err != nil {
		if strings.Contains(err.Error(), "unzip size") {
			return Document{}, fail(TooLarge, XLSX, err)
		}
		return Document{}, fail(Corrupt, XLSX, fmt.Errorf("the workbook cannot be opened: %w", err))
	}
	defer book.Close()

	var sections []string
	var kept pictures
	left := budget{cells: maxWorkbookCells, text: maxWorkbookText}
	for _, sheet := range book.GetSheetList() {
		if ctx.Err() != nil {
			return Document{}, unreadable(ctx, XLSX, ctx.Err())
		}
		if visible, err := book.GetSheetVisible(sheet); err == nil && !visible {
			continue
		}
		rows, err := sheetRows(ctx, book, sheet, &left)
		if err != nil {
			return Document{}, err
		}
		start := 0
		for start < len(rows) && strings.TrimSpace(strings.Join(rows[start], "")) == "" {
			start++
		}
		rows = trimRows(rows[start:])
		var blocks []string
		if len(rows) > 0 {
			text, err := tableWithin(ctx, rows, left.text)
			if errors.Is(err, errTableTooLong) {
				return Document{}, fail(TooLarge, XLSX, fmt.Errorf("sheet %q takes the workbook past the %d bytes of text allowed", sheet, maxWorkbookText))
			}
			if err != nil {
				return Document{}, unreadable(ctx, XLSX, err)
			}
			left.text -= len(text)
			blocks = append(blocks, text)
		}
		markers, err := sheetPictures(book, sheet, &kept)
		if err != nil {
			return Document{}, err
		}
		blocks = append(blocks, markers...)
		if len(blocks) == 0 {
			continue
		}
		sections = append(sections, "## "+oneLine(sheet)+"\n\n"+strings.Join(blocks, "\n\n"))
	}
	return Document{Pages: []Page{{Text: strings.Join(sections, "\n\n")}}, Pictures: kept.kept}, nil
}

// sheetRows reads a sheet row by row, as excelize's GetRows does, but refuses it as too large
// when a row is wider than maxSheetColumns or the workbook's cells or text so far are more
// than left, and stops at the deadline between rows. The iterator pads a row with empty
// strings up to its last value (excelize rows.go, appendSpace), so a row is at most 16,384
// cells before it is checked, and is dropped when it is refused.
func sheetRows(ctx context.Context, book *excelize.File, sheet string, left *budget) ([][]string, error) {
	iterator, err := book.Rows(sheet)
	if err != nil {
		return nil, fail(Corrupt, XLSX, fmt.Errorf("sheet %q cannot be read: %w", sheet, err))
	}
	var rows [][]string
	for iterator.Next() {
		if ctx.Err() != nil {
			iterator.Close()
			return nil, unreadable(ctx, XLSX, ctx.Err())
		}
		row, err := iterator.Columns()
		if err != nil {
			// As in GetRows, a row that cannot be read ends the sheet there.
			break
		}
		if len(row) > maxSheetColumns {
			iterator.Close()
			return nil, fail(TooLarge, XLSX, fmt.Errorf("sheet %q has a row of %d columns, more than the %d allowed", sheet, len(row), maxSheetColumns))
		}
		// A row with no cells, one between two rows of values, is still a line of the table.
		if left.cells -= max(len(row), 1); left.cells < 0 {
			iterator.Close()
			return nil, fail(TooLarge, XLSX, fmt.Errorf("sheet %q takes the workbook past the %d cells allowed", sheet, maxWorkbookCells))
		}
		for _, cell := range row {
			left.text -= len(cell)
		}
		if left.text < 0 {
			iterator.Close()
			return nil, fail(TooLarge, XLSX, fmt.Errorf("sheet %q takes the workbook past the %d bytes of text allowed", sheet, maxWorkbookText))
		}
		rows = append(rows, row)
	}
	if err := iterator.Close(); err != nil {
		return nil, fail(Corrupt, XLSX, fmt.Errorf("sheet %q cannot be read: %w", sheet, err))
	}
	return rows, nil
}

// sheetPictures keeps the pictures of a sheet, by the cells they are placed at, and
// returns their markers.
func sheetPictures(book *excelize.File, sheet string, kept *pictures) ([]string, error) {
	cells, err := book.GetPictureCells(sheet)
	if err != nil {
		return nil, fail(Corrupt, XLSX, fmt.Errorf("the pictures of sheet %q cannot be read: %w", sheet, err))
	}
	var markers []string
	for _, cell := range cells {
		placed, err := book.GetPictures(sheet, cell)
		if err != nil {
			return nil, fail(Corrupt, XLSX, fmt.Errorf("the pictures of sheet %q cannot be read: %w", sheet, err))
		}
		for _, picture := range placed {
			if marker := kept.keep(picture.File); marker != "" {
				markers = append(markers, marker)
			}
		}
	}
	return markers, nil
}

// cellCount counts the text of the shared strings a workbook's cells name, before excelize reads
// any cell. excelize builds a row's cells at once (rows.go, Columns), each its own copy of its
// text, so one long shared string in every cell of a row is that string many times over before
// the row can be counted. The workbook is refused when the shared strings its cells name are more
// than the text budget, so no row is built past it. Every part is read as a sheet, since a sheet
// may be any part the workbook names, so a hidden sheet's cells count as well. A number format
// writes a cell's text once for each text placeholder in it (numfmt.go, textHandler), so a cell's
// text is counted that many times over, for the format of the most placeholders the workbook
// defines.
type cellCount struct {
	ctx     context.Context
	archive *zip.Reader
	// shared is the length of each shared string, read at the first cell that names one.
	shared []int32
	read   bool
	// text is the length of the shared strings the cells name.
	text int
	// stored is the length of the cells' own text, which a number format may write more than once.
	stored int
	// longest is the length of the longest number format, in characters.
	longest int
	// repeat is the most times a number format writes a cell's text.
	repeat int
	// tokens is how many XML tokens have been read, of every part.
	tokens int
	// styles is whether the part being read is the one of the number formats.
	styles bool
	// counts is what the parts hold, each part counted once.
	counts tally
}

// tally is what the parts of a workbook hold, of the limits counted over more than one part.
type tally struct {
	elements int
	// rows and cells are how many row elements, and c elements that are a row's children, there
	// are. excelize decodes a sheet whole as it reads its pictures (GetPictureCells), into a
	// struct for each, a row or a cell that repeats a position as well, while the iterator keeps
	// one cell for each position. A sheet that is read holds no more of either than
	// maxWorkbookCells (sheetRows), so neither may the workbook, its hidden sheets counted.
	rows, cells int
}

// tokensPerDeadlineCheck is how many XML tokens are read between two checks of the deadline, so a
// part of elements that are no row, cell or string is not read to its end past it. A token is at
// least a few bytes and costs more to read than a check, so this is a few tens of kilobytes of a
// part between checks, at a cost too small to measure.
const tokensPerDeadlineCheck = 4096

// tick counts one token read, and is the deadline's error at every tokensPerDeadlineCheck tokens.
func (c *cellCount) tick() error {
	if c.tokens++; c.tokens%tokensPerDeadlineCheck != 0 {
		return nil
	}
	return c.ctx.Err()
}

// overBudget is whether the workbook's cells are more text, as excelize reads them, than the text
// budget. A cell's own text is in the file as often as the cell is, so it is counted only when a
// number format writes it more than once.
func (c *cellCount) overBudget() bool {
	if c.repeat <= 1 {
		return c.text > maxWorkbookText
	}
	return c.text+c.stored > maxWorkbookText/c.repeat
}

// workbook counts the cells of every part of a workbook.
func (c *cellCount) workbook(data []byte) error {
	archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return err
	}
	c.archive = archive
	// excelize refuses a workbook that unpacks to more than maxWorkbook (lib.go, checkFileSize), and
	// archive/zip reads no part past the size it says it unpacks to, so the count reads no more.
	size := uint64(0)
	for _, file := range archive.File {
		if file.UncompressedSize64 > maxWorkbook-size {
			return overLimit{fmt.Errorf("the workbook unpacks to more than the %d bytes allowed. Split it into smaller workbooks", maxWorkbook)}
		}
		size += file.UncompressedSize64
	}
	for _, file := range archive.File {
		// excelize reads the number formats from this part only (styles.go, stylesReader).
		c.styles = strings.ReplaceAll(file.Name, `\`, "/") == "xl/styles.xml"
		if err := c.part(file, &c.counts, c.cells); err != nil {
			return err
		}
	}
	return nil
}

// numFmt reads a number format of the styles part, as excelize reads one (xlsxNumFmt): its code,
// and the code that takes its place when there is one.
func (c *cellCount) numFmt(start xml.StartElement) {
	for _, attr := range start.Attr {
		if attr.Name.Local == "formatCode" || attr.Name.Local == "formatCode16" {
			c.format(attr.Value)
		}
	}
}

// format notes the length of a number format and how many times it writes a cell's text: once for
// each text or zero placeholder of a text section, parsed as excelize parses it (numfmt.go, format).
func (c *cellCount) format(code string) {
	length := utf8.RuneCountInString(code)
	c.longest = max(c.longest, length)
	if length > maxFormatCode {
		return
	}
	parser := nfp.NumberFormatParser()
	for _, section := range parser.Parse(code) {
		if section.Type != nfp.TokenSectionText {
			continue
		}
		placeholders := 0
		for _, item := range section.Items {
			if item.TType == nfp.TokenTypeTextPlaceHolder || item.TType == nfp.TokenTypeZeroPlaceHolder {
				placeholders++
			}
		}
		c.repeat = max(c.repeat, placeholders)
	}
}

// part reads one part of the workbook, handing each element to read, and counting what it holds into
// counts. A part that is not XML, or not past its first error, is read no further, as excelize
// reads it. A part past a limit of limitedTokens is that limit's error.
func (c *cellCount) part(file *zip.File, counts *tally, read func(*xml.Decoder) error) error {
	r, err := file.Open()
	if err != nil {
		return err
	}
	defer r.Close()
	tokens := &limitedTokens{guard: newTagGuard(r), counts: counts, tick: c.tick}
	tokens.raw = xml.NewDecoder(tokens.guard)
	tokens.raw.CharsetReader = func(label string, input io.Reader) (io.Reader, error) {
		// From here on the decoder reads the part's text as decoded, and the guard reads it there:
		// a byte of another encoding may be a quote or a bracket that is not one in the text.
		tokens.guard.off = true
		decoded, err := charset.NewReaderLabel(label, input)
		if err != nil {
			return nil, err
		}
		tokens.guard = newTagGuard(decoded)
		return tokens.guard, nil
	}
	if err := read(xml.NewTokenDecoder(tokens)); err != nil {
		return err
	}
	return tokens.err
}

// overLimit is a workbook past one of the limits the count checks: its size, elements, nesting,
// runs, or the length or attributes of a tag.
type overLimit struct{ error }

// limitedTokens hands a part's tokens to a decoder and stops after the first element past a limit:
// nested deeper than maxXMLDepth, past maxWorkbookElements, maxPartElements or maxWorkbookCells rows
// or cells, or a run past maxStringRuns in one string, and at the deadline. The decoder reads every token through it, the ones it decodes or
// skips whole as well. It reads raw tokens, so only the decoder over it keeps the open elements, and
// no more than maxXMLDepth of them. Once past a limit or the deadline it only returns that error,
// since a reader that goes on after an error would otherwise read the rest of the part.
type limitedTokens struct {
	raw *xml.Decoder
	// guard is the guard the decoder reads the part through.
	guard *tagGuard
	// tick counts each token read, the ones read inside an element decoded or skipped whole as well,
	// and is the deadline's error when it has passed.
	tick func() error
	// counts is the count this part adds to.
	counts *tally
	depth  int
	// rows is, for each open element, whether it is a row.
	rows []bool
	// inCells is the depth of the outermost sheet's cells, shared strings or pivot table's cells
	// being read, 0 out of them, and others how many elements under the root are out of them.
	inCells int
	others  int
	// inString is the depth of the outermost string (si or is) being read, 0 out of one, and runs
	// and phonetic are how many of its own children are runs and phonetic runs.
	inString int
	runs     int
	phonetic int
	err      error
}

func (l *limitedTokens) Token() (xml.Token, error) {
	if l.err != nil {
		return nil, l.err
	}
	if l.err = l.tick(); l.err != nil {
		return nil, l.err
	}
	token, err := l.raw.RawToken()
	if l.guard.err != nil {
		// Kept, since the decoder may report it as the syntax error of a character it cut, and a
		// reader that goes on after an error would otherwise take it for the end of the part.
		l.err = l.guard.err
		return nil, l.err
	}
	switch element := token.(type) {
	case xml.StartElement:
		l.depth++
		l.counts.elements++
		name := element.Name.Local
		if name == "c" && len(l.rows) > 0 && l.rows[len(l.rows)-1] {
			l.counts.cells++
		}
		if name == "row" {
			l.counts.rows++
		}
		l.rows = append(l.rows, name == "row")
		if l.inCells == 0 && (name == "sheetData" || name == "sst" || name == "pivotCacheRecords") {
			l.inCells = l.depth
		}
		if l.inCells == 0 && l.depth > 1 {
			l.others++
		}
		switch {
		case l.inString == 0 && (name == "si" || name == "is"):
			l.inString, l.runs, l.phonetic = l.depth, 0, 0
		case l.inString != 0 && l.depth == l.inString+1 && name == "r":
			l.runs++
		case l.inString != 0 && l.depth == l.inString+1 && name == "rPh":
			l.phonetic++
		}
		l.err = l.limit()
	case xml.EndElement:
		if l.depth == l.inString {
			l.inString = 0
		}
		if l.depth == l.inCells {
			l.inCells = 0
		}
		l.rows = l.rows[:max(len(l.rows)-1, 0)]
		l.depth--
	}
	return token, err
}

// limit is the error of the first limit the part is past, or nil.
func (l *limitedTokens) limit() error {
	switch {
	case l.depth > maxXMLDepth:
		return overLimit{fmt.Errorf("the workbook has XML elements nested more than %d deep, deeper than a spreadsheet program writes them", maxXMLDepth)}
	case l.counts.elements > maxWorkbookElements:
		return overLimit{fmt.Errorf("the workbook has more than the %d XML elements allowed. Split it into smaller workbooks", maxWorkbookElements)}
	case l.counts.rows > maxWorkbookCells || l.counts.cells > maxWorkbookCells:
		return overLimit{fmt.Errorf("the workbook's sheets, hidden ones as well, have more than the %d rows or cells allowed. Split it into smaller workbooks", maxWorkbookCells)}
	case l.others > maxPartElements:
		return overLimit{fmt.Errorf("a part of the workbook has more than the %d XML elements allowed outside its cells and strings", maxPartElements)}
	case l.runs > maxStringRuns || l.phonetic > maxStringRuns:
		return overLimit{fmt.Errorf("the workbook has a string of more than %d runs, more than a cell can show. Clear the formatting of its text", maxStringRuns)}
	}
	return nil
}

// cells counts the cells of a part, and notes the number formats of the styles part, checking the
// deadline at each row, as the rows are read, and every tokensPerDeadlineCheck tokens.
func (c *cellCount) cells(decoder *xml.Decoder) error {
	for {
		token, err := decoder.Token()
		if err != nil {
			return nil
		}
		start, ok := token.(xml.StartElement)
		switch {
		case !ok:
		case start.Name.Local == "row":
			if err := c.ctx.Err(); err != nil {
				return err
			}
		case start.Name.Local == "c":
			shared, stored, err := c.cell(decoder, start)
			if err != nil {
				return err
			}
			c.text += shared
			c.stored += stored
		case c.styles && start.Name.Local == "numFmt":
			c.numFmt(start)
		}
	}
}

// cell is the length of the shared string a cell names and of the cell's own text, read as excelize
// reads a cell (rows.go, cellXMLHandler; cell.go, getValueFrom): an inline string's text, or the
// cell's value. Every kind of cell but a formula's string goes through its number format.
func (c *cellCount) cell(decoder *xml.Decoder, start xml.StartElement) (int, int, error) {
	kind := ""
	for _, attr := range start.Attr {
		if attr.Name.Local == "t" {
			kind = attr.Value
		}
	}
	var value string
	inline, hasInline := 0, false
	for done := false; !done; {
		token, err := decoder.Token()
		if err != nil {
			break
		}
		switch element := token.(type) {
		case xml.StartElement:
			switch element.Name.Local {
			case "v":
				err = decoder.DecodeElement(&value, &element)
			case "is":
				// Read whole, so a value inside it is not the cell's.
				var text sharedString
				err = decoder.DecodeElement(&text, &element)
				inline, hasInline = inline+text.length(), true
			case "f":
				// Read whole, so a value inside it is not the cell's.
				err = decoder.Skip()
			}
		case xml.EndElement:
			done = element == start.End()
		}
		if err != nil {
			break
		}
	}
	switch {
	case kind == "str":
		return 0, 0, nil
	case kind == "inlineStr" && hasInline:
		return 0, inline, nil
	case kind != "s" || value == "":
		return 0, len(value), nil
	}
	if err := c.readShared(); err != nil {
		return 0, 0, err
	}
	// As in excelize, an index that is not a number is the first string.
	index, _ := strconv.Atoi(strings.TrimSpace(value))
	if index < 0 || index >= len(c.shared) {
		return 0, 0, nil
	}
	return int(c.shared[index]), 0, nil
}

// readShared reads the length of every shared string, once. excelize reads the strings that are
// the root's children when the part is small enough to hold (rows.go, sharedStringsReader), and
// every one in the part when it is not (rows.go, getFromStringItem): both are read, and a string
// is counted as the longer of the two at its index, so a cell is never counted shorter than it
// is read.
func (c *cellCount) readShared() error {
	if c.read {
		return nil
	}
	c.read = true
	for _, file := range c.archive.File {
		if !strings.EqualFold(strings.ReplaceAll(file.Name, `\`, "/"), "xl/sharedStrings.xml") {
			continue
		}
		for _, everyOne := range []bool{false, true} {
			// The loop over every part counts this part's elements into the workbook's, so these two
			// reads only check the part's own count against the limit.
			var counts tally
			if err := c.part(file, &counts, func(decoder *xml.Decoder) error { return c.sharedStrings(decoder, everyOne) }); err != nil {
				return err
			}
		}
	}
	return nil
}

// sharedStrings reads the length of the shared strings of a part: of every one in it, or of the
// root's children only.
func (c *cellCount) sharedStrings(decoder *xml.Decoder, everyOne bool) error {
	depth, index := 0, 0
	for {
		token, err := decoder.Token()
		if err != nil {
			return nil
		}
		switch element := token.(type) {
		case xml.StartElement:
			if element.Name.Local != "si" || !everyOne && depth != 1 {
				depth++
				continue
			}
			if err := c.ctx.Err(); err != nil {
				return err
			}
			var text sharedString
			err := decoder.DecodeElement(&text, &element)
			if index == len(c.shared) {
				c.shared = append(c.shared, 0)
			}
			// A string is at most the part it is in, a few hundred megabytes.
			c.shared[index] = max(c.shared[index], int32(text.length()))
			index++
			if err != nil {
				return nil
			}
		case xml.EndElement:
			depth--
		}
	}
}

// sharedString is a shared string as excelize reads it (xlsxSI): its text, and the text of its
// runs.
// excelize then only shortens it, when it unescapes characters (lib.go, bstrUnmarshal).
type sharedString struct {
	T *sharedText `xml:"t"`
	R []struct {
		T *sharedText `xml:"t"`
	} `xml:"r"`
}

// sharedText is the text of a string or of one of its runs (xlsxT).
type sharedText struct {
	Val string `xml:",chardata"`
}

func (s *sharedString) length() int {
	n := 0
	if s.T != nil {
		n += len(s.T.Val)
	}
	for _, run := range s.R {
		if run.T != nil {
			n += len(run.T.Val)
		}
	}
	return n
}

// maxTagBytes is the longest one tag of a workbook part may be, in bytes. encoding/xml reads a tag
// whole, its name and every attribute, before anything can check it, and excelize reads it again.
// The longest tags a spreadsheet program writes, such as a data connection's settings, are a few
// kilobytes.
const maxTagBytes = 1 << 20

// maxAttributes is the most attributes one element of a workbook part may have, its namespace
// declarations as well. encoding/xml builds a value for each attribute of a tag, and keeps each
// namespace an element declares while it is open, so this bounds those to maxXMLDepth times it. The
// elements Excel and LibreOffice write have a few dozen at most.
const maxAttributes = 256

// tagGuard reads a part to the decoder, and is an overLimit error at the first tag longer than
// maxTagBytes or with more than maxAttributes attributes, before the decoder reads it whole. It counts
// an attribute at each = of a tag outside its quoted values. A comment, a processing instruction or
// a declaration (<!, <?) is counted to its first >, since its text may hold a quote that opens no
// value. It is a byte reader, so the decoder reads it a byte at a time and it sees no byte the
// decoder does not read: a part that is not XML is past its first error long before any of its
// bytes could look like a tag.
type tagGuard struct {
	r *bufio.Reader
	// off is set once the decoder reads the part through another guard, over the decoded text.
	off bool
	// in is whether a tag is being read, and markup whether it is a comment, an instruction or a
	// declaration; begun is whether the first byte after its < has been read.
	in, markup, begun bool
	// quote is the quote of the value being read, or 0 out of one.
	quote byte
	// bytes and attributes are how many the tag being read holds so far.
	bytes, attributes int
	err               error
}

func newTagGuard(r io.Reader) *tagGuard {
	return &tagGuard{r: bufio.NewReader(r)}
}

// Read is how a charset reader reads the part, once the guard is off.
func (g *tagGuard) Read(p []byte) (int, error) {
	if g.off {
		return g.r.Read(p)
	}
	for i := range p {
		b, err := g.ReadByte()
		if err != nil {
			return i, err
		}
		p[i] = b
	}
	return len(p), nil
}

func (g *tagGuard) ReadByte() (byte, error) {
	if g.err != nil {
		return 0, g.err
	}
	b, err := g.r.ReadByte()
	if err != nil || g.off {
		return b, err
	}
	if !g.in {
		if b == '<' {
			g.in, g.begun, g.quote, g.bytes, g.attributes = true, false, 0, 0, 0
		}
		return b, nil
	}
	if !g.begun {
		g.begun, g.markup = true, b == '!' || b == '?'
	}
	g.bytes++
	switch {
	case g.quote != 0:
		if b == g.quote {
			g.quote = 0
		}
	case b == '>':
		g.in = false
	case g.markup:
	case b == '"' || b == '\'':
		g.quote = b
	case b == '=':
		g.attributes++
	}
	switch {
	case g.bytes > maxTagBytes:
		g.err = overLimit{fmt.Errorf("the workbook has an XML tag of more than %d bytes, longer than a spreadsheet program writes one", maxTagBytes)}
	case g.attributes > maxAttributes:
		g.err = overLimit{fmt.Errorf("the workbook has an XML element of more than %d attributes, more than a spreadsheet program writes", maxAttributes)}
	}
	if g.err != nil {
		return 0, g.err
	}
	return b, nil
}
