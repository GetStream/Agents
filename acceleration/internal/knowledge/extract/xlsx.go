package extract

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/xuri/excelize/v2"
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
	book, err := excelize.OpenReader(bytes.NewReader(data), excelize.Options{UnzipSizeLimit: maxWorkbook})
	if err != nil {
		if strings.Contains(err.Error(), "unzip size") {
			return Document{}, fail(TooLarge, XLSX, err)
		}
		return Document{}, fail(Corrupt, XLSX, fmt.Errorf("the workbook cannot be opened: %w", err))
	}
	defer book.Close()

	count := cellCount{ctx: ctx, charset: book.CharsetReader}
	if err := count.workbook(data); err != nil {
		if ctx.Err() != nil {
			return Document{}, unreadable(ctx, XLSX, err)
		}
		return Document{}, fail(Corrupt, XLSX, fmt.Errorf("the workbook cannot be read: %w", err))
	}
	if count.text > maxWorkbookText {
		return Document{}, fail(TooLarge, XLSX, fmt.Errorf("the shared strings the workbook's cells name are more than the %d bytes of text allowed", maxWorkbookText))
	}

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
// may make a cell's text longer than it is stored, which the rows count as they are read.
type cellCount struct {
	ctx     context.Context
	charset func(label string, input io.Reader) (io.Reader, error)
	archive *zip.Reader
	// shared is the length of each shared string, read at the first cell that names one.
	shared []int32
	read   bool
	text   int
}

// workbook counts the cells of every part of a workbook.
func (c *cellCount) workbook(data []byte) error {
	archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return err
	}
	c.archive = archive
	for _, file := range archive.File {
		if err := c.part(file, c.cells); err != nil {
			return err
		}
	}
	return nil
}

// part reads one part of the workbook, handing each element to read. A part that is not XML,
// or not past its first error, is read no further, as excelize reads it.
func (c *cellCount) part(file *zip.File, read func(*xml.Decoder) error) error {
	r, err := file.Open()
	if err != nil {
		return err
	}
	defer r.Close()
	decoder := xml.NewDecoder(r)
	decoder.CharsetReader = c.charset
	return read(decoder)
}

// cells counts the cells of a part, checking the deadline at each row, as the rows are read.
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
			n, err := c.cell(decoder, start)
			if err != nil {
				return err
			}
			c.text += n
		}
	}
}

// cell is the length of the shared string a cell names, read as excelize reads a cell (rows.go,
// cellXMLHandler; cell.go, getValueFrom). Any other text of a cell is in the file as often as the
// cell is, so it is never more than the part it is in.
func (c *cellCount) cell(decoder *xml.Decoder, start xml.StartElement) (int, error) {
	kind := ""
	for _, attr := range start.Attr {
		if attr.Name.Local == "t" {
			kind = attr.Value
		}
	}
	var value string
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
			case "f", "is":
				// Read whole, so a value inside them is not the cell's.
				err = decoder.Skip()
			}
		case xml.EndElement:
			done = element == start.End()
		}
		if err != nil {
			break
		}
	}
	if kind != "s" || value == "" {
		return 0, nil
	}
	if err := c.readShared(); err != nil {
		return 0, err
	}
	// As in excelize, an index that is not a number is the first string.
	index, _ := strconv.Atoi(strings.TrimSpace(value))
	if index < 0 || index >= len(c.shared) {
		return 0, nil
	}
	return int(c.shared[index]), nil
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
			if err := c.part(file, func(decoder *xml.Decoder) error { return c.sharedStrings(decoder, everyOne) }); err != nil {
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
