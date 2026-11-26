package extract

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
