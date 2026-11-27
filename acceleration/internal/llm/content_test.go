package llm

import (
	"strings"
	"unicode/utf8"
)

const (
	pngHeader  = "\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"
	jpegHeader = "\xff\xd8\xff\xe0\x00\x10JFIF\x00"
)

func (s *LLMSuite) TestBytesThatAreNotAnImageAreRefusedAsAnInvalidImage() {
	for _, label := range []string{"image/jpeg", "image/png", ""} {
		page := ImagePart{MIME: label, Data: []byte("<!doctype html><html><body>Not found</body></html>")}

		s.ErrorIs(page.Validate(), ErrInvalidImage, label)
	}
}

func (s *LLMSuite) TestAnImageUnderTheWrongLabelIsSentAsItsRealFormat() {
	photo := ImagePart{MIME: "image/jpeg", Data: []byte(pngHeader)}

	s.Require().NoError(photo.Validate())
	s.Equal("image/png", photo.MIME)
	s.Equal("data:image/png;base64,", photo.DataURI()[:len("data:image/png;base64,")])
}

func (s *LLMSuite) TestAnImageWithNoLabelIsSentAsItsRealFormat() {
	photo := ImagePart{Data: []byte(pngHeader)}

	s.Require().NoError(photo.Validate())
	s.Equal("image/png", photo.MIME)
}

func (s *LLMSuite) TestEachFormatAModelTakesIsAccepted() {
	formats := map[string]string{
		"image/jpeg": jpegHeader,
		"image/png":  pngHeader,
		"image/gif":  "GIF89a\x01\x00\x01\x00\x00\x00\x00",
		"image/webp": "RIFF\x24\x00\x00\x00WEBPVP8 ",
	}
	for label, header := range formats {
		photo := ImagePart{MIME: label, Data: []byte(header)}

		s.Require().NoError(photo.Validate(), label)
		s.Equal(label, photo.MIME)
	}
}

func (s *LLMSuite) TestAnImageFormatNoModelTakesIsRefusedAsAnInvalidImage() {
	bitmap := ImagePart{MIME: "image/jpeg", Data: []byte("BM\x46\x00\x00\x00\x00\x00\x00\x00\x36\x00\x00\x00")}

	s.ErrorIs(bitmap.Validate(), ErrInvalidImage)
}

func (s *LLMSuite) TestAnImageUnderALabelNoModelTakesIsSentAsItsRealFormat() {
	photo := ImagePart{MIME: "image/heic", Data: []byte(jpegHeader)}

	s.Require().NoError(photo.Validate())
	s.Equal("image/jpeg", photo.MIME)
}

func (s *LLMSuite) TestALongLabelIsLoggedCutShortAndWhole() {
	s.Equal("image/png", labelPrefix("image/png"))
	s.Len(labelPrefix("image/"+strings.Repeat("x", 10_000)), 40)
	cut := labelPrefix(strings.Repeat("é", 100))
	s.True(utf8.ValidString(cut))
	s.Len(cut, 40)
}
