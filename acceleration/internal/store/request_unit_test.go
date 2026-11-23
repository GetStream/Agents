package store

import (
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestRequestValidate(t *testing.T) {
	valid := func() Request {
		return Request{CustomerID: "acme", Modality: "stt", Provider: "deepgram", Model: "flux-general-en", UsageID: uuid.New()}
	}
	request := valid()
	require.NoError(t, request.Validate())

	for field, broken := range map[string]func(*Request){
		"customer_id": func(r *Request) { r.CustomerID = "" },
		"modality":    func(r *Request) { r.Modality = " " },
		"usage_id":    func(r *Request) { r.UsageID = uuid.Nil },
	} {
		request := valid()
		broken(&request)
		var invalid *ValidationError
		require.True(t, errors.As(request.Validate(), &invalid), field)
		require.Equal(t, field, invalid.Field)
	}
}
