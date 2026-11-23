package blob

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/suite"
)

type BucketSuite struct {
	suite.Suite
	bucket *Bucket
}

func TestBucketSuite(t *testing.T) { suite.Run(t, new(BucketSuite)) }

func (s *BucketSuite) SetupTest() {
	bucket, err := Open(context.Background(), "file://"+s.T().TempDir())
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = bucket.Close() })
	s.bucket = bucket
}

// stopping is an upload whose sender goes away part way: it cancels the write and fails.
type stopping struct{ cancel context.CancelFunc }

var errSenderGone = errors.New("the sender went away")

func (r stopping) Read([]byte) (int, error) {
	r.cancel()
	return 0, errSenderGone
}

func (s *BucketSuite) TestAWriteThatFailedSaysWhyEvenWhenNothingWasLeftToRemove() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := s.bucket.Write(ctx, "upload", "text/plain", stopping{cancel: cancel})

	s.ErrorIs(err, errSenderGone, "an object never written is not found to remove, which is not why the write failed")
}
