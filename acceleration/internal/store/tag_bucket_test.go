//go:build integration

package store

import "time"

func (s *StoreSuite) TestSpendIsBrokenDownByWhateverTheCustomerLabelledIt() {
	s.tagged("acme", s.base.Add(time.Minute), 1000, map[string]string{"project": "support"})
	s.tagged("acme", s.base.Add(2*time.Minute), 3000, map[string]string{"project": "support"})
	s.tagged("acme", s.base.Add(3*time.Minute), 500, map[string]string{"project": "sales"})

	buckets, err := s.store.TagBuckets.Select(s.ctx, "acme", TagBucketFilter{Modality: "stt", Key: "project", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
	s.Require().NoError(err)

	s.Require().Len(buckets, 2, "one row per project")
	s.Equal("sales", buckets[0].TagValue)
	s.EqualValues(500, buckets[0].AudioMsTotal)
	s.Equal("support", buckets[1].TagValue)
	s.EqualValues(4000, buckets[1].AudioMsTotal)
}

func (s *StoreSuite) TestARequestCarryingTwoLabelsIsCountedUnderBoth() {
	s.tagged("acme", s.base.Add(time.Minute), 1000,
		map[string]string{"project": "support", "environment": "dev"})

	byProject, err := s.store.TagBuckets.Select(s.ctx, "acme", TagBucketFilter{Modality: "stt", Key: "project", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
	s.Require().NoError(err)
	s.Require().Len(byProject, 1)
	s.EqualValues(1000, byProject[0].AudioMsTotal)

	byEnvironment, err := s.store.TagBuckets.Select(s.ctx, "acme", TagBucketFilter{Modality: "stt", Key: "environment", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
	s.Require().NoError(err)
	s.Require().Len(byEnvironment, 1)
	s.EqualValues(1000, byEnvironment[0].AudioMsTotal, "the same request, seen through the other label")
}

func (s *StoreSuite) TestATaggedRequestIsInTheTagStatisticsWithoutARollup() {
	s.tagged("acme", s.base.Add(time.Minute), 1000, map[string]string{"project": "support"})
	s.tagged("acme", s.base.Add(2*time.Minute), 500, map[string]string{"environment": "dev"})

	buckets, err := s.store.TagBuckets.Select(s.ctx, "acme", TagBucketFilter{Modality: "stt", Key: "project", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
	s.Require().NoError(err)

	s.Require().Len(buckets, 1, "the request without the label is not in its breakdown")
	s.Equal("project", buckets[0].TagKey)
	s.Equal("support", buckets[0].TagValue)
	s.Equal(s.base, buckets[0].Bucket.UTC())
	s.EqualValues(1000, buckets[0].AudioMsTotal)
	s.EqualValues(1, buckets[0].RequestCount)
	s.Require().NotNil(buckets[0].Uptime)
	s.InDelta(1.0, *buckets[0].Uptime, 0.001)
}

func (s *StoreSuite) TestTagStatisticsAreOnlyTheCallingCustomers() {
	s.tagged("acme", s.base.Add(time.Minute), 1000, map[string]string{"project": "support"})
	s.tagged("globex", s.base.Add(time.Minute), 5000, map[string]string{"project": "support"})

	buckets, err := s.store.TagBuckets.Select(s.ctx, "acme", TagBucketFilter{Modality: "stt", Key: "project", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
	s.Require().NoError(err)
	s.Require().Len(buckets, 1)
	s.EqualValues(1000, buckets[0].AudioMsTotal)
}

// tagged stores a labelled request, which is what the tag rollups are built from.
func (s *StoreSuite) tagged(customerID string, at time.Time, audioMs int64, tags map[string]string) {
	latencyMs := 100.0
	s.Require().NoError(s.store.Requests.Insert(s.ctx, &Request{
		Modality:   "stt",
		CustomerID: customerID,
		Provider:   "deepgram",
		Model:      "flux-general-en",
		Tags:       tags,
		StartedAt:  at,
		AudioMs:    audioMs,
		LatencyMs:  &latencyMs,
		Success:    true,
	}))
}

func (s *StoreSuite) TestTagBucketQueryBudgets() {
	s.tagged("acme", s.base, 1000, map[string]string{"project": "support"})
	s.Equal(len(s.store.shards()), s.countQueries(func() {
		buckets, err := s.store.TagBuckets.Select(s.ctx, "acme", TagBucketFilter{Modality: "stt", Key: "project", Granularity: Hourly, From: s.base, To: s.base.Add(time.Hour)})
		s.Require().NoError(err)
		s.Len(buckets, 1)
	}))
}
