package voices

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/GetStream/Vision-Agents/acceleration/internal/blob"
	"github.com/GetStream/Vision-Agents/acceleration/internal/pricing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/routing"
	"github.com/GetStream/Vision-Agents/acceleration/internal/stack"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// EnvBucketURL is the environment variable naming the bucket voice recordings live in,
// for example s3://voices?region=eu-west-1, gs://voices, or file:///var/lib/router/voices.
const EnvBucketURL = "ROUTER_VOICES_BUCKET_URL"

// VoiceClone is the model a clone is recorded as, under its provider, and the kind its usage
// id names.
const VoiceClone = "voice_clone"

// MaxLine is the most characters a voice is asked to say to be heard, which is enough for a
// line or two: the line is sent to the provider and billed.
const MaxLine = 500

// prepareFailed is the reason a failed binding records. The provider's own answer can carry
// its hosts, request ids and bodies, so it goes to the log only.
const prepareFailed = "the provider could not make a voice from these recordings"

// ErrLineTooLong is a line to hear a voice say that is over MaxLine characters.
var ErrLineTooLong = errors.New("voices: the line is too long to hear a voice say")

// Service is the control plane for a customer's own voices: it holds the rows, the
// recordings behind them and the ids each provider gave them back. It is the only caller of
// a Cloner, so it records every clone it asks for.
type Service struct {
	store    *store.Store
	bucket   *blob.Bucket
	cloners  *Registry
	recorder *routing.Recorder
	gate     routing.Gate
	models   []routing.ProviderConfig
	logger   *slog.Logger
}

// Options configures a Service. The first three dependencies are required, since a voice
// with nowhere to keep its recordings and nobody to teach them to is not a voice.
type Options struct {
	Store   *store.Store
	Bucket  *blob.Bucket
	Cloners *Registry
	// Recorder records each clone as a tts row. Nil records nothing.
	Recorder *routing.Recorder
	// Gate admits each recording, clone and line as it does a routed request. Nil admits all.
	Gate routing.Gate
	// Models are the text-to-speech models requests are routed to, which say how each handles
	// the data it is sent.
	Models []routing.ProviderConfig
	Logger *slog.Logger
}

// NewService validates the options and returns a Service.
func NewService(options Options) (*Service, error) {
	if options.Store == nil {
		return nil, errors.New("voices: a database is required")
	}
	if options.Bucket == nil {
		return nil, fmt.Errorf("voices: an object bucket is required (set %s)", EnvBucketURL)
	}
	if options.Cloners == nil {
		return nil, errors.New("voices: at least one provider must be able to clone")
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	recorder := options.Recorder
	if recorder != nil {
		recorder = recorder.For(routing.TTS)
	}
	return &Service{
		store:    options.Store,
		bucket:   options.Bucket,
		cloners:  options.Cloners,
		recorder: recorder,
		gate:     options.Gate,
		models:   options.Models,
		logger:   logger,
	}, nil
}

// Create records a new, empty voice.
func (s *Service) Create(ctx context.Context, customerID, name, description string) (store.Voice, error) {
	voice := store.Voice{CustomerID: customerID, Name: name, Description: description}
	if err := s.store.Voices.Insert(ctx, &voice); err != nil {
		return store.Voice{}, err
	}
	return voice, nil
}

// AddSample stores a recording and attaches it to the voice. It costs nothing, so only a
// customer who may do no work at all is refused.
func (s *Service) AddSample(ctx context.Context, customerID, voiceID string, sample Sample) error {
	if _, err := s.admit(ctx, customerID); err != nil {
		return err
	}
	voice, err := s.store.Voices.Get(ctx, customerID, voiceID)
	if err != nil {
		return err
	}
	if len(sample.Content) == 0 {
		return stack.Wrap(errors.New("voices: a recording with no audio in it is not a recording"))
	}

	key := sampleKey(voice, sample.Name)
	written, err := s.bucket.Write(ctx, key, sample.ContentType, bytes.NewReader(sample.Content))
	if err != nil {
		return err
	}

	stored := store.VoiceSample{
		VoiceID:     voice.ID,
		ObjectKey:   key,
		ContentType: sample.ContentType,
		Bytes:       written,
		Transcript:  sample.Transcript,
	}
	if err := s.store.VoiceSamples.Insert(ctx, &stored); err != nil {
		// The row is what makes the object findable, so an orphan is swept up here rather
		// than left in the bucket costing money nobody can account for.
		if err := s.bucket.Delete(ctx, key); err != nil {
			s.logger.Warn("could not remove a recording whose row failed to save",
				"voice", voice.ID, "key", key, "error", err)
		}
		return err
	}
	return nil
}

// Prepare teaches the voice to each named provider, or to every provider that can clone
// when none are named. One provider refusing does not stop the others: its binding records
// why, and the router simply will not choose it for this voice. Each clone is recorded for
// the customer, under the key that asked.
//
// A provider the customer's policies keep their voice from (clonable) is refused before any is
// asked when it is named, and left out when none are, which is refused when that leaves none.
// What a provider already has of the voice stays as it was.
func (s *Service) Prepare(ctx context.Context, customerID, voiceID string, providers []string) error {
	admission, err := s.admit(ctx, customerID)
	if err != nil {
		return err
	}
	voice, err := s.store.Voices.Get(ctx, customerID, voiceID)
	if err != nil {
		return err
	}

	request, err := s.request(ctx, voice)
	if err != nil {
		return err
	}
	named := len(providers) > 0
	if !named {
		providers = s.Providers()
	}
	if len(providers) == 0 {
		return stack.Wrap(errors.New("voices: this deployment has no provider that can be given a voice"))
	}

	var kept []string
	var refused error
	for _, provider := range providers {
		if err := s.clonable(admission, provider); err != nil {
			if named {
				return err
			}
			// Money is named over a policy: paying would have served the customer here.
			if refused == nil || errors.Is(err, admission.Spend) {
				refused = err
			}
			continue
		}
		kept = append(kept, provider)
	}
	if len(kept) == 0 {
		return refused
	}

	for _, provider := range kept {
		cloner, err := s.cloners.Cloner(provider)
		if err != nil {
			return err
		}
		s.prepareOne(ctx, voice, provider, cloner, request)
	}
	return nil
}

// Delete removes the voice from every provider that has it, then the recordings, then the
// row. Providers first, because a voice we have forgotten about but are still billed for
// is the failure worth avoiding.
func (s *Service) Delete(ctx context.Context, customerID, voiceID string) error {
	voice, err := s.store.Voices.Get(ctx, customerID, voiceID)
	if err != nil {
		return err
	}

	bindings, err := s.store.VoiceBindings.Select(ctx, voice.ID)
	if err != nil {
		return err
	}
	for _, binding := range bindings {
		if binding.ExternalID == "" {
			continue
		}
		cloner, err := s.cloners.Cloner(binding.Provider)
		if err != nil {
			continue
		}
		if err := cloner.Delete(ctx, binding.ExternalID); err != nil {
			s.logger.Warn("a provider still holds a voice that has been deleted here",
				"voice", voice.ID, "provider", binding.Provider, "external_id", binding.ExternalID,
				"error", err)
		}
	}

	samples, err := s.store.VoiceSamples.Select(ctx, voice.ID)
	if err != nil {
		return err
	}
	for _, sample := range samples {
		if err := s.bucket.Delete(ctx, sample.ObjectKey); err != nil {
			s.logger.Warn("could not remove a recording of a deleted voice",
				"voice", voice.ID, "key", sample.ObjectKey, "error", err)
		}
	}

	return s.store.Voices.Delete(ctx, customerID, voice.ID)
}

// Providers reports which providers a voice can be prepared with, sorted by name.
func (s *Service) Providers() []string {
	providers := s.cloners.Providers()
	sort.Strings(providers)
	return providers
}

// Speak says a line in the voice through one provider it is ready with. The line is a tts row
// of the customer's, under the key that asked, billed the characters it sent at the rate of
// the model the provider speaks it with. A line over MaxLine characters is refused before the
// provider is asked, and so is one the customer may not pay for or whose model their policies
// do not let it be sent to.
func (s *Service) Speak(ctx context.Context, customerID, voiceID, provider, text string) (Speech, error) {
	if strings.TrimSpace(text) == "" {
		return Speech{}, stack.Wrap(errors.New("voices: there is nothing to say"))
	}
	characters := utf8.RuneCountInString(text)
	if characters > MaxLine {
		return Speech{}, stack.Wrap(fmt.Errorf("%w: at most %d characters, this one has %d", ErrLineTooLong, MaxLine, characters))
	}
	cloner, err := s.cloners.Cloner(provider)
	if err != nil {
		return Speech{}, err
	}
	externalID, err := s.store.VoiceBindings.GetReady(ctx, customerID, voiceID, provider)
	if err != nil {
		return Speech{}, err
	}
	admission, err := s.admit(ctx, customerID)
	if err != nil {
		return Speech{}, err
	}
	model := s.model(provider, cloner.SpeechModel())
	if _, err := admission.Allowed([]routing.ProviderConfig{model}); err != nil {
		return Speech{}, err
	}
	config := routing.ProviderConfig{Provider: provider, Model: cloner.SpeechModel()}
	if err := s.affordable(admission, config); err != nil {
		return Speech{}, err
	}
	return routing.Call(ctx, routing.Meter{
		Recorder: s.recorder,
		Config:   config,
		Owner:    owner(ctx, customerID),
		Estimate: pricing.Quantities{pricing.Characters: int64(characters)},
	}, func(ctx context.Context) (Speech, error) {
		speech, err := cloner.Speak(ctx, externalID, text)
		speech.Characters = int64(characters)
		return speech, err
	})
}

// owner is whose work a customer's voices are, under the key that asked.
func owner(ctx context.Context, customerID string) routing.Owner {
	return routing.Owner{CustomerID: customerID, Attribution: routing.AttributionFrom(ctx)}
}

// admit asks the customer's policies whether they may do any work, and what it is held to.
func (s *Service) admit(ctx context.Context, customerID string) (routing.Admission, error) {
	if s.gate == nil {
		return routing.Admission{}, nil
	}
	return s.gate.Admit(ctx, owner(ctx, customerID))
}

// affordable refuses work that costs money when the customer may not spend any, as routing does.
func (s *Service) affordable(admission routing.Admission, config routing.ProviderConfig) error {
	if admission.Spend != nil && s.recorder != nil && !s.recorder.Billable(config) {
		return nil
	}
	return admission.Spend
}

// clonable refuses a provider the customer's recordings may not be sent to. A clone names no
// model a policy can, so the provider may have it when the customer may be spoken to by any of
// its models. One with no routed model is judged, and named in the refusal, as its clone.
func (s *Service) clonable(admission routing.Admission, provider string) error {
	var models []routing.ProviderConfig
	for _, model := range s.models {
		if model.Provider == provider {
			models = append(models, model)
		}
	}
	clone := routing.ProviderConfig{Provider: provider, Model: VoiceClone}
	if len(models) == 0 {
		models = append(models, clone)
	}
	if _, err := admission.Allowed(models); err != nil {
		return err
	}
	return s.affordable(admission, clone)
}

// model is the provider's model as it is routed, or nothing but its name when it is not.
func (s *Service) model(provider, name string) routing.ProviderConfig {
	for _, model := range s.models {
		if model.Provider == provider && model.Model == name {
			return model
		}
	}
	return routing.ProviderConfig{Provider: provider, Model: name}
}

// clone is a voice a provider made, named by the clone it made, so the same clone reported
// again bills nothing and a new one upstream is billed.
type clone struct {
	Prepared
	id uuid.UUID
}

func (c clone) UsageID() uuid.UUID { return c.id }

// Samples returns a voice's recordings, oldest first.
func (s *Service) Samples(ctx context.Context, voiceID string) ([]store.VoiceSample, error) {
	return s.store.VoiceSamples.Select(ctx, voiceID)
}

// Bindings returns what each provider made of a voice.
func (s *Service) Bindings(ctx context.Context, voiceID string) ([]store.VoiceBinding, error) {
	return s.store.VoiceBindings.Select(ctx, voiceID)
}

// prepareOne sends the recordings to one provider and writes down what came back. It marks
// the binding pending first, so a clone that is still running is distinguishable from one
// that was never asked for.
//
// A provider that already had the voice keeps its previous id on the binding until a new
// clone replaces it, and the replaced clone is then taken off the provider. Otherwise every
// re-preparation would leave one more voice upstream that nothing here knows to delete.
func (s *Service) prepareOne(
	ctx context.Context, voice store.Voice, provider string, cloner Cloner, request Request,
) {
	previous, err := s.externalID(ctx, voice.ID, provider)
	if err != nil {
		s.logger.Warn("could not read what a provider already had of a voice",
			"voice", voice.ID, "provider", provider, "error", err)
		return
	}

	pending := store.VoiceBinding{
		VoiceID: voice.ID, Provider: provider, State: store.VoicePending, ExternalID: previous,
	}
	if err := s.store.VoiceBindings.Upsert(ctx, &pending); err != nil {
		s.logger.Warn("could not record that a voice is being prepared",
			"voice", voice.ID, "provider", provider, "error", err)
		return
	}

	binding := store.VoiceBinding{VoiceID: voice.ID, Provider: provider, ExternalID: previous}
	made, err := routing.Call(ctx, routing.Meter{
		Recorder: s.recorder,
		Config:   routing.ProviderConfig{Provider: provider, Model: VoiceClone},
		Owner:    owner(ctx, voice.CustomerID),
		Estimate: pricing.Quantities{pricing.Clones: 1},
	}, func(ctx context.Context) (clone, error) {
		prepared, err := cloner.Prepare(ctx, request)
		return clone{Prepared: prepared, id: routing.UsageID(VoiceClone, provider, voice.CustomerID, voice.ID, prepared.ExternalID)}, err
	})
	externalID := made.ExternalID
	if err != nil {
		binding.State = store.VoiceFailed
		binding.Error = prepareFailed
		s.logger.Warn("a provider would not take this voice",
			"voice", voice.ID, "provider", provider, "error", err)
	} else {
		binding.State = store.VoiceReady
		binding.ExternalID = externalID
	}

	if err := s.store.VoiceBindings.Upsert(ctx, &binding); err != nil {
		s.logger.Warn("could not record what a provider made of a voice",
			"voice", voice.ID, "provider", provider, "error", err)
		return
	}
	if binding.State == store.VoiceReady && previous != "" && previous != externalID {
		if err := cloner.Delete(ctx, previous); err != nil {
			s.logger.Warn("a provider still holds a voice that has been replaced here",
				"voice", voice.ID, "provider", provider, "external_id", previous, "error", err)
		}
	}
}

// externalID is the id a provider last gave the voice, or empty when it never had it.
func (s *Service) externalID(ctx context.Context, voiceID, provider string) (string, error) {
	bindings, err := s.store.VoiceBindings.Select(ctx, voiceID)
	if err != nil {
		return "", err
	}
	for _, binding := range bindings {
		if binding.Provider == provider {
			return binding.ExternalID, nil
		}
	}
	return "", nil
}

// request reads the recordings back out of the bucket, which is what every cloner is given.
func (s *Service) request(ctx context.Context, voice store.Voice) (Request, error) {
	stored, err := s.store.VoiceSamples.Select(ctx, voice.ID)
	if err != nil {
		return Request{}, err
	}
	if len(stored) == 0 {
		return Request{}, stack.Wrap(errors.New("voices: add a recording before preparing the voice"))
	}

	samples := make([]Sample, 0, len(stored))
	for _, sample := range stored {
		content, err := s.bucket.Read(ctx, sample.ObjectKey)
		if err != nil {
			return Request{}, err
		}
		samples = append(samples, Sample{
			Name:        path.Base(sample.ObjectKey),
			ContentType: sample.ContentType,
			Content:     content,
			Transcript:  sample.Transcript,
		})
	}

	return Request{Name: voice.Name, Description: voice.Description, Samples: samples}, nil
}

// sampleKey lays recordings out under the customer and the voice, so a bucket stays
// readable to whoever has to go looking in it.
func sampleKey(voice store.Voice, filename string) string {
	name := path.Base(strings.TrimSpace(filename))
	if name == "" || name == "." || name == "/" {
		name = "sample.wav"
	}
	return path.Join(voice.CustomerID, voice.ID, store.NewID()+"-"+name)
}
