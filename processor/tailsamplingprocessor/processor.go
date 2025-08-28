// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tailsamplingprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor"

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/timeutils"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/cache"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/idbatcher"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/sampling"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/store"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/telemetry"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/tracelimiter"
)

// legacyTraceDataAdapter implements store.DecodedSpans interface for legacy sampling.TraceData
// This allows the policy evaluation to work consistently with both storage paths
type legacyTraceDataAdapter struct {
	traceData *sampling.TraceData
	traceID   store.TraceID
	spanCache []store.SpanRecord // Cached conversion of ptrace spans to SpanRecord
	cached    bool
}

// Len returns the total number of spans in the trace
func (l *legacyTraceDataAdapter) Len() int {
	return l.traceData.ReceivedBatches.SpanCount()
}

// At returns a SpanRecord for the given index
func (l *legacyTraceDataAdapter) At(i int) store.SpanRecord {
	// Lazy conversion - convert all spans to SpanRecord format on first access
	if !l.cached {
		l.convertSpansToRecords()
		l.cached = true
	}

	if i < 0 || i >= len(l.spanCache) {
		// Return empty SpanRecord for invalid index
		return store.SpanRecord{}
	}

	return l.spanCache[i]
}

// convertSpansToRecords converts ptrace.Traces to []store.SpanRecord format
func (l *legacyTraceDataAdapter) convertSpansToRecords() {
	traces := l.traceData.ReceivedBatches
	spanCount := traces.SpanCount()
	l.spanCache = make([]store.SpanRecord, 0, spanCount)

	resourceSpans := traces.ResourceSpans()
	for i := 0; i < resourceSpans.Len(); i++ {
		rs := resourceSpans.At(i)
		scopeSpans := rs.ScopeSpans()

		for j := 0; j < scopeSpans.Len(); j++ {
			ss := scopeSpans.At(j)
			spans := ss.Spans()

			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)

				// Convert ptrace.Span to store.SpanRecord
				spanRecord := store.SpanRecord{
					TraceID:       l.traceID,
					SpanID:        span.SpanID(),
					ParentSpanID:  span.ParentSpanID(),
					Name:          span.Name(),
					Kind:          int8(span.Kind()),
					StartUnixNano: uint64(span.StartTimestamp()),
					EndUnixNano:   uint64(span.EndTimestamp()),
					StatusCode:    int8(span.Status().Code()),
					Attributes:    convertAttributes(span.Attributes()),
				}

				l.spanCache = append(l.spanCache, spanRecord)
			}
		}
	}
}

// convertAttributes converts pcommon.Map to map[string]any for SpanRecord
func convertAttributes(attrs pcommon.Map) map[string]any {
	result := make(map[string]any, attrs.Len())

	attrs.Range(func(k string, v pcommon.Value) bool {
		switch v.Type() {
		case pcommon.ValueTypeStr:
			result[k] = v.Str()
		case pcommon.ValueTypeBool:
			result[k] = v.Bool()
		case pcommon.ValueTypeInt:
			result[k] = v.Int()
		case pcommon.ValueTypeDouble:
			result[k] = v.Double()
		case pcommon.ValueTypeBytes:
			result[k] = v.Bytes().AsRaw()
		default:
			// For complex types (maps, slices), convert to string representation
			result[k] = v.AsString()
		}
		return true
	})

	return result
}

// policy combines a sampling policy evaluator with the destinations to be
// used for that policy.
type policy struct {
	// name used to identify this policy instance.
	name string
	// evaluator that decides if a trace is sampled or not by this policy instance.
	evaluator sampling.PolicyEvaluator
	// attribute to use in the telemetry to denote the policy.
	attribute metric.MeasurementOption
}

// tailSamplingSpanProcessor handles the incoming trace data and uses the given sampling
// policy to sample traces.
type tailSamplingSpanProcessor struct {
	ctx context.Context

	set       processor.Settings
	telemetry *metadata.TelemetryBuilder
	logger    *zap.Logger

	nextConsumer       consumer.Traces
	maxNumTraces       uint64
	policies           []*policy
	idToTrace          sync.Map
	policyTicker       timeutils.TTicker
	tickerFrequency    time.Duration
	decisionBatcher    idbatcher.Batcher
	sampledIDCache     cache.Cache[bool]
	nonSampledIDCache  cache.Cache[bool]
	traceLimiter       traceLimiter
	numTracesOnMap     *atomic.Uint64
	recordPolicy       bool
	setPolicyMux       sync.Mutex
	pendingPolicy      []PolicyCfg
	sampleOnFirstMatch bool
}

type traceLimiter interface {
	AcceptTrace(context.Context, pcommon.TraceID, time.Time)
	OnDeleteTrace()
}

// spanAndScope a structure for holding information about span and its instrumentation scope.
// required for preserving the instrumentation library information while sampling.
// We use pointers there to fast find the span in the map.
type spanAndScope struct {
	span                 *ptrace.Span
	instrumentationScope *pcommon.InstrumentationScope
}

var (
	attrDecisionSampled    = metric.WithAttributes(attribute.String("sampled", "true"), attribute.String("decision", "sampled"))
	attrDecisionNotSampled = metric.WithAttributes(attribute.String("sampled", "false"), attribute.String("decision", "not_sampled"))
	attrDecisionDropped    = metric.WithAttributes(attribute.String("sampled", "false"), attribute.String("decision", "dropped"))
	decisionToAttributes   = map[sampling.Decision]metric.MeasurementOption{
		sampling.Sampled:          attrDecisionSampled,
		sampling.NotSampled:       attrDecisionNotSampled,
		sampling.InvertNotSampled: attrDecisionNotSampled,
		sampling.InvertSampled:    attrDecisionSampled,
		sampling.Dropped:          attrDecisionDropped,
	}

	attrSampledTrue  = metric.WithAttributes(attribute.String("sampled", "true"))
	attrSampledFalse = metric.WithAttributes(attribute.String("sampled", "false"))
)

type Option func(*tailSamplingSpanProcessor)

// newTracesProcessor returns a processor.TracesProcessor that will perform tail sampling according to the given
// configuration.
func newTracesProcessor(ctx context.Context, set processor.Settings, nextConsumer consumer.Traces, cfg Config) (processor.Traces, error) {
	telemetrySettings := set.TelemetrySettings
	telemetry, err := metadata.NewTelemetryBuilder(telemetrySettings)
	if err != nil {
		return nil, err
	}
	nopCache := cache.NewNopDecisionCache[bool]()
	sampledDecisions := nopCache
	nonSampledDecisions := nopCache
	if cfg.DecisionCache.SampledCacheSize > 0 {
		sampledDecisions, err = cache.NewLRUDecisionCache[bool](cfg.DecisionCache.SampledCacheSize)
		if err != nil {
			return nil, err
		}
	}
	if cfg.DecisionCache.NonSampledCacheSize > 0 {
		nonSampledDecisions, err = cache.NewLRUDecisionCache[bool](cfg.DecisionCache.NonSampledCacheSize)
		if err != nil {
			return nil, err
		}
	}

	tsp := &tailSamplingSpanProcessor{
		ctx:                ctx,
		set:                set,
		telemetry:          telemetry,
		nextConsumer:       nextConsumer,
		maxNumTraces:       cfg.NumTraces,
		sampledIDCache:     sampledDecisions,
		nonSampledIDCache:  nonSampledDecisions,
		logger:             telemetrySettings.Logger,
		numTracesOnMap:     &atomic.Uint64{},
		sampleOnFirstMatch: cfg.SampleOnFirstMatch,
	}
	tsp.policyTicker = &timeutils.PolicyTicker{OnTickFunc: tsp.samplingPolicyOnTick}

	var tl traceLimiter
	if cfg.BlockOnOverflow {
		tl = tracelimiter.NewBlockingTracesLimiter(cfg.NumTraces)
	} else {
		tl = tracelimiter.NewDropOldTracesLimiter(cfg.NumTraces, tsp.dropTrace)
	}
	tsp.traceLimiter = tl

	for _, opt := range cfg.Options {
		opt(tsp)
	}

	if tsp.tickerFrequency == 0 {
		tsp.tickerFrequency = time.Second
	}

	if tsp.policies == nil {
		err := tsp.loadSamplingPolicy(cfg.PolicyCfgs)
		if err != nil {
			return nil, err
		}
	}

	if tsp.decisionBatcher == nil {
		// this will start a goroutine in the background, so we run it only if everything went
		// well in creating the policies
		numDecisionBatches := math.Max(1, cfg.DecisionWait.Seconds())
		inBatcher, err := idbatcher.New(uint64(numDecisionBatches), cfg.ExpectedNewTracesPerSec, uint64(2*runtime.NumCPU()))
		if err != nil {
			return nil, err
		}
		tsp.decisionBatcher = inBatcher
	}

	// Storage configuration is handled at a higher level (factory/config)
	// The processor always uses sync.Map for backward compatibility
	tsp.logger.Debug("Processor initialized with in-memory storage")

	return tsp, nil
}

// withDecisionBatcher sets the batcher used to batch trace IDs for policy evaluation.
func withDecisionBatcher(batcher idbatcher.Batcher) Option {
	return func(tsp *tailSamplingSpanProcessor) {
		tsp.decisionBatcher = batcher
	}
}

// withPolicies sets the sampling policies to be used by the processor.
func withPolicies(policies []*policy) Option {
	return func(tsp *tailSamplingSpanProcessor) {
		tsp.policies = policies
	}
}

// withTickerFrequency sets the frequency at which the processor will evaluate the sampling policies.
func withTickerFrequency(frequency time.Duration) Option {
	return func(tsp *tailSamplingSpanProcessor) {
		tsp.tickerFrequency = frequency
	}
}

// WithSampledDecisionCache sets the cache which the processor uses to store recently sampled trace IDs.
func WithSampledDecisionCache(c cache.Cache[bool]) Option {
	return func(tsp *tailSamplingSpanProcessor) {
		tsp.sampledIDCache = c
	}
}

// WithNonSampledDecisionCache sets the cache which the processor uses to store recently non-sampled trace IDs.
func WithNonSampledDecisionCache(c cache.Cache[bool]) Option {
	return func(tsp *tailSamplingSpanProcessor) {
		tsp.nonSampledIDCache = c
	}
}

func withRecordPolicy() Option {
	return func(tsp *tailSamplingSpanProcessor) {
		tsp.recordPolicy = true
	}
}



func getPolicyEvaluator(settings component.TelemetrySettings, cfg *PolicyCfg) (sampling.PolicyEvaluator, error) {
	switch cfg.Type {
	case Composite:
		return getNewCompositePolicy(settings, &cfg.CompositeCfg)
	case And:
		return getNewAndPolicy(settings, &cfg.AndCfg)
	case Drop:
		return getNewDropPolicy(settings, &cfg.DropCfg)
	default:
		return getSharedPolicyEvaluator(settings, &cfg.sharedPolicyCfg)
	}
}

func getSharedPolicyEvaluator(settings component.TelemetrySettings, cfg *sharedPolicyCfg) (sampling.PolicyEvaluator, error) {
	settings.Logger = settings.Logger.With(zap.Any("policy", cfg.Type))

	switch cfg.Type {
	case AlwaysSample:
		return sampling.NewAlwaysSample(settings), nil
	case Latency:
		lfCfg := cfg.LatencyCfg
		return sampling.NewLatency(settings, lfCfg.ThresholdMs, lfCfg.UpperThresholdMs), nil
	case NumericAttribute:
		nafCfg := cfg.NumericAttributeCfg
		var minValuePtr, maxValuePtr *int64
		if nafCfg.MinValue != 0 {
			minValuePtr = &nafCfg.MinValue
		}
		if nafCfg.MaxValue != 0 {
			maxValuePtr = &nafCfg.MaxValue
		}
		return sampling.NewNumericAttributeFilter(settings, nafCfg.Key, minValuePtr, maxValuePtr, nafCfg.InvertMatch), nil
	case Probabilistic:
		pCfg := cfg.ProbabilisticCfg
		return sampling.NewProbabilisticSampler(settings, pCfg.HashSalt, pCfg.SamplingPercentage), nil
	case StringAttribute:
		safCfg := cfg.StringAttributeCfg
		return sampling.NewStringAttributeFilter(settings, safCfg.Key, safCfg.Values, safCfg.EnabledRegexMatching, safCfg.CacheMaxSize, safCfg.InvertMatch), nil
	case StatusCode:
		scfCfg := cfg.StatusCodeCfg
		return sampling.NewStatusCodeFilter(settings, scfCfg.StatusCodes)
	case RateLimiting:
		rlfCfg := cfg.RateLimitingCfg
		return sampling.NewRateLimiting(settings, rlfCfg.SpansPerSecond), nil
	case SpanCount:
		spCfg := cfg.SpanCountCfg
		return sampling.NewSpanCount(settings, spCfg.MinSpans, spCfg.MaxSpans), nil
	case TraceState:
		tsfCfg := cfg.TraceStateCfg
		return sampling.NewTraceStateFilter(settings, tsfCfg.Key, tsfCfg.Values), nil
	case BooleanAttribute:
		bafCfg := cfg.BooleanAttributeCfg
		return sampling.NewBooleanAttributeFilter(settings, bafCfg.Key, bafCfg.Value, bafCfg.InvertMatch), nil
	case OTTLCondition:
		ottlfCfg := cfg.OTTLConditionCfg
		return sampling.NewOTTLConditionFilter(settings, ottlfCfg.SpanConditions, ottlfCfg.SpanEventConditions, ottlfCfg.ErrorMode)

	default:
		return nil, fmt.Errorf("unknown sampling policy type %s", cfg.Type)
	}
}

type policyDecisionMetrics struct {
	tracesSampled int
	spansSampled  int64
}

type policyMetrics struct {
	idNotFoundOnMapCount, evaluateErrorCount, decisionSampled, decisionNotSampled, decisionDropped int64
	tracesSampledByPolicyDecision                                                                  []map[sampling.Decision]policyDecisionMetrics
}

func newPolicyMetrics(numPolicies int) *policyMetrics {
	tracesSampledByPolicyDecision := make([]map[sampling.Decision]policyDecisionMetrics, numPolicies)
	for i := range tracesSampledByPolicyDecision {
		tracesSampledByPolicyDecision[i] = make(map[sampling.Decision]policyDecisionMetrics)
	}
	return &policyMetrics{
		tracesSampledByPolicyDecision: tracesSampledByPolicyDecision,
	}
}

func (m *policyMetrics) addDecision(policyIndex int, decision sampling.Decision, spansSampled int64) {
	stats := m.tracesSampledByPolicyDecision[policyIndex][decision]
	stats.tracesSampled++
	stats.spansSampled += spansSampled
	m.tracesSampledByPolicyDecision[policyIndex][decision] = stats
}

func (tsp *tailSamplingSpanProcessor) loadSamplingPolicy(cfgs []PolicyCfg) error {
	telemetrySettings := tsp.set.TelemetrySettings
	componentID := tsp.set.ID.Name()

	cLen := len(cfgs)
	policies := make([]*policy, 0, cLen)
	dropPolicies := make([]*policy, 0, cLen)
	policyNames := make(map[string]struct{}, cLen)

	for _, cfg := range cfgs {
		if cfg.Name == "" {
			return errors.New("policy name cannot be empty")
		}

		if _, exists := policyNames[cfg.Name]; exists {
			return fmt.Errorf("duplicate policy name %q", cfg.Name)
		}
		policyNames[cfg.Name] = struct{}{}

		eval, err := getPolicyEvaluator(telemetrySettings, &cfg)
		if err != nil {
			return fmt.Errorf("failed to create policy evaluator for %q: %w", cfg.Name, err)
		}

		uniquePolicyName := cfg.Name
		if componentID != "" {
			uniquePolicyName = fmt.Sprintf("%s.%s", componentID, cfg.Name)
		}

		p := &policy{
			name:      cfg.Name,
			evaluator: eval,
			attribute: metric.WithAttributes(attribute.String("policy", uniquePolicyName)),
		}

		if cfg.Type == Drop {
			dropPolicies = append(dropPolicies, p)
		} else {
			policies = append(policies, p)
		}
	}
	// Dropped decision takes precedence over all others, therefore we evaluate them first.
	tsp.policies = slices.Concat(dropPolicies, policies)

	tsp.logger.Debug("Loaded sampling policy", zap.Int("policies.len", len(policies)))

	return nil
}

func (tsp *tailSamplingSpanProcessor) SetSamplingPolicy(cfgs []PolicyCfg) {
	tsp.logger.Debug("Setting pending sampling policy", zap.Int("pending.len", len(cfgs)))

	tsp.setPolicyMux.Lock()
	defer tsp.setPolicyMux.Unlock()

	tsp.pendingPolicy = cfgs
}

func (tsp *tailSamplingSpanProcessor) loadPendingSamplingPolicy() {
	tsp.setPolicyMux.Lock()
	defer tsp.setPolicyMux.Unlock()

	// Nothing pending, do nothing.
	pLen := len(tsp.pendingPolicy)
	if pLen == 0 {
		return
	}

	tsp.logger.Debug("Loading pending sampling policy", zap.Int("pending.len", pLen))

	err := tsp.loadSamplingPolicy(tsp.pendingPolicy)

	// Empty pending regardless of error. If policy is invalid, it will fail on
	// every tick, no need to do extra work and flood the log with errors.
	tsp.pendingPolicy = nil

	if err != nil {
		tsp.logger.Error("Failed to load pending sampling policy", zap.Error(err))
		tsp.logger.Debug("Continuing to use the previously loaded sampling policy")
	}
}

func (tsp *tailSamplingSpanProcessor) samplingPolicyOnTick() {
	tsp.logger.Debug("Sampling Policy Evaluation ticked")

	tsp.loadPendingSamplingPolicy()

	ctx := context.Background()
	metrics := newPolicyMetrics(len(tsp.policies))
	startTime := time.Now()

	batch, _ := tsp.decisionBatcher.CloseCurrentAndTakeFirstBatch()
	batchLen := len(batch)

	for _, id := range batch {
		decodedSpans, meta, found, err := tsp.fetchTraceForEvaluation(id)
		if err != nil {
			tsp.logger.Error("Failed to fetch trace for evaluation", zap.Error(err))
			metrics.idNotFoundOnMapCount++
			continue
		}
		if !found {
			metrics.idNotFoundOnMapCount++
			continue
		}

		// Set decision time on metadata
		decisionTime := time.Now()

		decision, traceData := tsp.makeDecisionFromSpans(id, decodedSpans, meta, metrics)

		tsp.telemetry.ProcessorTailSamplingGlobalCountTracesSampled.Add(tsp.ctx, 1, decisionToAttributes[decision])

		// Handle span extraction and decision persistence based on storage path
		err = tsp.handleDecisionResult(ctx, id, decision, decisionTime, traceData)
		if err != nil {
			tsp.logger.Error("Failed to handle decision result", zap.Error(err))
		}
	}

	tsp.telemetry.ProcessorTailSamplingSamplingDecisionTimerLatency.Record(tsp.ctx, int64(time.Since(startTime)/time.Millisecond))
	tsp.telemetry.ProcessorTailSamplingSamplingTracesOnMemory.Record(tsp.ctx, int64(tsp.numTracesOnMap.Load()))
	tsp.telemetry.ProcessorTailSamplingSamplingTraceDroppedTooEarly.Add(tsp.ctx, metrics.idNotFoundOnMapCount)
	tsp.telemetry.ProcessorTailSamplingSamplingPolicyEvaluationError.Add(tsp.ctx, metrics.evaluateErrorCount)

	for i, p := range tsp.policies {
		for decision, stats := range metrics.tracesSampledByPolicyDecision[i] {
			tsp.telemetry.ProcessorTailSamplingCountTracesSampled.Add(tsp.ctx, int64(stats.tracesSampled), p.attribute, decisionToAttributes[decision])
			if telemetry.IsMetricStatCountSpansSampledEnabled() {
				tsp.telemetry.ProcessorTailSamplingCountSpansSampled.Add(tsp.ctx, stats.spansSampled, p.attribute, decisionToAttributes[decision])
			}
		}
	}

	tsp.logger.Debug("Sampling policy evaluation completed",
		zap.Int("batch.len", batchLen),
		zap.Int64("sampled", metrics.decisionSampled),
		zap.Int64("notSampled", metrics.decisionNotSampled),
		zap.Int64("dropped", metrics.decisionDropped),
		zap.Int64("droppedPriorToEvaluation", metrics.idNotFoundOnMapCount),
		zap.Int64("policyEvaluationErrors", metrics.evaluateErrorCount),
	)
}

// makeDecisionFromSpans evaluates policies using DecodedSpans interface with streaming support
func (tsp *tailSamplingSpanProcessor) makeDecisionFromSpans(id pcommon.TraceID, decodedSpans store.DecodedSpans, meta store.Meta, metrics *policyMetrics) (sampling.Decision, *sampling.TraceData) {
	finalDecision := sampling.NotSampled
	samplingDecisions := map[sampling.Decision]*policy{
		sampling.Error:            nil,
		sampling.Sampled:          nil,
		sampling.NotSampled:       nil,
		sampling.InvertSampled:    nil,
		sampling.InvertNotSampled: nil,
		sampling.Dropped:          nil,
	}

	ctx := context.Background()
	startTime := time.Now()

	// Check if trace is large and requires streaming evaluation
	if meta.SpanCount > tsp.getStreamingThreshold() {
		return tsp.evaluatePoliciesWithStreaming(ctx, id, decodedSpans, meta, metrics, startTime)
	}

	// For smaller traces, use existing evaluation path
	traceData := tsp.convertToTraceDataForEvaluation(decodedSpans, meta)

	// Check all policies before making a final decision.
	for i, p := range tsp.policies {
		decision, err := p.evaluator.Evaluate(ctx, id, traceData)
		latency := time.Since(startTime)
		tsp.telemetry.ProcessorTailSamplingSamplingDecisionLatency.Record(ctx, int64(latency/time.Microsecond), p.attribute)

		if err != nil {
			if samplingDecisions[sampling.Error] == nil {
				samplingDecisions[sampling.Error] = p
			}
			metrics.evaluateErrorCount++
			tsp.logger.Debug("Sampling policy error", zap.Error(err))
			continue
		}

		metrics.addDecision(i, decision, int64(meta.SpanCount))

		// We associate the first policy with the sampling decision to understand what policy sampled a span
		if samplingDecisions[decision] == nil {
			samplingDecisions[decision] = p
		}

		// Break early if dropped. This can drastically reduce tick/decision latency.
		if decision == sampling.Dropped {
			break
		}
		// If sampleOnFirstMatch is enabled, make decision as soon as a policy matches
		if tsp.sampleOnFirstMatch && decision == sampling.Sampled {
			break
		}
	}

	var sampledPolicy *policy

	switch {
	case samplingDecisions[sampling.Dropped] != nil: // Dropped takes precedence
		finalDecision = sampling.Dropped
	case samplingDecisions[sampling.InvertNotSampled] != nil: // Then InvertNotSampled
		finalDecision = sampling.NotSampled
	case samplingDecisions[sampling.Sampled] != nil:
		finalDecision = sampling.Sampled
		sampledPolicy = samplingDecisions[sampling.Sampled]
	case samplingDecisions[sampling.InvertSampled] != nil && samplingDecisions[sampling.NotSampled] == nil:
		finalDecision = sampling.Sampled
		sampledPolicy = samplingDecisions[sampling.InvertSampled]
	}

	if tsp.recordPolicy && sampledPolicy != nil {
		sampling.SetAttrOnScopeSpans(traceData, "tailsampling.policy", sampledPolicy.name)
	}

	switch finalDecision {
	case sampling.Sampled:
		metrics.decisionSampled++
	case sampling.NotSampled:
		metrics.decisionNotSampled++
	case sampling.Dropped:
		metrics.decisionDropped++
	}

	return finalDecision, traceData
}

// convertToTraceDataForEvaluation converts DecodedSpans to sampling.TraceData for existing policy evaluators
func (tsp *tailSamplingSpanProcessor) convertToTraceDataForEvaluation(decodedSpans store.DecodedSpans, meta store.Meta) *sampling.TraceData {
	// Check if this is the legacy adapter - if so, extract the original traces directly
	if legacyAdapter, ok := decodedSpans.(*legacyTraceDataAdapter); ok {
		// For legacy path, return the original TraceData to preserve resource/scope information
		return legacyAdapter.traceData
	}

	// For TraceBufferStore path, create ptrace.Traces from DecodedSpans
	traces := ptrace.NewTraces()
	spanMap := make(map[string]*ptrace.Span) // Track spans to avoid duplicates

	// Group spans by their structure for efficient organization
	resourceSpans := traces.ResourceSpans().AppendEmpty()
	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty()

	// Convert each SpanRecord back to ptrace.Span format
	for i := 0; i < decodedSpans.Len(); i++ {
		spanRecord := decodedSpans.At(i)

		// Skip duplicate spans (though shouldn't happen with proper deduplication)
		spanKey := fmt.Sprintf("%x", spanRecord.SpanID)
		if _, exists := spanMap[spanKey]; exists {
			continue
		}

		// Create new span in the traces structure
		span := scopeSpans.Spans().AppendEmpty()

		// Convert SpanRecord fields to ptrace.Span
		span.SetTraceID(pcommon.TraceID(spanRecord.TraceID))
		span.SetSpanID(pcommon.SpanID(spanRecord.SpanID))
		span.SetParentSpanID(pcommon.SpanID(spanRecord.ParentSpanID))
		span.SetName(spanRecord.Name)
		span.SetKind(ptrace.SpanKind(spanRecord.Kind))
		span.SetStartTimestamp(pcommon.Timestamp(spanRecord.StartUnixNano))
		span.SetEndTimestamp(pcommon.Timestamp(spanRecord.EndUnixNano))

		// Set status
		status := span.Status()
		status.SetCode(ptrace.StatusCode(spanRecord.StatusCode))

		// Convert attributes
		attrs := span.Attributes()
		for key, value := range spanRecord.Attributes {
			switch v := value.(type) {
			case string:
				attrs.PutStr(key, v)
			case bool:
				attrs.PutBool(key, v)
			case int64:
				attrs.PutInt(key, v)
			case float64:
				attrs.PutDouble(key, v)
			default:
				// Convert other types to string
				attrs.PutStr(key, fmt.Sprintf("%v", v))
			}
		}

		spanMap[spanKey] = &span
	}

	// Create TraceData structure compatible with existing policy evaluators
	spanCount := &atomic.Int64{}
	spanCount.Store(int64(meta.SpanCount))

	traceData := &sampling.TraceData{
		ArrivalTime:     meta.FirstSeen,
		SpanCount:       spanCount,
		ReceivedBatches: traces,
		FinalDecision:   sampling.Unspecified, // Not decided yet
	}

	return traceData
}

// handleDecisionResult processes the decision and handles span extraction/persistence
func (tsp *tailSamplingSpanProcessor) handleDecisionResult(ctx context.Context, id pcommon.TraceID, decision sampling.Decision, decisionTime time.Time, traceData *sampling.TraceData) error {
	// Persist decision using integrated decision persistence
	err := tsp.persistDecision(ctx, id, decision)
	if err != nil {
		return fmt.Errorf("failed to persist decision: %w", err)
	}

	// Update TraceData in sync.Map
	d, ok := tsp.idToTrace.Load(id)
	if ok {
		existingTraceData := d.(*sampling.TraceData)
		existingTraceData.Lock()
		existingTraceData.FinalDecision = decision
		existingTraceData.DecisionTime = decisionTime
		existingTraceData.Unlock()
	}

	// Handle span extraction for sampled traces
	if decision == sampling.Sampled {
		// Use the TraceData that has policy attributes set during evaluation
		// This preserves any policy attributes added by SetAttrOnScopeSpans()
		tsp.releaseSampledTrace(ctx, id, traceData.ReceivedBatches)
	} else {
		// For not sampled traces, just mark for cleanup
		tsp.releaseNotSampledTrace(id)
	}

	return nil
}

// Legacy makeDecision method for compatibility - delegates to new method
func (tsp *tailSamplingSpanProcessor) makeDecision(id pcommon.TraceID, trace *sampling.TraceData, metrics *policyMetrics) sampling.Decision {
	finalDecision := sampling.NotSampled
	samplingDecisions := map[sampling.Decision]*policy{
		sampling.Error:            nil,
		sampling.Sampled:          nil,
		sampling.NotSampled:       nil,
		sampling.InvertSampled:    nil,
		sampling.InvertNotSampled: nil,
		sampling.Dropped:          nil,
	}

	ctx := context.Background()
	startTime := time.Now()

	// Check all policies before making a final decision.
	for i, p := range tsp.policies {
		decision, err := p.evaluator.Evaluate(ctx, id, trace)
		latency := time.Since(startTime)
		tsp.telemetry.ProcessorTailSamplingSamplingDecisionLatency.Record(ctx, int64(latency/time.Microsecond), p.attribute)

		if err != nil {
			if samplingDecisions[sampling.Error] == nil {
				samplingDecisions[sampling.Error] = p
			}
			metrics.evaluateErrorCount++
			tsp.logger.Debug("Sampling policy error", zap.Error(err))
			continue
		}

		metrics.addDecision(i, decision, trace.SpanCount.Load())

		// We associate the first policy with the sampling decision to understand what policy sampled a span
		if samplingDecisions[decision] == nil {
			samplingDecisions[decision] = p
		}

		// Break early if dropped. This can drastically reduce tick/decision latency.
		if decision == sampling.Dropped {
			break
		}
		// If sampleOnFirstMatch is enabled, make decision as soon as a policy matches
		if tsp.sampleOnFirstMatch && decision == sampling.Sampled {
			break
		}
	}

	var sampledPolicy *policy

	switch {
	case samplingDecisions[sampling.Dropped] != nil: // Dropped takes precedence
		finalDecision = sampling.Dropped
	case samplingDecisions[sampling.InvertNotSampled] != nil: // Then InvertNotSampled
		finalDecision = sampling.NotSampled
	case samplingDecisions[sampling.Sampled] != nil:
		finalDecision = sampling.Sampled
		sampledPolicy = samplingDecisions[sampling.Sampled]
	case samplingDecisions[sampling.InvertSampled] != nil && samplingDecisions[sampling.NotSampled] == nil:
		finalDecision = sampling.Sampled
		sampledPolicy = samplingDecisions[sampling.InvertSampled]
	}

	if tsp.recordPolicy && sampledPolicy != nil {
		sampling.SetAttrOnScopeSpans(trace, "tailsampling.policy", sampledPolicy.name)
	}

	switch finalDecision {
	case sampling.Sampled:
		metrics.decisionSampled++
	case sampling.NotSampled:
		metrics.decisionNotSampled++
	case sampling.Dropped:
		metrics.decisionDropped++
	}

	return finalDecision
}

// ConsumeTraces is required by the processor.Traces interface.
func (tsp *tailSamplingSpanProcessor) ConsumeTraces(_ context.Context, td ptrace.Traces) error {
	resourceSpans := td.ResourceSpans()
	for i := 0; i < resourceSpans.Len(); i++ {
		tsp.processTraces(resourceSpans.At(i))
	}
	return nil
}

func (*tailSamplingSpanProcessor) groupSpansByTraceKey(resourceSpans ptrace.ResourceSpans) map[pcommon.TraceID][]spanAndScope {
	idToSpans := make(map[pcommon.TraceID][]spanAndScope)
	ilss := resourceSpans.ScopeSpans()
	for j := 0; j < ilss.Len(); j++ {
		scope := ilss.At(j)
		spans := scope.Spans()
		is := scope.Scope()
		spansLen := spans.Len()
		for k := 0; k < spansLen; k++ {
			span := spans.At(k)
			key := span.TraceID()
			idToSpans[key] = append(idToSpans[key], spanAndScope{
				span:                 &span,
				instrumentationScope: &is,
			})
		}
	}
	return idToSpans
}

func (tsp *tailSamplingSpanProcessor) processTraces(resourceSpans ptrace.ResourceSpans) {
	currTime := time.Now()

	// Group spans per their traceId to minimize contention on trace storage
	idToSpansAndScope := tsp.groupSpansByTraceKey(resourceSpans)
	var newTraceIDs int64
	for id, spans := range idToSpansAndScope {
		// Check for cached decisions using storage-aware decision lookup
		decision, isCached, err := tsp.getCachedDecision(id)
		if err != nil {
			tsp.logger.Error("Failed to check cached decision", zap.Error(err), zap.Stringer("id", id))
			// Continue processing without cache optimization
		} else if isCached {
			if decision == sampling.Sampled {
				tsp.logger.Debug("Trace ID has cached sampled decision", zap.Stringer("id", id))
				traceTd := ptrace.NewTraces()
				appendToTraces(traceTd, resourceSpans, spans)
				tsp.releaseSampledTrace(tsp.ctx, id, traceTd)
				tsp.telemetry.ProcessorTailSamplingEarlyReleasesFromCacheDecision.
					Add(tsp.ctx, int64(len(spans)), attrSampledTrue)
			} else {
				tsp.logger.Debug("Trace ID has cached not-sampled decision", zap.Stringer("id", id))
				tsp.telemetry.ProcessorTailSamplingEarlyReleasesFromCacheDecision.
					Add(tsp.ctx, int64(len(spans)), attrSampledFalse)
			}
			continue
		}

		lenSpans := int64(len(spans))

		// Use sync.Map storage
		d, loaded := tsp.loadTraceData(id)
		if !loaded {
			spanCount := &atomic.Int64{}
			spanCount.Store(lenSpans)

			td := &sampling.TraceData{
				ArrivalTime:     currTime,
				SpanCount:       spanCount,
				ReceivedBatches: ptrace.NewTraces(),
			}

			if d, loaded = tsp.loadOrStoreTraceData(id, td); !loaded {
				newTraceIDs++
				tsp.decisionBatcher.AddToCurrentBatch(id)
				tsp.numTracesOnMap.Add(1)
				tsp.traceLimiter.AcceptTrace(tsp.ctx, id, currTime)
			}
		}

		actualData := d.(*sampling.TraceData)
		if loaded {
			actualData.SpanCount.Add(lenSpans)
		}

		actualData.Lock()
		finalDecision := actualData.FinalDecision

		if finalDecision == sampling.Unspecified {
			// If the final decision hasn't been made, add the new spans under the lock.
			appendToTraces(actualData.ReceivedBatches, resourceSpans, spans)
			actualData.Unlock()
			continue
		}

		actualData.Unlock()

		switch finalDecision {
		case sampling.Sampled:
			traceTd := ptrace.NewTraces()
			appendToTraces(traceTd, resourceSpans, spans)
			tsp.releaseSampledTrace(tsp.ctx, id, traceTd)
		case sampling.NotSampled:
			tsp.releaseNotSampledTrace(id)
		default:
			tsp.logger.Warn("Unexpected sampling decision", zap.Int("decision", int(finalDecision)))
		}

		if !actualData.DecisionTime.IsZero() {
			tsp.telemetry.ProcessorTailSamplingSamplingLateSpanAge.Record(tsp.ctx, int64(time.Since(actualData.DecisionTime)/time.Second))
		}
	}

	tsp.telemetry.ProcessorTailSamplingNewTraceIDReceived.Add(tsp.ctx, newTraceIDs)
}

func (*tailSamplingSpanProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// Start is invoked during service startup.
func (tsp *tailSamplingSpanProcessor) Start(context.Context, component.Host) error {
	tsp.policyTicker.Start(tsp.tickerFrequency)
	return nil
}

// Shutdown is invoked during service shutdown.
func (tsp *tailSamplingSpanProcessor) Shutdown(context.Context) error {
	tsp.decisionBatcher.Stop()
	tsp.policyTicker.Stop()
	return nil
}

func (tsp *tailSamplingSpanProcessor) dropTrace(traceID pcommon.TraceID, deletionTime time.Time) {
	var trace *sampling.TraceData
	if d, ok := tsp.loadTraceData(traceID); ok {
		trace = d.(*sampling.TraceData)
		tsp.deleteTraceData(traceID)
		// Subtract one from numTracesOnMap per https://godoc.org/sync/atomic#AddUint64
		tsp.numTracesOnMap.Add(^uint64(0))
		tsp.traceLimiter.OnDeleteTrace()
	}
	if trace == nil {
		tsp.logger.Debug("Attempt to delete trace ID not on table", zap.Stringer("id", traceID))
		return
	}

	tsp.telemetry.ProcessorTailSamplingSamplingTraceRemovalAge.Record(tsp.ctx, int64(deletionTime.Sub(trace.ArrivalTime)/time.Second))
}

// releaseSampledTrace sends the trace data to the next consumer and cleans up the trace.
// For early cache hits, it ensures decision is cached. For post-evaluation, caching is handled by persistDecision().
func (tsp *tailSamplingSpanProcessor) releaseSampledTrace(ctx context.Context, id pcommon.TraceID, td ptrace.Traces) {
	// Send traces to downstream consumer
	if err := tsp.nextConsumer.ConsumeTraces(ctx, td); err != nil {
		tsp.logger.Warn(
			"Error sending spans to destination",
			zap.Error(err))
	}

	// Ensure decision is cached (idempotent if already cached by persistDecision)
	tsp.sampledIDCache.Put(id, true)

	// Only clean up trace data if the cache actually stored the decision (original logic)
	// For no-op caches, Get() always returns false, so dropTrace() won't be called
	_, ok := tsp.sampledIDCache.Get(id)
	if ok {
		tsp.dropTrace(id, time.Now())
	}
}

// releaseNotSampledTrace cleans up not sampled traces.
// For early cache hits, it ensures decision is cached. For post-evaluation, caching is handled by persistDecision().
func (tsp *tailSamplingSpanProcessor) releaseNotSampledTrace(id pcommon.TraceID) {
	// Ensure decision is cached (idempotent if already cached by persistDecision)
	tsp.nonSampledIDCache.Put(id, true)

	// Only clean up trace data if the cache actually stored the decision (original logic)
	// For no-op caches, Get() always returns false, so dropTrace() won't be called
	_, ok := tsp.nonSampledIDCache.Get(id)
	if ok {
		tsp.dropTrace(id, time.Now())
	}
}

func appendToTraces(dest ptrace.Traces, rss ptrace.ResourceSpans, spanAndScopes []spanAndScope) {
	rs := dest.ResourceSpans().AppendEmpty()
	rss.Resource().CopyTo(rs.Resource())

	scopePointerToNewScope := make(map[*pcommon.InstrumentationScope]*ptrace.ScopeSpans)
	for _, spanAndScope := range spanAndScopes {
		// If the scope of the spanAndScope is not in the map, add it to the map and the destination.
		if scope, ok := scopePointerToNewScope[spanAndScope.instrumentationScope]; !ok {
			is := rs.ScopeSpans().AppendEmpty()
			spanAndScope.instrumentationScope.CopyTo(is.Scope())
			scopePointerToNewScope[spanAndScope.instrumentationScope] = &is

			sp := is.Spans().AppendEmpty()
			spanAndScope.span.CopyTo(sp)
		} else {
			sp := scope.Spans().AppendEmpty()
			spanAndScope.span.CopyTo(sp)
		}
	}
}

// Decision caching methods that integrate TraceBufferStore with legacy caches
// These methods provide a clean interface between decision persistence and caching.

// getCachedDecision checks for cached decisions
func (tsp *tailSamplingSpanProcessor) getCachedDecision(traceID pcommon.TraceID) (sampling.Decision, bool, error) {
	if _, ok := tsp.sampledIDCache.Get(traceID); ok {
		return sampling.Sampled, true, nil
	}
	if _, ok := tsp.nonSampledIDCache.Get(traceID); ok {
		return sampling.NotSampled, true, nil
	}
	return sampling.NotSampled, false, nil
}

// persistDecision stores decisions in caches
func (tsp *tailSamplingSpanProcessor) persistDecision(ctx context.Context, traceID pcommon.TraceID, decision sampling.Decision) error {
	switch decision {
	case sampling.Sampled:
		tsp.sampledIDCache.Put(traceID, true)
	case sampling.NotSampled, sampling.Dropped:
		tsp.nonSampledIDCache.Put(traceID, true)
	}
	return nil
}

// Storage methods using sync.Map

// loadTraceData loads trace data from sync.Map
func (tsp *tailSamplingSpanProcessor) loadTraceData(traceID pcommon.TraceID) (any, bool) {
	return tsp.idToTrace.Load(traceID)
}

// loadOrStoreTraceData loads or stores trace data in sync.Map
func (tsp *tailSamplingSpanProcessor) loadOrStoreTraceData(traceID pcommon.TraceID, data *sampling.TraceData) (any, bool) {
	return tsp.idToTrace.LoadOrStore(traceID, data)
}

// deleteTraceData deletes trace data from sync.Map
func (tsp *tailSamplingSpanProcessor) deleteTraceData(traceID pcommon.TraceID) {
	tsp.idToTrace.Delete(traceID)
}


// fetchTraceForEvaluation retrieves trace data for policy evaluation
// getStreamingThreshold returns the span count threshold above which streaming evaluation is used
func (tsp *tailSamplingSpanProcessor) getStreamingThreshold() int {
	// Default threshold: 10,000 spans
	// This prevents loading very large traces entirely into memory
	return 10000
}

// getStreamingBatchSize returns the batch size for streaming evaluation
func (tsp *tailSamplingSpanProcessor) getStreamingBatchSize() int {
	// Process spans in batches of 1000 to limit memory usage
	return 1000
}

// evaluatePoliciesWithStreaming performs policy evaluation for large traces using streaming approach
func (tsp *tailSamplingSpanProcessor) evaluatePoliciesWithStreaming(ctx context.Context, id pcommon.TraceID, decodedSpans store.DecodedSpans, meta store.Meta, metrics *policyMetrics, startTime time.Time) (sampling.Decision, *sampling.TraceData) {
	finalDecision := sampling.NotSampled
	samplingDecisions := map[sampling.Decision]*policy{
		sampling.Error:            nil,
		sampling.Sampled:          nil,
		sampling.NotSampled:       nil,
		sampling.InvertSampled:    nil,
		sampling.InvertNotSampled: nil,
		sampling.Dropped:          nil,
	}

	// Create a streaming trace data structure that builds incrementally
	streamingTraceData := tsp.createStreamingTraceData(id, meta)
	batchSize := tsp.getStreamingBatchSize()
	spanIndex := 0

	// Process spans in batches to maintain bounded memory usage
	for spanIndex < decodedSpans.Len() {
		// Calculate batch end
		batchEnd := spanIndex + batchSize
		if batchEnd > decodedSpans.Len() {
			batchEnd = decodedSpans.Len()
		}

		// Add batch of spans to streaming trace data
		err := tsp.addSpanBatchToTraceData(streamingTraceData, decodedSpans, spanIndex, batchEnd)
		if err != nil {
			tsp.logger.Error("Failed to add span batch to trace data", zap.Error(err))
			metrics.evaluateErrorCount++
			return sampling.Error, streamingTraceData
		}

		// Evaluate policies with current batch (for policies that can make early decisions)
		for i, p := range tsp.policies {
			decision, err := p.evaluator.Evaluate(ctx, id, streamingTraceData)
			latency := time.Since(startTime)
			tsp.telemetry.ProcessorTailSamplingSamplingDecisionLatency.Record(ctx, int64(latency/time.Microsecond), p.attribute)

			if err != nil {
				if samplingDecisions[sampling.Error] == nil {
					samplingDecisions[sampling.Error] = p
				}
				metrics.evaluateErrorCount++
				tsp.logger.Debug("Streaming policy evaluation error", zap.Error(err))
				continue
			}

			metrics.addDecision(i, decision, int64(meta.SpanCount))

			// Track the first policy that made each decision type
			if samplingDecisions[decision] == nil {
				samplingDecisions[decision] = p
			}

			// Early termination for dropped traces
			if decision == sampling.Dropped {
				finalDecision = sampling.Dropped
				break
			}

			// Early termination for sampled traces (if enabled)
			if tsp.sampleOnFirstMatch && decision == sampling.Sampled {
				finalDecision = sampling.Sampled
				break
			}
		}

		// If we have a definitive decision (dropped or sampled with early exit), stop processing
		if finalDecision == sampling.Dropped || (tsp.sampleOnFirstMatch && finalDecision == sampling.Sampled) {
			break
		}

		spanIndex = batchEnd
	}

	// Determine final decision if not already set
	if finalDecision == sampling.NotSampled {
		switch {
		case samplingDecisions[sampling.Dropped] != nil:
			finalDecision = sampling.Dropped
		case samplingDecisions[sampling.InvertNotSampled] != nil:
			finalDecision = sampling.NotSampled
		case samplingDecisions[sampling.Sampled] != nil:
			finalDecision = sampling.Sampled
		case samplingDecisions[sampling.InvertSampled] != nil && samplingDecisions[sampling.NotSampled] == nil:
			finalDecision = sampling.Sampled
		}
	}

	// Apply policy attributes for sampled traces
	if tsp.recordPolicy && finalDecision == sampling.Sampled {
		var sampledPolicy *policy
		if samplingDecisions[sampling.Sampled] != nil {
			sampledPolicy = samplingDecisions[sampling.Sampled]
		} else if samplingDecisions[sampling.InvertSampled] != nil {
			sampledPolicy = samplingDecisions[sampling.InvertSampled]
		}
		if sampledPolicy != nil {
			sampling.SetAttrOnScopeSpans(streamingTraceData, "tailsampling.policy", sampledPolicy.name)
		}
	}

	// Update metrics
	switch finalDecision {
	case sampling.Sampled:
		metrics.decisionSampled++
	case sampling.NotSampled:
		metrics.decisionNotSampled++
	case sampling.Dropped:
		metrics.decisionDropped++
	}

	return finalDecision, streamingTraceData
}

// createStreamingTraceData creates a TraceData structure optimized for streaming evaluation
func (tsp *tailSamplingSpanProcessor) createStreamingTraceData(id pcommon.TraceID, meta store.Meta) *sampling.TraceData {
	spanCount := &atomic.Int64{}
	spanCount.Store(int64(meta.SpanCount))

	return &sampling.TraceData{
		ArrivalTime:     meta.FirstSeen,
		SpanCount:       spanCount,
		ReceivedBatches: ptrace.NewTraces(),
		FinalDecision:   sampling.Unspecified,
	}
}

// addSpanBatchToTraceData adds a batch of spans from DecodedSpans to TraceData for evaluation
func (tsp *tailSamplingSpanProcessor) addSpanBatchToTraceData(traceData *sampling.TraceData, decodedSpans store.DecodedSpans, startIdx, endIdx int) error {
	// Get or create resource spans
	traces := traceData.ReceivedBatches
	var resourceSpans ptrace.ResourceSpans
	if traces.ResourceSpans().Len() == 0 {
		resourceSpans = traces.ResourceSpans().AppendEmpty()
	} else {
		resourceSpans = traces.ResourceSpans().At(0)
	}

	// Get or create scope spans
	var scopeSpans ptrace.ScopeSpans
	if resourceSpans.ScopeSpans().Len() == 0 {
		scopeSpans = resourceSpans.ScopeSpans().AppendEmpty()
	} else {
		scopeSpans = resourceSpans.ScopeSpans().At(0)
	}

	// Add spans from the batch
	for i := startIdx; i < endIdx; i++ {
		spanRecord := decodedSpans.At(i)
		
		// Create ptrace.Span from SpanRecord
		span := scopeSpans.Spans().AppendEmpty()
		
		// Convert SpanRecord fields to ptrace.Span
		span.SetTraceID(pcommon.TraceID(spanRecord.TraceID))
		span.SetSpanID(pcommon.SpanID(spanRecord.SpanID))
		span.SetParentSpanID(pcommon.SpanID(spanRecord.ParentSpanID))
		span.SetName(spanRecord.Name)
		span.SetKind(ptrace.SpanKind(spanRecord.Kind))
		span.SetStartTimestamp(pcommon.Timestamp(spanRecord.StartUnixNano))
		span.SetEndTimestamp(pcommon.Timestamp(spanRecord.EndUnixNano))

		// Set status
		status := span.Status()
		status.SetCode(ptrace.StatusCode(spanRecord.StatusCode))

		// Convert attributes
		attrs := span.Attributes()
		for key, value := range spanRecord.Attributes {
			switch v := value.(type) {
			case string:
				attrs.PutStr(key, v)
			case bool:
				attrs.PutBool(key, v)
			case int64:
				attrs.PutInt(key, v)
			case float64:
				attrs.PutDouble(key, v)
			default:
				// Convert other types to string
				attrs.PutStr(key, fmt.Sprintf("%v", v))
			}
		}
	}

	return nil
}

func (tsp *tailSamplingSpanProcessor) fetchTraceForEvaluation(traceID pcommon.TraceID) (store.DecodedSpans, store.Meta, bool, error) {
	d, ok := tsp.idToTrace.Load(traceID)
	if !ok {
		return nil, store.Meta{}, false, nil
	}

	traceData := d.(*sampling.TraceData)
	traceData.Lock()
	defer traceData.Unlock()

	// Convert legacy TraceData to DecodedSpans interface for consistent policy evaluation
	legacySpans := &legacyTraceDataAdapter{
		traceData: traceData,
		traceID:   store.TraceID(traceID),
	}

	meta := store.Meta{
		TraceID:     store.TraceID(traceID),
		FirstSeen:   traceData.ArrivalTime,
		LastSeen:    traceData.ArrivalTime,
		SpanCount:   int(traceData.SpanCount.Load()),
		ApproxBytes: int64(traceData.ReceivedBatches.SpanCount() * 1024), // Rough estimate
		State:       store.StateActive,
		HasSpill:    false,
	}

	return legacySpans, meta, true, nil
}
