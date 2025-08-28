package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

func main() {
	log.Println("Starting trace generator for object-only test...")

	// Initialize OpenTelemetry
	tp, err := initTracer()
	if err != nil {
		log.Fatalf("Failed to initialize tracer: %v", err)
	}
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down tracer provider: %v", err)
		}
	}()

	// Generate test traces for 5 minutes
	ctx := context.Background()
	endTime := time.Now().Add(5 * time.Minute)
	
	log.Println("Generating traces for 5 minutes...")
	
	for time.Now().Before(endTime) {
		generateTraceScenario(ctx)
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
	}
	
	log.Println("Trace generation completed")
}

func initTracer() (*tracesdk.TracerProvider, error) {
	// Create OTLP HTTP exporter
	exporter, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpoint("http://otel-collector:4318"),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP exporter: %w", err)
	}

	// Create resource
	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceName("trace-generator"),
			semconv.ServiceVersion("1.0.0"),
			attribute.String("environment", "integration-test"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Create tracer provider
	tp := tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(exporter),
		tracesdk.WithResource(res),
		tracesdk.WithSampler(tracesdk.AlwaysSample()),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp, nil
}

func generateTraceScenario(ctx context.Context) {
	tracer := otel.Tracer("integration-test")
	
	// Randomly choose a scenario
	scenarios := []func(context.Context, trace.Tracer){
		generateNormalTrace,
		generateErrorTrace,
		generateSlowTrace,
		generateHighCardinalityTrace,
	}
	
	scenario := scenarios[rand.Intn(len(scenarios))]
	scenario(ctx, tracer)
}

func generateNormalTrace(ctx context.Context, tracer trace.Tracer) {
	ctx, span := tracer.Start(ctx, "normal-operation")
	defer span.End()
	
	span.SetAttributes(
		attribute.String("service.name", "payment-service"),
		attribute.String("operation", "process-payment"),
		attribute.String("user.id", fmt.Sprintf("user-%d", rand.Intn(1000))),
		attribute.Float64("amount", rand.Float64()*1000),
	)
	
	// Simulate work
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	
	// Add child spans
	generateChildSpan(ctx, tracer, "validate-payment")
	generateChildSpan(ctx, tracer, "charge-card")
	generateChildSpan(ctx, tracer, "send-receipt")
}

func generateErrorTrace(ctx context.Context, tracer trace.Tracer) {
	ctx, span := tracer.Start(ctx, "error-operation")
	defer span.End()
	
	span.SetAttributes(
		attribute.String("service.name", "order-service"),
		attribute.String("operation", "create-order"),
		attribute.String("user.id", fmt.Sprintf("user-%d", rand.Intn(1000))),
	)
	
	// Simulate work
	time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
	
	// Set error status
	span.SetStatus(codes.Error, "Database connection failed")
	span.RecordError(fmt.Errorf("connection timeout after 5s"))
}

func generateSlowTrace(ctx context.Context, tracer trace.Tracer) {
	ctx, span := tracer.Start(ctx, "slow-operation")
	defer span.End()
	
	span.SetAttributes(
		attribute.String("service.name", "analytics-service"),
		attribute.String("operation", "generate-report"),
		attribute.String("report.type", "monthly"),
	)
	
	// Simulate slow work (over 1 second to trigger latency policy)
	time.Sleep(time.Duration(1000+rand.Intn(2000)) * time.Millisecond)
	
	generateChildSpan(ctx, tracer, "query-database")
	generateChildSpan(ctx, tracer, "process-data")
	generateChildSpan(ctx, tracer, "generate-pdf")
}

func generateHighCardinalityTrace(ctx context.Context, tracer trace.Tracer) {
	services := []string{
		"user-service", "product-service", "inventory-service",
		"notification-service", "audit-service", "search-service",
	}
	
	service := services[rand.Intn(len(services))]
	
	ctx, span := tracer.Start(ctx, fmt.Sprintf("%s-operation", service))
	defer span.End()
	
	span.SetAttributes(
		attribute.String("service.name", service),
		attribute.String("operation", "process-request"),
		attribute.String("request.id", fmt.Sprintf("req-%d", rand.Intn(10000))),
		attribute.String("region", []string{"us-east", "us-west", "eu-central"}[rand.Intn(3)]),
		attribute.Int("batch.size", rand.Intn(100)+1),
	)
	
	// Simulate work
	time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)
}

func generateChildSpan(ctx context.Context, tracer trace.Tracer, operation string) {
	_, span := tracer.Start(ctx, operation)
	defer span.End()
	
	span.SetAttributes(
		attribute.String("component", "internal"),
		attribute.String("operation", operation),
	)
	
	// Simulate work
	time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
}