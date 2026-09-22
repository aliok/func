package keda

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	httpv1alpha1 "github.com/kedacore/http-add-on/operator/apis/http/v1alpha1"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"knative.dev/func/pkg/deployer"
	"knative.dev/func/pkg/deployers"
	fn "knative.dev/func/pkg/functions"
	"knative.dev/func/pkg/k8s"
)

const (
	KedaDeployerName = deployers.Keda
)

const (
	// scalerTypeAnnotation records which KEDA scaler the last keda deploy
	// provisioned for this function -- scalerTypeHTTP or scalerTypeKafka --
	// on the function's Service, the same durable object the exposure record
	// lives on. KEDA allows only one scaler per workload, and switching types
	// requires the old scaler fully gone before the new one is admitted (its
	// finalizer makes removal asynchronous). Rather than speculatively delete
	// the other type's resources on every deploy -- which fails on clusters
	// whose deploy ServiceAccount lacks the delete verb on keda.sh resources,
	// since Kubernetes checks authorization before existence -- Deploy reads
	// this record up front and refuses a type switch, telling the user to
	// delete and redeploy. A missing record reads as http: that was the only
	// keda scaler before this annotation existed.
	scalerTypeAnnotation = "function.knative.dev/keda-scaler"

	// triggerAuthRecordedAnnotation records, for a kafka scaler, whether the
	// last deploy created a TriggerAuthentication. Deploy and Remove consult
	// it so they only ever delete a TriggerAuthentication that was actually
	// created, instead of issuing a speculative delete for one that never
	// existed.
	triggerAuthRecordedAnnotation = "function.knative.dev/keda-trigger-auth"

	scalerTypeHTTP  = "http"
	scalerTypeKafka = "kafka"
)

type DeployerOpt func(*Deployer)

type Deployer struct {
	k8s.Deployer

	verbose   bool
	decorator deployer.DeployDecorator
	exposer   deployer.Exposer
}

func NewDeployer(opts ...DeployerOpt) *Deployer {
	d := &Deployer{
		Deployer: *k8s.NewDeployer(
			// init with the kedaDeployerDecorator to have the correct deployer labels&annotations
			k8s.WithDeployerDecorator(&kedaDeployerDecorator{}),
		),
	}

	for _, opt := range opts {
		opt(d)
	}
	return d
}

func WithDeployerVerbose(verbose bool) DeployerOpt {
	return func(d *Deployer) {
		d.verbose = verbose
		k8s.WithDeployerVerbose(verbose)(&d.Deployer)
	}
}

func WithExposer(exposer deployer.Exposer) DeployerOpt {
	return func(d *Deployer) {
		d.exposer = exposer
	}
}

func WithDeployerDecorator(decorator deployer.DeployDecorator) DeployerOpt {
	// use the custom keda decorator, which wraps the given decorator,
	// but with the keda specific annotations
	kedaDecorator := &kedaDeployerDecorator{
		wrapper: decorator,
	}

	return func(d *Deployer) {
		d.decorator = kedaDecorator
		k8s.WithDeployerDecorator(kedaDecorator)(&d.Deployer)
	}
}

var _ deployer.DeployDecorator = &kedaDeployerDecorator{}

type kedaDeployerDecorator struct {
	wrapper deployer.DeployDecorator
}

func (k *kedaDeployerDecorator) UpdateAnnotations(function fn.Function, annotations map[string]string) map[string]string {
	if k.wrapper != nil {
		annotations = k.wrapper.UpdateAnnotations(function, annotations)
	}

	// set correct deployer name
	annotations[deployer.DeployerNameAnnotation] = KedaDeployerName

	return annotations
}

func (k *kedaDeployerDecorator) UpdateLabels(function fn.Function, labels map[string]string) map[string]string {
	if k.wrapper != nil {
		labels = k.wrapper.UpdateLabels(function, labels)
	}

	return labels
}

func (d *Deployer) Deploy(ctx context.Context, f fn.Function) (fn.DeploymentResult, error) {
	triggers := triggers(f)
	if len(triggers) == 0 {
		// triggers(f) only returns empty when scale.keda is present with an
		// explicitly empty triggers list (the nil-Scale/nil-KEDA case falls
		// back to a plain http trigger). ValidateScale already rejects this,
		// but Deploy is reachable without Function.Validate first (library
		// callers, tests): without this check, Deploy would silently skip
		// both the HTTPScaledObject and Kafka ScaledObject paths and deploy
		// with no scaler at all.
		return fn.DeploymentResult{}, fmt.Errorf("function %q: deployer keda requires at least one trigger in scale.keda.triggers", f.Name)
	}
	wantHTTP := hasHTTPTrigger(triggers)
	wantKafka := hasKafkaTrigger(triggers)

	seenTriggerTypes := map[string]bool{}
	for i, t := range triggers {
		if t.Type != "http" && t.Type != "kafka" {
			// ValidateScale already rejects any type other than http/kafka
			// (cron is explicitly unsupported; anything else is invalid),
			// but Deploy is reachable without it first (library callers,
			// tests): an unrecognized type makes both wantHTTP and
			// wantKafka false, so without this check Deploy would
			// silently skip every scaler path and deploy the raw
			// workload with no scaling at all, instead of failing.
			return fn.DeploymentResult{}, fmt.Errorf(
				"function %q: scale.keda.triggers[%d].type has invalid value %q, allowed: http, kafka", f.Name, i, t.Type)
		}
		if seenTriggerTypes[t.Type] {
			// ValidateScale already rejects a repeated type, but Deploy is
			// reachable without it first: kafkaTrigger()/the http
			// targetValue lookup both only ever use the first match, so a
			// second trigger of the same type would silently have its
			// settings ignored instead of failing.
			return fn.DeploymentResult{}, fmt.Errorf(
				"function %q: scale.keda.triggers[%d].type %q is repeated: only one trigger of each type is supported", f.Name, i, t.Type)
		}
		seenTriggerTypes[t.Type] = true
	}
	if wantHTTP && wantKafka {
		// ValidateScale already rejects this combination, but Deploy is
		// reachable without it first: the deployer creates a separate
		// HTTPScaledObject for "http" and a separate ScaledObject for
		// "kafka", both targeting the same Deployment, and KEDA only
		// allows one scaler per workload.
		return fn.DeploymentResult{}, fmt.Errorf(
			"function %q: scale.keda.triggers must not combine type http with type kafka: they cannot scale the same Deployment together, not yet supported", f.Name)
	}

	if pollingIntervalIgnored(f, wantHTTP, wantKafka) {
		// Not fatal: the value is inert, not invalid. httpScaledObject scales
		// off the KEDA HTTP add-on's interceptor metrics, which have no polling
		// interval, so it never reads scale.keda.pollingInterval -- only a
		// kafka trigger's ScaledObject honors it. Warn so a caller who set it
		// expecting it to apply isn't left wondering why nothing changed.
		fmt.Fprintf(os.Stderr, "Warning: scale.keda.pollingInterval is ignored for an http trigger (it applies only to kafka triggers); function %q\n", f.Name)
	}

	if wantHTTP {
		if err := validateBridgeName(f.Name); err != nil {
			return fn.DeploymentResult{}, err
		}
	}
	if wantKafka {
		// Counterpart to validateBridgeName above: the Kafka path names its
		// ScaledObject/TriggerAuthentication by appending suffixes to f.Name,
		// which can overflow the 63-character DNS label limit and fail
		// resource creation server-side with no preflight otherwise.
		if err := validateKafkaResourceNames(f.Name, needsTriggerAuth(f.Run.Kafka)); err != nil {
			return fn.DeploymentResult{}, err
		}
	}

	// The following are all pure functions of f -- no cluster state needed --
	// so they run before d.Deployer.Deploy creates anything. ValidateScale
	// already rejects each of these, but Deploy is reachable without going
	// through Function.Validate first (library callers, tests): failing
	// before the raw Deployment/Service exist, rather than after, avoids
	// leaving a partial workload behind with no Kafka scaler and no error
	// pointing at why.
	if f.Scale != nil {
		// scale.min/max are int64 in func.yaml but replicaBounds narrows them
		// to int32 (Kubernetes replica counts). Reject values that would not
		// survive the narrowing before it silently wraps -- e.g. 1<<32 casts
		// to int32(0), which would otherwise slip past the maxScale >= 1 check
		// below as a bogus value.
		if f.Scale.Min != nil && (*f.Scale.Min < 0 || *f.Scale.Min > math.MaxInt32) {
			return fn.DeploymentResult{}, fmt.Errorf("function %q: scale.min %d is out of range [0, %d]", f.Name, *f.Scale.Min, math.MaxInt32)
		}
		if f.Scale.Max != nil && (*f.Scale.Max < 0 || *f.Scale.Max > math.MaxInt32) {
			return fn.DeploymentResult{}, fmt.Errorf("function %q: scale.max %d is out of range [0, %d]", f.Name, *f.Scale.Max, math.MaxInt32)
		}
	}
	minScale, maxScale := replicaBounds(f)
	if maxScale < 1 {
		// deployer: keda's HTTPScaledObject/ScaledObject map scale.max
		// straight into an HPA's maxReplicas, which must be >= 1.
		return fn.DeploymentResult{}, fmt.Errorf("function %q: scale.max must be >= 1 for deployer: keda, got %d", f.Name, maxScale)
	}
	if minScale > maxScale {
		return fn.DeploymentResult{}, fmt.Errorf("function %q: scale.max (%d) must be >= scale.min (%d)", f.Name, maxScale, minScale)
	}
	if wantKafka && f.Run.Kafka == nil {
		return fn.DeploymentResult{}, fmt.Errorf("function %q: scale.keda.triggers has a kafka trigger but run.kafka is not configured", f.Name)
	}
	if wantKafka && f.Run.Kafka != nil {
		// buildScaledObject sets these directly as KEDA trigger metadata
		// (bootstrapServers/topic/consumerGroup); an empty value produces
		// a ScaledObject that can't connect to any broker.
		var missing []string
		if f.Run.Kafka.Brokers == "" {
			missing = append(missing, "brokers")
		}
		if f.Run.Kafka.Topic == "" {
			missing = append(missing, "topic")
		}
		if f.Run.Kafka.ConsumerGroup == "" {
			missing = append(missing, "consumerGroup")
		}
		if len(missing) > 0 {
			return fn.DeploymentResult{}, fmt.Errorf("function %q: run.kafka is missing required field(s): %s", f.Name, strings.Join(missing, ", "))
		}
		// buildTriggerAuth's TLS-path resolution only depends on
		// f.Run.Kafka/f.Run.Volumes -- no live deployment needed -- so it
		// can run here, before the raw Deployment/Service exist, instead
		// of only inside buildTriggerAuth after they already do.
		if err := validateKafkaTLSPaths(f.Run.Kafka, f.Run.Volumes); err != nil {
			return fn.DeploymentResult{}, fmt.Errorf("function %q: %w", f.Name, err)
		}
	}
	if wantKafka && f.Run.Kafka != nil {
		// Validate the securityProtocol/TLS/SASL consistency with the same
		// rules Function.Validate applies, so a direct Deploy caller that
		// bypasses it can't create a ScaledObject whose SASL/TLS metadata
		// silently disagrees with how the function's own container
		// authenticates. buildScaledObject only emits "sasl" metadata for a
		// non-empty mechanism, so e.g. SASL_SSL with an empty mechanism would
		// otherwise leave KEDA connecting without SASL at all. Excludes the
		// runtime/invoke checks (a function-authoring concern, not a KEDA
		// resource concern) and the broker/topic/consumerGroup checks (done
		// above), so it doesn't reject valid direct-deploy inputs.
		if errs := fn.ValidateKafkaSecurity(f.Run.Kafka); len(errs) > 0 {
			return fn.DeploymentResult{}, fmt.Errorf("function %q: %s", f.Name, strings.Join(errs, "; "))
		}
	}

	// Final sweep with the shared scale validator, for direct callers that
	// bypass Function.Validate. The tailored guards above cover the cases
	// worth a deploy-specific message (and their own tests); this catches the
	// remaining ValidateScale checks they don't -- pollingInterval/
	// cooldownPeriod bounds, per-trigger threshold bounds, and the
	// scale.keda<->scale.kpa mutual exclusion -- so an invalid scaler config
	// can't create/update the raw Deployment before KEDA rejects or silently
	// ignores it. Gated on an explicit scale.keda/scale.kpa so the intentional
	// default-http fallback (neither set -> triggers() supplies a plain http
	// trigger) still deploys instead of tripping "keda requires a trigger".
	if f.Scale != nil && (f.Scale.KEDA != nil || f.Scale.KPA != nil) {
		if errs := fn.ValidateScale(f.Scale, KedaDeployerName, f.Run.Kafka); len(errs) > 0 {
			return fn.DeploymentResult{}, fmt.Errorf("function %q: %s", f.Name, strings.Join(errs, "; "))
		}
	}

	k8sClientset, err := k8s.NewKubernetesClientset()
	if err != nil {
		return fn.DeploymentResult{}, fmt.Errorf("failed to create K8sClientset: %v", err)
	}
	dynClient, err := k8s.NewDynamicClient()
	if err != nil {
		return fn.DeploymentResult{}, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	// Refuse a scaler-type switch before anything is changed on the cluster.
	// KEDA allows one scaler per workload, and its finalizer makes the old
	// scaler's removal asynchronous, so switching http<->kafka in place races
	// the admission webhook and can leave the function with no scaler at all.
	// Read the type the last deploy recorded on the function's Service; if it
	// differs from what this deploy wants, tell the user to delete and
	// redeploy rather than deleting the other type's resources speculatively.
	wantType := scalerTypeHTTP
	if wantKafka {
		wantType = scalerTypeKafka
	}
	priorType, priorHadAuth, err := readScalerRecord(ctx, k8sClientset, f)
	if err != nil {
		return fn.DeploymentResult{}, err
	}
	if priorType != "" && priorType != wantType {
		return fn.DeploymentResult{}, fmt.Errorf(
			"function %q is already deployed with %s scaling; switching to %s scaling in place is not "+
				"supported because KEDA allows only one scaler per workload. Run `func delete` and then deploy again",
			f.Name, priorType, wantType)
	}

	var interceptorNS string
	var exposeRefusal error
	if wantHTTP {
		interceptorNS, exposeRefusal = interceptorNamespace(ctx, k8sClientset)
		if err := d.validateExposure(f, exposeRefusal); err != nil {
			return fn.DeploymentResult{}, err
		}
	}

	// execute raw deployment deployer
	deployResult, err := d.Deployer.Deploy(ctx, f)
	if err != nil {
		return fn.DeploymentResult{}, fmt.Errorf("failed to deploy function via raw deployer: %w", err)
	}

	namespace := deployResult.Namespace

	deployment, err := k8sClientset.AppsV1().Deployments(namespace).Get(ctx, f.Name, metav1.GetOptions{})
	if err != nil {
		return fn.DeploymentResult{}, fmt.Errorf("failed to get deployment %s/%s: %v", namespace, f.Name, err)
	}

	appService, err := k8sClientset.CoreV1().Services(namespace).Get(ctx, f.Name, metav1.GetOptions{})
	if err != nil {
		return fn.DeploymentResult{}, fmt.Errorf("failed to get service %s/%s: %v", namespace, f.Name, err)
	}

	// No cross-type cleanup runs here: the switch guard above guarantees this
	// deploy is provisioning the same scaler type the last one did (or the
	// first for a new function), so there is never a scaler of the other type
	// to remove. `func delete` reclaims the previous type's resources.

	// HTTP trigger path: bridge Service + HTTPScaledObject
	var url string
	appliedExpose := ""
	if wantHTTP {
		ref := deployer.NewExposureRef(f.Name, namespace, interceptorNS)
		if err := ensureInterceptorBridgeService(ctx, k8sClientset, ref, deployment); err != nil {
			return fn.DeploymentResult{}, fmt.Errorf("failed to ensure proxy service exists: %w", err)
		}

		labels, err := deployer.GenerateCommonLabels(f, d.decorator)
		if err != nil {
			return fn.DeploymentResult{}, fmt.Errorf("failed to generate common labels: %w", err)
		}
		annotations := deployer.GenerateCommonAnnotations(f, d.decorator, false, KedaDeployerName)

		target := deployTarget{
			clientset:   k8sClientset,
			dynClient:   dynClient,
			ref:         ref,
			deployment:  deployment,
			appService:  appService,
			labels:      labels,
			annotations: annotations,
			minScale:    minScale,
			maxScale:    maxScale,
			scale:       f.Scale,
		}

		if d.exposer != nil && fn.ActiveExpose(f.Expose) {
			if url, err = d.deployExposed(ctx, target); err != nil {
				return fn.DeploymentResult{}, err
			}
			appliedExpose = f.Expose
		} else {
			if url, err = d.deployClusterLocal(ctx, target); err != nil {
				return fn.DeploymentResult{}, err
			}
		}
	} else {
		// No HTTP trigger — URL is the app service. The Service listens on
		// port 80 (routing to the container's DefaultHTTPPort via
		// targetPort), so the URL, like elsewhere in the codebase (e.g.
		// pkg/k8s/describer.go), has no explicit port.
		url = fmt.Sprintf("http://%s.%s.svc", f.Name, namespace)

		// A prior deploy may have exposed this function over HTTP. Nothing
		// reconciles that exposure once the HTTP trigger is gone, so clear
		// it the same way deployClusterLocal does -- otherwise the old
		// Route and the Service's exposure annotations stay active,
		// pointing at a function that no longer has anything serving HTTP.
		target := deployTarget{
			clientset: k8sClientset,
			dynClient: dynClient,
			ref:       deployer.NewExposureRef(f.Name, namespace, ""),
		}
		if err := d.clearExposure(ctx, target, appService.Annotations[k8s.RouteNamespaceAnnotation]); err != nil {
			return fn.DeploymentResult{}, err
		}
	}

	// Kafka trigger path: TriggerAuthentication + ScaledObject
	if wantKafka && f.Run.Kafka != nil {
		needsAuth := needsTriggerAuth(f.Run.Kafka)
		if needsAuth {
			ta, err := buildTriggerAuth(f, deployment, namespace)
			if err != nil {
				// A TLS path was explicitly configured but doesn't resolve
				// to any configured volume. Failing here avoids a
				// ScaledObject whose authenticationRef points at a
				// TriggerAuthentication missing the credential it needs.
				return fn.DeploymentResult{}, fmt.Errorf("function %q: %w", f.Name, err)
			}
			if ta == nil {
				// needsTriggerAuth said SASL/TLS credentials need a
				// TriggerAuthentication, but buildTriggerAuth found nothing
				// to resolve at all. Failing here avoids a ScaledObject
				// whose authenticationRef points at a TriggerAuthentication
				// that was never created.
				return fn.DeploymentResult{}, fmt.Errorf(
					"function %q: run.kafka SASL/TLS credentials are configured but could not be resolved to a Secret or environment variable; "+
						"check that run.kafka.sasl/tls paths match a configured volume", f.Name)
			}
			if err := ensureTriggerAuth(ctx, dynClient, ta); err != nil {
				return fn.DeploymentResult{}, fmt.Errorf("failed to ensure TriggerAuthentication: %w", err)
			}
		}

		kt := kafkaTrigger(triggers)
		so := buildScaledObject(f, kt, deployment, namespace, minScale, maxScale)
		if so != nil {
			if err := ensureScaledObject(ctx, dynClient, so); err != nil {
				return fn.DeploymentResult{}, fmt.Errorf("failed to ensure ScaledObject: %w", err)
			}
		}

		if !needsAuth && priorHadAuth {
			// SASL/TLS credentials were removed from run.kafka while the kafka
			// trigger stayed, and the record says a prior deploy created a
			// TriggerAuthentication that nothing references anymore. Delete it
			// only AFTER the ScaledObject above has been reconciled to drop its
			// authenticationRef -- deleting first would, if that update then
			// failed, leave the live ScaledObject pointing at a
			// TriggerAuthentication that no longer exists. Gated on the record
			// so a kafka deploy that never had credentials doesn't issue a
			// speculative delete (Forbidden on tighter RBAC). Not fatal:
			// owner-ref GC covers a failure.
			if err := deleteTriggerAuth(ctx, dynClient, namespace, triggerAuthName(f.Name)); err != nil {
				fmt.Fprintf(os.Stderr, "warning: %v\n", err)
			}
		}
	}

	// Record the scaler type (and, for kafka, whether a TriggerAuthentication
	// was created) so the next deploy can refuse a type switch up front and a
	// later delete only removes what was actually created.
	if err := recordScaler(ctx, k8sClientset, namespace, f.Name, wantType, wantKafka && needsTriggerAuth(f.Run.Kafka)); err != nil {
		return fn.DeploymentResult{}, err
	}

	return fn.DeploymentResult{
		Status:    deployResult.Status,
		URL:       url,
		Namespace: deployResult.Namespace,
		Deployer:  KedaDeployerName,
		Expose:    appliedExpose,
	}, nil
}

// validateExposure refuses, before anything is created, an exposure this
// deploy could not honor: a Route name Kubernetes would reject, or an
// interceptor that cannot be confirmed to exist (exposeRefusal, resolved by
// interceptorNamespace). Nothing to check when no exposure is wanted.
func (d *Deployer) validateExposure(f fn.Function, exposeRefusal error) error {
	if d.exposer == nil || !fn.ActiveExpose(f.Expose) {
		return nil
	}
	// The Route's name needs the namespace the function will land in;
	// k8s.DeployNamespace is the same rule the raw deployer uses, so this
	// cannot validate a name the deploy will not use.
	exposeNS, err := k8s.DeployNamespace(f)
	if err != nil {
		return err
	}
	if err := validateExposureName(f, exposeNS); err != nil {
		return err
	}
	// Refuse rather than build a Route to a Service that may not be there:
	// such a Route is admitted and then serves nothing. The two refusals
	// share the NO but not the WHY: "not found" and "could not look" send an
	// operator to different fixes.
	if exposeRefusal != nil {
		return fmt.Errorf("cannot expose function %q: %w", f.Name, exposeRefusal)
	}
	return nil
}

// deployTarget is everything one keda deploy resolved and fetched before
// choosing a path: the clients, the function's placement, replica bounds,
// and the live objects the HSO hangs off
type deployTarget struct {
	clientset   kubernetes.Interface
	dynClient   dynamic.Interface
	ref         deployer.ExposureRef
	deployment  *v1.Deployment
	appService  *corev1.Service
	labels      map[string]string
	annotations map[string]string
	minScale    int32
	maxScale    int32
	scale       *fn.ScaleOptions
}

// bridgeHosts are the cluster-local names the HSO registers for f: requests
// through the bridge Service reach the interceptor carrying one of these.
func bridgeHosts(ref deployer.ExposureRef) []string {
	return []string{
		fmt.Sprintf("%s.%s.svc", interceptorBridgeServiceName(ref.FunctionName), ref.FunctionNamespace),
		interceptorBridgeServiceName(ref.FunctionName),
	}
}

// deployExposed settles an exposed function in the order Route -> HSO ->
// record. The Route goes first because the router mints the hostname and the
// HSO write is where that hostname gets registered: the interceptor 404s any
// Host header no HSO registers. The record is last: teardown and describe
// read it, never the cluster. A record that cannot be written takes the
// just-created Route back down. A kill between create and record still
// orphans, and delete will not collect that Route; the next exposed deploy
// reclaims it, because Expose finds an existing Route by the function's
// labels. Only reached with an Exposer and an active intent.
func (d *Deployer) deployExposed(ctx context.Context, t deployTarget) (string, error) {
	exposedHost, err := d.exposer.Expose(ctx, t.dynClient, interceptorExposure(t.ref, t.labels, t.annotations))
	if err != nil {
		return "", fmt.Errorf("failed to expose function externally: %w", err)
	}

	hosts := append(bridgeHosts(t.ref), exposedHost)
	if err := ensureHTTPScaledObject(ctx, t, hosts, t.scale); err != nil {
		return "", fmt.Errorf("failed to ensure http scaled object exists: %w", err)
	}

	// reconcile annotations to function service about exposure
	if err := k8s.RecordExposure(ctx, t.clientset, t.ref, exposedHost); err != nil {
		if rbErr := d.exposer.Unexpose(ctx, t.dynClient, t.ref); rbErr != nil {
			return "", fmt.Errorf("recording the exposure failed: %w; rolling the Route back failed too: %v", err, rbErr)
		}
		return "", fmt.Errorf("recording the exposure failed, the Route was rolled back: %w", err)
	}

	// ocproute terminates TLS at the edge and redirects http.
	return fmt.Sprintf("https://%s", exposedHost), nil
}

// deployClusterLocal settles a cluster-local function in the order HSO ->
// removal -> record. The HSO shrink kills external traffic first: dropping
// the exposed hostname from the host list makes the interceptor 404 it, so a
// Forbidden in the interceptor's namespace (while removing the now-dead
// Route in clearExposure) fails the deploy with the function scalable and
// effectively unexposed.
func (d *Deployer) deployClusterLocal(ctx context.Context, t deployTarget) (string, error) {
	hosts := bridgeHosts(t.ref)
	if err := ensureHTTPScaledObject(ctx, t, hosts, t.scale); err != nil {
		return "", fmt.Errorf("failed to ensure http scaled object exists: %w", err)
	}

	if err := d.clearExposure(ctx, t, t.appService.Annotations[k8s.RouteNamespaceAnnotation]); err != nil {
		return "", err
	}

	return fmt.Sprintf("http://%s:8080", hosts[0]), nil // TODO: check on HTTPS too
}

// clearExposure Unexposes the recorded Route, then clears the Service
// record. Unexpose first so a failure leaves the record for retry. Nil
// exposer is a no-op. recordedNS is a parameter so tests can omit it.
func (d *Deployer) clearExposure(ctx context.Context, t deployTarget, recordedNS string) error {
	if d.exposer == nil {
		return nil
	}

	if recordedNS != "" {
		ref := t.ref
		ref.Namespace = recordedNS
		if err := d.exposer.Unexpose(ctx, t.dynClient, ref); err != nil {
			return fmt.Errorf("failed to remove external exposure: %w", err)
		}
	}

	// hostname == "" -> remove the record
	if err := k8s.RecordExposure(ctx, t.clientset, t.ref, ""); err != nil {
		return fmt.Errorf("failed to clear the exposure record: %w", err)
	}
	return nil
}

// readScalerRecord returns the scaler type the last deploy recorded on the
// function's Service, and whether it recorded a TriggerAuthentication. An
// absent Service (never deployed) returns "" -- no prior scaler, nothing to
// switch from. A Service with no record predates this annotation, when http
// was the only keda scaler, so it reads as scalerTypeHTTP. A namespace that
// can't be resolved likewise means the function was never deployed.
func readScalerRecord(ctx context.Context, clientset kubernetes.Interface, f fn.Function) (scalerType string, hasAuth bool, err error) {
	ns, err := k8s.DeployNamespace(f)
	if err != nil {
		return "", false, nil
	}
	svc, err := clientset.CoreV1().Services(ns).Get(ctx, f.Name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("failed to read keda scaler record on service %s/%s: %w", ns, f.Name, err)
	}
	t := svc.Annotations[scalerTypeAnnotation]
	if t == "" {
		t = scalerTypeHTTP
	}
	return t, svc.Annotations[triggerAuthRecordedAnnotation] == "true", nil
}

// recordScaler writes the scaler type -- and, for kafka, whether a
// TriggerAuthentication was created -- onto the function's Service, mirroring
// k8s.RecordExposure. Get->Update with conflict retry, since a concurrent
// deploy or cluster operator may patch the Service between the two calls.
func recordScaler(ctx context.Context, clientset kubernetes.Interface, ns, name, scalerType string, hasAuth bool) error {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		svc, err := clientset.CoreV1().Services(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if svc.Annotations == nil {
			svc.Annotations = map[string]string{}
		}
		svc.Annotations[scalerTypeAnnotation] = scalerType
		if scalerType == scalerTypeKafka && hasAuth {
			svc.Annotations[triggerAuthRecordedAnnotation] = "true"
		} else {
			delete(svc.Annotations, triggerAuthRecordedAnnotation)
		}
		_, err = clientset.CoreV1().Services(ns).Update(ctx, svc, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return fmt.Errorf("failed to record keda scaler type on service %q: %w", name, err)
	}
	return nil
}

const (
	// defaultMinReplicas / defaultMaxReplicas are the HTTPScaledObject
	// replica bounds when the function does not set scale.min / scale.max.
	defaultMinReplicas int32 = 1
	defaultMaxReplicas int32 = 10
)

// pollingIntervalIgnored reports whether scale.keda.pollingInterval is set
// but will have no effect. Only a kafka trigger's ScaledObject honors it
// (see buildScaledObjectSpec); the HTTPScaledObject scales off the KEDA HTTP
// add-on's interceptor metrics, which have no polling interval, so
// httpScaledObject never reads it. Deploy warns (rather than rejects) when
// this holds. Pure so it can be unit-tested without capturing stderr.
func pollingIntervalIgnored(f fn.Function, wantHTTP, wantKafka bool) bool {
	return wantHTTP && !wantKafka &&
		f.Scale != nil && f.Scale.KEDA != nil && f.Scale.KEDA.PollingInterval != nil
}

// replicaBounds is scale.min and scale.max from the function, or the
// defaults above when either is unset. The HTTPScaledObject spec requires
// both; these fallbacks are keda's, not shared with the raw or knative
// deployers.
func replicaBounds(f fn.Function) (min, max int32) {
	min, max = defaultMinReplicas, defaultMaxReplicas
	if f.Scale != nil {
		if f.Scale.Min != nil {
			min = int32(*f.Scale.Min)
		}
		if f.Scale.Max != nil {
			max = int32(*f.Scale.Max)
		}
	}
	return
}

func httpScaledObject(t deployTarget, hosts []string, scale *fn.ScaleOptions) (*httpv1alpha1.HTTPScaledObject, error) {
	deployment := t.deployment
	service := t.appService
	if len(service.Spec.Ports) == 0 {
		return nil, fmt.Errorf("service %s has no ports defined", service.Name)
	}

	cooldown := int32(300)
	targetValue := int64(100)
	if scale != nil && scale.KEDA != nil {
		if scale.KEDA.CooldownPeriod != nil {
			cooldown = *scale.KEDA.CooldownPeriod
		}
		for _, trig := range scale.KEDA.Triggers {
			if trig.Type == "http" && trig.TargetValue != nil {
				targetValue = *trig.TargetValue
				break
			}
		}
	}

	controllerTrue := true
	return &httpv1alpha1.HTTPScaledObject{
		ObjectMeta: metav1.ObjectMeta{
			Name:        t.ref.FunctionName,
			Namespace:   t.ref.FunctionNamespace,
			Labels:      t.labels,
			Annotations: t.annotations,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       deployment.Name,
					UID:        deployment.UID,
					Controller: &controllerTrue,
				},
			},
		},
		Spec: httpv1alpha1.HTTPScaledObjectSpec{
			Hosts: hosts,
			ScaleTargetRef: httpv1alpha1.ScaleTargetRef{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       deployment.Name,
				Service:    service.Name,
				Port:       service.Spec.Ports[0].Port,
			},
			Replicas: &httpv1alpha1.ReplicaStruct{
				Min: &t.minScale,
				Max: &t.maxScale,
			},
			CooldownPeriod: &cooldown,
			ScalingMetric: &httpv1alpha1.ScalingMetricSpec{
				Rate: &httpv1alpha1.RateMetricSpec{
					TargetValue: int(targetValue),
					Window: metav1.Duration{
						Duration: time.Minute,
					},
					Granularity: metav1.Duration{
						Duration: time.Second,
					},
				},
			},
		},
	}, nil
}

func interceptorBridgeServiceName(name string) string {
	return name + interceptorBridgeSuffix
}

func interceptorBridgeService(ref deployer.ExposureRef, deployment *v1.Deployment) *corev1.Service {
	controllerTrue := true
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      interceptorBridgeServiceName(ref.FunctionName),
			Namespace: ref.FunctionNamespace,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       deployment.Name,
					UID:        deployment.UID,
					Controller: &controllerTrue,
				},
			},
		},
		Spec: corev1.ServiceSpec{
			Type:         corev1.ServiceTypeExternalName,
			ExternalName: fmt.Sprintf("%s.%s.svc.cluster.local", interceptorServiceName, ref.Namespace),
		},
	}
}

// ensureInterceptorBridgeService makes sure to create the service which serves
// as the entrypoint to the function this service will server as an external-name
// service and forward the request to the keda interceptor-proxy by preserving
// the host name. This service name is also used in the HTTPScaledObject as
// host name to allow the interceptor to match the request with the correct
// target/scaledObject.
func ensureInterceptorBridgeService(ctx context.Context,
	clientset *kubernetes.Clientset, ref deployer.ExposureRef, deployment *v1.Deployment) error {

	expected := interceptorBridgeService(ref, deployment)
	existing, err := clientset.CoreV1().Services(expected.Namespace).Get(ctx, expected.Name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			if _, err := clientset.CoreV1().Services(expected.Namespace).Create(ctx, expected, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create service to interceptor proxy: %w", err)
			}

			return nil
		}

		return fmt.Errorf("failed to get service to interceptor proxy: %w", err)
	}

	// check if we need to update
	if !equality.Semantic.DeepEqual(existing.Spec, expected.Spec) {
		// Preserve resource version for update
		expected.ResourceVersion = existing.ResourceVersion

		if _, err = clientset.CoreV1().Services(ref.FunctionNamespace).Update(ctx, expected, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("failed to update service to interceptor proxy: %w", err)
		}

		return nil
	}

	return nil
}

func ensureHTTPScaledObject(ctx context.Context, t deployTarget, hosts []string, scale *fn.ScaleOptions) error {
	expected, err := httpScaledObject(t, hosts, scale)
	if err != nil {
		return fmt.Errorf("failed to generate http scaled object: %w", err)
	}

	httpScaledObjectClientset, err := NewHTTPScaledObjectClientset()
	if err != nil {
		return fmt.Errorf("failed to create HTTPScaledObject clientset: %v", err)
	}

	existing, err := httpScaledObjectClientset.HttpV1alpha1().HTTPScaledObjects(expected.Namespace).Get(ctx, expected.Name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			if _, err := httpScaledObjectClientset.HttpV1alpha1().HTTPScaledObjects(expected.Namespace).Create(ctx, expected, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("failed to create HTTPScaledObject: %w", err)
			}

			if err := WaitForHTTPScaledObjectAvailable(ctx, httpScaledObjectClientset, t.ref.FunctionNamespace, expected.Name, k8s.DefaultWaitingTimeout); err != nil {
				return fmt.Errorf("HTTPScaledObject did not become ready: %w", err)
			}

			return nil
		}

		return fmt.Errorf("failed to get HTTPScaledObject: %w", err)
	}

	// check if we need to update
	if !equality.Semantic.DeepEqual(existing.Spec, expected.Spec) {
		// Preserve resource version for update
		expected.ResourceVersion = existing.ResourceVersion

		if _, err = httpScaledObjectClientset.HttpV1alpha1().HTTPScaledObjects(expected.Namespace).Update(ctx, expected, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("failed to update HTTPScaledObject: %w", err)
		}

		if err := WaitForHTTPScaledObjectAvailable(ctx, httpScaledObjectClientset, t.ref.FunctionNamespace, expected.Name, k8s.DefaultWaitingTimeout); err != nil {
			return fmt.Errorf("HTTPScaledObject did not become ready: %w", err)
		}

		return nil
	}

	return nil
}

func UsesKedaDeployer(annotations map[string]string) bool {
	deployer, ok := annotations[deployer.DeployerNameAnnotation]

	return ok && deployer == KedaDeployerName
}
