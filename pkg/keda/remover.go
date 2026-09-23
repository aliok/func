package keda

import (
	"context"
	"fmt"
	"os"

	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/func/pkg/deployer"
	fn "knative.dev/func/pkg/functions"
	"knative.dev/func/pkg/k8s"
	"knative.dev/func/pkg/ocproute"
)

func NewRemover(verbose bool) *Remover {
	return &Remover{
		verbose: verbose,
	}
}

type Remover struct {
	verbose bool
}

func (remover *Remover) Remove(ctx context.Context, name, ns string) error {
	if ns == "" {
		fmt.Fprintf(os.Stderr, "no namespace defined when trying to delete a function in keda remover\n")
		return fn.ErrNamespaceRequired
	}

	clientset, err := k8s.NewKubernetesClientset()
	if err != nil {
		return fmt.Errorf("could not setup kubernetes clientset: %w", err)
	}

	svc, err := clientset.CoreV1().Services(ns).Get(ctx, name, metav1.GetOptions{})
	switch {
	case apiErrors.IsNotFound(err):
		// No Service means nothing to inspect: the record lived on it, and so
		// did the annotation saying this was keda's.
		return fn.ErrNotHandled

	case err != nil:
		return err

	case !UsesKedaDeployer(svc.Annotations):
		// Another deployer's function, and its own objects are all still here
		// to say so.
		return fn.ErrNotHandled
	}

	dynClient, err := k8s.NewDynamicClient()
	if err != nil {
		return fmt.Errorf("could not setup dynamic client: %w", err)
	}

	// Remove the recorded Route before deleting anything: keda's Route has no
	// owner reference (it would have to cross namespaces), so nothing collects
	// it, and its record - these Service annotations - is deleted with the
	// Service below. A failure here touches nothing, so the user can retry.
	// A Route left unrecorded by a crash is not searched for; the next
	// exposed redeploy finds it by its function labels.
	if recordedNS := svc.Annotations[k8s.RouteNamespaceAnnotation]; recordedNS != "" {
		if err := ocproute.New(KedaDeployerName).Unexpose(ctx, dynClient, deployer.NewExposureRef(name, ns, recordedNS)); err != nil {
			return fmt.Errorf("could not remove the Route exposing function %q in namespace %q; "+
				"nothing was deleted and the function is still running, if you fix this you can run delete again: %w",
				name, recordedNS, err)
		}
	}

	// Clean up Kafka scaling resources before deleting the Deployment, but only
	// when the record on the Service says this function was scaled by kafka.
	// They carry ownerReferences so they'd be garbage-collected with the
	// Deployment anyway; the explicit deletion just avoids races with a slow GC.
	// Gating on the record means an http-only function -- which never created
	// either -- issues no delete against keda.sh, so `func delete`, run with the
	// user's own credentials, doesn't fail with Forbidden on resources the user
	// may have no rights to and that never existed (Kubernetes checks
	// authorization before existence). The TriggerAuthentication is created only
	// for a kafka function using SASL/TLS credentials, so it is deleted only if
	// present. Errors here are warnings, not fatal: owner-ref GC still collects
	// anything left behind when the Deployment goes.
	if svc.Annotations[scalerTypeAnnotation] == scalerTypeKafka {
		if err := deleteScaledObject(ctx, dynClient, ns, scaledObjectName(name)); err != nil {
			fmt.Fprintf(os.Stderr, "warning: %v\n", err)
		}
		deleteTriggerAuthIfExists(ctx, dynClient, ns, name)
	}

	deploymentClient := clientset.AppsV1().Deployments(ns)

	// Delete only the Deployment; owner references take the rest with it.
	if err := deploymentClient.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		if apiErrors.IsNotFound(err) {
			return fn.ErrFunctionNotFound
		}
		return fmt.Errorf("keda remover failed to delete the deployment: %v", err)
	}

	if err := k8s.WaitForServiceRemoved(ctx, clientset, ns, name, k8s.DefaultWaitingTimeout); err != nil {
		return fmt.Errorf("keda remover failed to propagate service deletion: %v", err)
	}

	return nil
}
