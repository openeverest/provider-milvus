package provider

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/openeverest/provider-milvus/definition/components"
	"github.com/openeverest/provider-milvus/internal/milvusapi"
)

// milvusHealthPort serves Milvus's /healthz.
const milvusHealthPort = 9091

// The operator's default probes (milvus-operator GetDefault*Probe). A tuned
// probe replaces the default wholesale, so it starts from these. Every field
// the API server would default is set: the operator diffs its rendered pod
// template against the live Deployment, and a missing default rolls the pods
// on every reconcile.
func defaultStartupProbe() corev1.Probe {
	return corev1.Probe{
		ProbeHandler:     corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(milvusHealthPort)}},
		TimeoutSeconds:   3,
		PeriodSeconds:    10,
		FailureThreshold: 18,
		SuccessThreshold: 1,
	}
}

func defaultLivenessProbe() corev1.Probe {
	return corev1.Probe{
		ProbeHandler:     corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(milvusHealthPort)}},
		TimeoutSeconds:   10,
		PeriodSeconds:    15,
		FailureThreshold: 3,
		SuccessThreshold: 1,
	}
}

func defaultReadinessProbe() corev1.Probe {
	return corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
			Path:   "/healthz",
			Port:   intstr.FromInt32(milvusHealthPort),
			Scheme: corev1.URISchemeHTTP,
		}},
		TimeoutSeconds:   3,
		PeriodSeconds:    15,
		FailureThreshold: 2,
		SuccessThreshold: 1,
	}
}

// applyProbes hands the operator the tuned probes; untuned ones are left to
// its defaults so existing pods do not roll.
func applyProbes(spec *milvusapi.ComponentSpec, probes *components.Probes) {
	if probes == nil {
		return
	}
	rendered := &milvusapi.Probes{
		StartupProbe:   tunedProbe(defaultStartupProbe(), probes.Startup),
		LivenessProbe:  tunedProbe(defaultLivenessProbe(), probes.Liveness),
		ReadinessProbe: tunedProbe(defaultReadinessProbe(), probes.Readiness),
	}
	if rendered.StartupProbe == nil && rendered.LivenessProbe == nil && rendered.ReadinessProbe == nil {
		return
	}
	spec.Probes = rendered
}

func tunedProbe(probe corev1.Probe, tuning *components.ProbeTuning) *corev1.Probe {
	if tuning == nil {
		return nil
	}
	if tuning.InitialDelaySeconds != nil {
		probe.InitialDelaySeconds = *tuning.InitialDelaySeconds
	}
	if tuning.PeriodSeconds != nil {
		probe.PeriodSeconds = *tuning.PeriodSeconds
	}
	if tuning.TimeoutSeconds != nil {
		probe.TimeoutSeconds = *tuning.TimeoutSeconds
	}
	if tuning.FailureThreshold != nil {
		probe.FailureThreshold = *tuning.FailureThreshold
	}
	return &probe
}

func validateProbes(name string, probes *components.Probes) error {
	if probes == nil {
		return nil
	}
	for _, probe := range []struct {
		name   string
		tuning *components.ProbeTuning
	}{{"startup", probes.Startup}, {"liveness", probes.Liveness}, {"readiness", probes.Readiness}} {
		tuning := probe.tuning
		if tuning == nil {
			continue
		}
		for _, field := range []struct {
			name  string
			value *int32
		}{{"periodSeconds", tuning.PeriodSeconds}, {"timeoutSeconds", tuning.TimeoutSeconds}, {"failureThreshold", tuning.FailureThreshold}} {
			if field.value != nil && *field.value < 1 {
				return fmt.Errorf("component %q probes.%s.%s must be at least 1", name, probe.name, field.name)
			}
		}
		if tuning.InitialDelaySeconds != nil && *tuning.InitialDelaySeconds < 0 {
			return fmt.Errorf("component %q probes.%s.initialDelaySeconds must not be negative", name, probe.name)
		}
	}
	return nil
}
