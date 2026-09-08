package functions

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
)

type Options struct {
	// Scale is kept for YAML deserialization of old func.yaml files (pre-0.37.0
	// stored scale under deploy.options.scale). The 0.34.0 migration writes to
	// it and the 0.37.0 migration moves it to Function.Scale. Hidden from the
	// JSON schema so new files never use this path.
	Scale     *ScaleOptions     `yaml:"scale,omitempty" jsonschema:"-"`
	Resources *ResourcesOptions `yaml:"resources,omitempty"`
}

type ScaleOptions struct {
	Min  *int64            `yaml:"min,omitempty" jsonschema_extras:"minimum=0"`
	Max  *int64            `yaml:"max,omitempty" jsonschema_extras:"minimum=0"`
	KEDA *KEDAScaleOptions `yaml:"keda,omitempty"`
	KPA  *KPAScaleOptions  `yaml:"kpa,omitempty"`
}

type KEDAScaleOptions struct {
	PollingInterval *int32        `yaml:"pollingInterval,omitempty" jsonschema_extras:"minimum=1"`
	CooldownPeriod  *int32        `yaml:"cooldownPeriod,omitempty" jsonschema_extras:"minimum=1"`
	Triggers        []KEDATrigger `yaml:"triggers,omitempty"`
}

type KEDATrigger struct {
	Type                   string `yaml:"type" jsonschema:"enum=http,enum=kafka,enum=cron"`
	TargetValue            *int64 `yaml:"targetValue,omitempty" jsonschema_extras:"minimum=1"`
	LagThreshold           *int64 `yaml:"lagThreshold,omitempty" jsonschema_extras:"minimum=1"`
	ActivationLagThreshold *int64 `yaml:"activationLagThreshold,omitempty" jsonschema_extras:"minimum=0"`
	Timezone               string `yaml:"timezone,omitempty"`
	Start                  string `yaml:"start,omitempty"`
	End                    string `yaml:"end,omitempty"`
	DesiredReplicas        *int64 `yaml:"desiredReplicas,omitempty" jsonschema_extras:"minimum=1"`
}

type KPAScaleOptions struct {
	Metric      *string  `yaml:"metric,omitempty" jsonschema:"enum=concurrency,enum=rps"`
	Target      *float64 `yaml:"target,omitempty" jsonschema_extras:"minimum=0.01"`
	Utilization *float64 `yaml:"utilization,omitempty" jsonschema:"minimum=1,maximum=100"`
}

type ResourcesOptions struct {
	Requests *ResourcesRequestsOptions `yaml:"requests,omitempty"`
	Limits   *ResourcesLimitsOptions   `yaml:"limits,omitempty"`
}

type ResourcesLimitsOptions struct {
	CPU         *string `yaml:"cpu,omitempty" jsonschema:"pattern=^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$"`
	Memory      *string `yaml:"memory,omitempty" jsonschema:"pattern=^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$"`
	Concurrency *int64  `yaml:"concurrency,omitempty" jsonschema_extras:"minimum=0"`
}

type ResourcesRequestsOptions struct {
	CPU    *string `yaml:"cpu,omitempty" jsonschema:"pattern=^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$"`
	Memory *string `yaml:"memory,omitempty" jsonschema:"pattern=^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$"`
}

// validateOptions checks that input Options are correctly set.
// Scale validation is handled separately by ValidateScale.
// Returns array of error messages, empty if no errors are found
func validateOptions(options Options) (errors []string) {

	// options.resource
	if options.Resources != nil {

		// options.resource.requests
		if options.Resources.Requests != nil {

			if options.Resources.Requests.CPU != nil {
				_, err := resource.ParseQuantity(*options.Resources.Requests.CPU)
				if err != nil {
					errors = append(errors, fmt.Sprintf("options field \"resources.requests.cpu\" has invalid value set: \"%s\"; \"%s\"",
						*options.Resources.Requests.CPU, err.Error()))
				}
			}

			if options.Resources.Requests.Memory != nil {
				_, err := resource.ParseQuantity(*options.Resources.Requests.Memory)
				if err != nil {
					errors = append(errors, fmt.Sprintf("options field \"resources.requests.memory\" has invalid value set: \"%s\"; \"%s\"",
						*options.Resources.Requests.Memory, err.Error()))
				}
			}
		}

		// options.resource.limits
		if options.Resources.Limits != nil {

			if options.Resources.Limits.CPU != nil {
				_, err := resource.ParseQuantity(*options.Resources.Limits.CPU)
				if err != nil {
					errors = append(errors, fmt.Sprintf("options field \"resources.limits.cpu\" has invalid value set: \"%s\"; \"%s\"",
						*options.Resources.Limits.CPU, err.Error()))
				}
			}

			if options.Resources.Limits.Memory != nil {
				_, err := resource.ParseQuantity(*options.Resources.Limits.Memory)
				if err != nil {
					errors = append(errors, fmt.Sprintf("options field \"resources.limits.memory\" has invalid value set: \"%s\"; \"%s\"",
						*options.Resources.Limits.Memory, err.Error()))
				}
			}

			if options.Resources.Limits.Concurrency != nil {
				if *options.Resources.Limits.Concurrency < 0 {
					errors = append(errors, fmt.Sprintf("options field \"resources.limits.concurrency\" has value set to \"%d\", but it must not be less than 0",
						*options.Resources.Limits.Concurrency))
				}
			}
		}
	}

	return
}

