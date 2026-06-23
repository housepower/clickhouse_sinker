package statistics

import "testing"

func TestErrorBypassMetricsRegistered(t *testing.T) {
	// 仅验证 metric 变量已构造且 label 维度正确,不 panic 即可。
	MsgsDroppedTotal.WithLabelValues("t", "data").Inc()
	MsgsDeadLetteredTotal.WithLabelValues("t", "transient").Inc()
	DeadLetterErrorsTotal.WithLabelValues("t").Inc()
	TaskQuarantinedTotal.WithLabelValues("t", "structural").Inc()
}
