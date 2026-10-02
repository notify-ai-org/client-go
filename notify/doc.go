// Package notify is the Go client SDK for the Notify.ai control plane
// (acp-server). It is the Go counterpart of the Java client SDK
// (dev.notify-ai:notify-ai-agent-client) and speaks the same wire protocol.
//
// Java annotations map to Go registration calls:
//
//	@EnableNotify          notify.New(cfg) + client.Start(ctx)
//	@Model / @Vocabulary   notify.RegisterModel[T] + `notify:"name" notifyDesc:"..."` struct tags
//	@Event                 notify.Event(client, spec, fn) — wraps fn, returns a function with the same signature
//	@Rule                  notify.Rule(client, spec, fn)
//	@SubjectSupplier       notify.SubjectSupplier(client, event, description, fn)
//	@VocabularySupplier    notify.VocabularySupplier(client, event, description, fn)
//	@Callback              notify.Callback(client, event, notify.Before|notify.After, fn)
//
// Calling a wrapped event function runs BEFORE callbacks, the function itself,
// AFTER callbacks, the subject supplier and rules, then buffers an
// EventCapture that a background dispatcher sends to acp-server over HTTP
// (POST /api/event) or Kafka (see KafkaTransportFactory).
package notify
