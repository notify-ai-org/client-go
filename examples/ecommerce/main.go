// Command ecommerce is the Go port of examples/ecommerce-app's order flow,
// exercising every SDK registration type against a running acp-server.
//
//	NOTIFY_AI_ACP_SERVER_URL=http://localhost:8080 \
//	NOTIFY_AI_CLIENT_TOKEN=client-... go run ./examples/ecommerce
//
//	curl -X POST localhost:8091/api/orders/place \
//	  -d '{"orderId":"ORD-1","customerId":"CUST-1","amount":120,"items":["Mechanical Keyboard"]}'
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/notify-ai-org/client-go/notify"
)

// ── Models (@Model / @Vocabulary) ───────────────────────────────────────────

type OrderPayload struct {
	OrderID         string   `json:"orderId" notify:"orderId" notifyDesc:"Unique order identifier"`
	CustomerID      string   `json:"customerId" notify:"customerId" notifyDesc:"Customer who placed the order"`
	Amount          float64  `json:"amount" notify:"amount" notifyDesc:"Total order amount in USD"`
	Items           []string `json:"items" notify:"items" notifyDesc:"List of item names in the order"`
	ShippingAddress string   `json:"shippingAddress" notify:"shippingAddress" notifyDesc:"Delivery address for the order"`
}

func (OrderPayload) NotifyModelDescription() string {
	return "Payload for order placement and payment events"
}

type ShipmentPayload struct {
	OrderID           string `json:"orderId" notify:"orderId" notifyDesc:"Order being shipped"`
	TrackingNumber    string `json:"trackingNumber" notify:"trackingNumber" notifyDesc:"Carrier tracking number"`
	Carrier           string `json:"carrier" notify:"carrier" notifyDesc:"Shipping carrier name"`
	EstimatedDelivery string `json:"estimatedDelivery" notify:"estimatedDelivery" notifyDesc:"Estimated delivery date (YYYY-MM-DD)"`
}

func (ShipmentPayload) NotifyModelDescription() string { return "Payload for shipment tracking events" }

type CartPayload struct {
	CartID         string   `json:"cartId" notify:"cartId" notifyDesc:"Unique cart identifier"`
	CustomerID     string   `json:"customerId" notify:"customerId" notifyDesc:"Customer who owns the cart"`
	Items          []string `json:"items" notify:"items" notifyDesc:"Items left in the cart"`
	LastActivityAt string   `json:"lastActivityAt" notify:"lastActivityAt" notifyDesc:"Timestamp of last cart activity (ISO-8601)"`
}

func (CartPayload) NotifyModelDescription() string { return "Payload for abandoned cart events" }

type Customer struct{ ID, Name, Email, Phone string }

// ── Service ─────────────────────────────────────────────────────────────────

type OrderService struct {
	mu        sync.Mutex
	customers map[string]Customer
	orders    map[string]OrderPayload
}

func (s *OrderService) PlaceOrder(_ context.Context, p OrderPayload) (OrderPayload, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.orders[p.OrderID] = p
	log.Printf("📦 Order placed: %s for customer %s — $%.2f", p.OrderID, p.CustomerID, p.Amount)
	return p, nil
}

func (s *OrderService) ReportPaymentFailed(_ context.Context, p OrderPayload) (OrderPayload, error) {
	log.Printf("💳 Payment FAILED for order: %s — $%.2f", p.OrderID, p.Amount)
	return p, nil
}

func (s *OrderService) ShipOrder(_ context.Context, p ShipmentPayload) (ShipmentPayload, error) {
	log.Printf("Order shipped: %s via %s — tracking: %s", p.OrderID, p.Carrier, p.TrackingNumber)
	return p, nil
}

func (s *OrderService) AbandonCart(_ context.Context, p CartPayload) (CartPayload, error) {
	log.Printf("🛒 Cart abandoned: %s by customer %s", p.CartID, p.CustomerID)
	return p, nil
}

func (s *OrderService) customer(id string) (Customer, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.customers[id]
	return c, ok
}

func (s *OrderService) order(id string) (OrderPayload, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.orders[id]
	return o, ok
}

func emailTo(c Customer) []notify.Subject {
	return []notify.Subject{notify.NewEmailSubject(c.Email, "", "", "", map[string]string{"firstName": c.Name})}
}

func main() {
	cfg := notify.ConfigFromEnv()
	if cfg.ApplicationName == "" {
		cfg.ApplicationName = "ecommerce-app-go"
	}
	if cfg.ACPServerURL == "" {
		cfg.ACPServerURL = "http://localhost:8080"
	}
	cfg.BasePackage = "github.com/notify-ai-org/client-go/examples/ecommerce"
	client := notify.New(cfg)

	svc := &OrderService{
		customers: map[string]Customer{
			"CUST-1": {"CUST-1", "Alice Johnson", "alice@example.com", "+1-555-0101"},
			"CUST-2": {"CUST-2", "Bob Smith", "bob@example.com", "+1-555-0102"},
			"CUST-3": {"CUST-3", "Carol Davis", "carol@example.com", "+1-555-0103"},
		},
		orders: map[string]OrderPayload{},
	}

	// ── Events (@Event) ─────────────────────────────────────────────────────
	placeOrder := notify.Event(client, notify.EventSpec{Key: "ORDER_PLACED", Description: "Customer placed an order",
		EventType: "static", ScheduleIntent: "immediate", PreferredTimeWindow: "09:00-18:00", Priority: 2}, svc.PlaceOrder)
	paymentFailed := notify.Event(client, notify.EventSpec{Key: "PAYMENT_FAILED", Description: "Payment processing failed for an order",
		EventType: "static", ScheduleIntent: "immediate", PreferredTimeWindow: "00:00-23:59", Priority: 4}, svc.ReportPaymentFailed)
	shipOrder := notify.Event(client, notify.EventSpec{Key: "ORDER_SHIPPED", Description: "Order has been shipped to the customer",
		EventType: "static", ScheduleIntent: "immediate", PreferredTimeWindow: "09:00-18:00", Priority: 5}, svc.ShipOrder)
	abandonCart := notify.Event(client, notify.EventSpec{Key: "ABANDONED_CART", Description: "Customer abandoned their shopping cart",
		EventType: "deferred", ScheduleIntent: "deferred", PreferredTimeWindow: "09:00-21:00", Priority: 3}, svc.AbandonCart)

	// ── Subject suppliers (@SubjectSupplier) ────────────────────────────────
	notify.SubjectSupplier(client, "ORDER_PLACED", "Resolves order customer to email recipients",
		func(_ context.Context, p OrderPayload) ([]notify.Subject, error) {
			c, ok := svc.customer(p.CustomerID)
			if !ok {
				return nil, nil
			}
			return emailTo(c), nil
		})
	notify.SubjectSupplier(client, "PAYMENT_FAILED", "Resolves customer to SMS for urgent payment alerts",
		func(_ context.Context, p OrderPayload) ([]notify.Subject, error) {
			c, ok := svc.customer(p.CustomerID)
			if !ok {
				return nil, nil
			}
			return []notify.Subject{notify.NewSmsSubject(c.Phone, "", map[string]string{"firstName": c.Name})}, nil
		})
	notify.SubjectSupplier(client, "ORDER_SHIPPED", "Resolves order to email recipients for shipment tracking",
		func(_ context.Context, p ShipmentPayload) ([]notify.Subject, error) {
			o, ok := svc.order(p.OrderID)
			if !ok {
				return nil, nil
			}
			c, ok := svc.customer(o.CustomerID)
			if !ok {
				return nil, nil
			}
			return emailTo(c), nil
		})
	notify.SubjectSupplier(client, "ABANDONED_CART", "Resolves cart owner to email for re-engagement",
		func(_ context.Context, p CartPayload) ([]notify.Subject, error) {
			c, ok := svc.customer(p.CustomerID)
			if !ok {
				return nil, nil
			}
			return emailTo(c), nil
		})

	// ── Vocabulary supplier (@VocabularySupplier) ───────────────────────────
	notify.VocabularySupplier(client, "ORDER_PLACED", "Enriches order payload with a default shipping address",
		func(_ context.Context, p OrderPayload) (any, error) {
			if c, ok := svc.customer(p.CustomerID); ok && p.ShippingAddress == "" {
				p.ShippingAddress = "Default address for " + c.Name
			}
			return p, nil
		})

	// ── Rules (@Rule) ───────────────────────────────────────────────────────
	notify.Rule(client, notify.RuleSpec{Name: "fraud-check", Event: "ORDER_PLACED", Description: "Blocks orders over $1000 as potential fraud"},
		func(_ context.Context, p OrderPayload) (bool, error) { return p.Amount < 1000, nil })
	notify.Rule(client, notify.RuleSpec{Name: "inventory-check", Event: "ORDER_PLACED", Description: "Checks whether all items are in stock"},
		func(context.Context, OrderPayload) (bool, error) { return true, nil })

	// ── Callbacks (@Callback) ───────────────────────────────────────────────
	notify.Callback(client, "ORDER_PLACED", notify.Before, func(_ context.Context, p OrderPayload) error {
		log.Printf("⏳ [BEFORE] About to process ORDER_PLACED for order: %s", p.OrderID)
		return nil
	})
	notify.Callback(client, "ORDER_PLACED", notify.After, func(_ context.Context, p OrderPayload) error {
		log.Printf("✅ [AFTER] ORDER_PLACED processing complete for order: %s", p.OrderID)
		return nil
	})

	if err := client.Start(); err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/orders/place", handle(placeOrder, func(p OrderPayload) any {
		return map[string]any{"status": "ORDER_PLACED", "orderId": p.OrderID, "amount": p.Amount}
	}))
	mux.HandleFunc("POST /api/orders/payment-failed", handle(paymentFailed, func(p OrderPayload) any {
		return map[string]any{"status": "PAYMENT_FAILED", "orderId": p.OrderID}
	}))
	mux.HandleFunc("POST /api/orders/ship", handle(shipOrder, func(p ShipmentPayload) any {
		return map[string]any{"status": "ORDER_SHIPPED", "orderId": p.OrderID, "trackingNumber": p.TrackingNumber}
	}))
	mux.HandleFunc("POST /api/orders/abandon-cart", handle(abandonCart, func(p CartPayload) any {
		return map[string]any{"status": "ABANDONED_CART", "cartId": p.CartID}
	}))

	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8091"
	}
	server := &http.Server{Addr: addr, Handler: mux}
	go func() {
		slog.Info("ecommerce example listening", "addr", addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	stop, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	<-stop.Done()
	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()
	_ = server.Shutdown(ctx)
	if err := client.Close(ctx); err != nil {
		slog.Warn("notify close", "error", err)
	}
}

func handle[P, R any](event func(context.Context, P) (R, error), reply func(P) any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var p P
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if _, err := event(r.Context(), p); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(reply(p))
	}
}
