# Microservices CA 

A multi-service application demonstrating the Saga (choreography), Transactional Outbox, and CQRS patterns using FastAPI, Kafka, and Redis.

---

## Prerequisites

Docker

## How to Run

1. Clone / navigate to the project directory:

cd microsvc


2. Build and start all services:

docker compose up --build


This starts 7 containers: Zookeeper, Kafka, three Redis instances, and the three services.

3. Wait for services to be ready

Kafka takes about 15–20 seconds to fully start.

4. Verify all services are healthy:


curl http://localhost:8000/health
curl http://localhost:8001/health
curl http://localhost:8002/health


Each should return:

{"status": "ok", "service": "order-service"}

5. Stop the system:

docker compose down


---

## Testing the APIs — Step by Step

### Test 1 — Successful order

**Step 1: Check the stock level before ordering**

```bash
curl http://localhost:8001/stock/running-shoes
```

Expected response:
```json
{"product_id": "running-shoes", "stock": 100}
```

**Step 2: Place the order**

```bash
curl -s -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "cust-001",
    "items": [
      {"product_id": "shoes", "quantity": 3, "unit_price": 10.00}
    ]
  }'
```

Expected response (order total = 3 × £10 = £30, which is ≤ £500):
```json
{
  "order_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "status": "PENDING",
  "total": 30.0
}
```

Copy the `order_id` value — you will need it for the next steps.

**Step 3: Poll the order status**

Replace `<order_id>` with the value from Step 2:

```bash
curl http://localhost:8000/orders/<order_id>
```

Run this a couple of times over a few seconds. You will see the status progress:


**Step 4: Confirm stock was decremented**

```bash
curl http://localhost:8001/stock/running-shoes
```

Expected: stock is now 97 (100 − 3).

**Step 5: Confirm the payment record**

```bash
curl http://localhost:8002/payments/<order_id>
```

Expected:
```json
{
  "event_type": "payment.succeeded",
  "order_id": "...",
  "payload": {"total": 30.0, "items": [...]}
}
```

---

### Test 2 - Failed order (compensating actions)

**Step 1: Check stock before the test**

```bash
curl http://localhost:8001/stock/yoga-mat
```

Expected: 50 units.

**Step 2: Place an order that will fail payment (total = 5 × £110 = £550)**

```bash
curl -s -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "cust-002",
    "items": [
      {"product_id": "yoga-mat", "quantity": 5, "unit_price": 110.00}
    ]
  }'
```

Copy the `order_id`.

**Step 3: Poll the order status**

```bash
curl http://localhost:8000/orders/<order_id>
```

Final state will be:
```json
{
  "status": "CANCELLED",
  "event_log": ["order.created", "inventory.reserved", "payment.failed", "order.cancelled"]
}
```

Notice that `inventory.reserved` appears in the log — the stock was reserved, then payment failed, then the compensating action ran.

**Step 4: Confirm stock was restored**

```bash
curl http://localhost:8001/stock/yoga-mat
```

Expected: back to 50 — the compensating action released the reservation.

**Step 5: Confirm the payment record shows the failure**

```bash
curl http://localhost:8002/payments/<order_id>
```

Expected:
```json
{
  "event_type": "payment.failed",
  "payload": {"total": 550.0, "reason": "total exceeds limit", ...}
}
```

---

### Test 3 — Insufficient stock

Place an order for more stock than exists:

```bash
curl -s -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "cust-003",
    "items": [
      {"product_id": "yoga-mat", "quantity": 999, "unit_price": 1.00}
    ]
  }'
```

The order will be cancelled immediately - no payment is attempted, and stock is unchanged. The `event_log` will show `["order.created", "inventory.insufficient", "order.cancelled"]`.

---

