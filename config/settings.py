import random
from faker import Faker

fake = Faker()
Faker.seed(42)          # reproducible names/emails across runs
random.seed(42)

# ── Kafka ─────────────────────────────────────────────
KAFKA_BROKER = "localhost:9092"

TOPICS = {
    "web_logs":    "web-logs",
    "transactions":"transactions",
    "reviews":     "reviews",
    "social":      "social-media",
}

PRODUCER_CONFIG = {
    "bootstrap_servers": KAFKA_BROKER,
    "acks": "all",                  # wait for broker ack
    "retries": 3,
    "linger_ms": 10,                # micro-batch up to 10ms for throughput
    "batch_size": 16384,            # 16KB batch
    "compression_type": "gzip",
}

# ── Shared customer pool ──────────────────────────────
# 500 customers shared across ALL generators so that
# customer_id joins work correctly in the gold layer.
NUM_CUSTOMERS = 10000

CUSTOMERS = [
    {
        "customer_id": f"CUST-{str(i).zfill(5)}",  # CUST-00001 … CUST-00500
        "name":        fake.name(),
        "email":       fake.email(),
        "country":     fake.country_code(),
        "age_group":   random.choice(["18-24", "25-34", "35-44", "45-54", "55+"]),
        "segment":     random.choice(["vip", "regular", "new", "at_risk"]),
    }
    for i in range(1, NUM_CUSTOMERS + 1)
]

CUSTOMER_IDS = [c["customer_id"] for c in CUSTOMERS]

# ── Product catalogue ─────────────────────────────────
PRODUCT_CATEGORIES = [
    "Electronics", "Clothing", "Home & Kitchen",
    "Books", "Sports", "Beauty", "Toys", "Grocery",
]

PRODUCTS = [
    {"product_id": f"PROD-{str(i).zfill(4)}",
     "name": fake.catch_phrase(),
     "category": random.choice(PRODUCT_CATEGORIES),
     "price": round(random.uniform(5.0, 999.0), 2)}
    for i in range(1, 201)          # 200 products
]

PRODUCT_IDS = [p["product_id"] for p in PRODUCTS]

# ── Web log constants ─────────────────────────────────
EVENT_TYPES   = ["click", "search", "pageview", "add_to_cart", "remove_from_cart"]
DEVICES       = ["mobile", "desktop", "tablet"]
REFERRERS     = ["google", "direct", "facebook", "instagram", "email", "twitter"]

PAGES = [
    "/", "/products", "/cart", "/checkout",
    "/orders", "/account", "/search", "/deals",
]

# ── Payment methods ───────────────────────────────────
PAYMENT_METHODS = ["credit_card", "debit_card", "upi", "netbanking", "wallet"]

# ── Order statuses ────────────────────────────────────
ORDER_STATUSES = ["placed", "confirmed", "shipped", "delivered", "returned", "cancelled"]

# ── Social platforms ──────────────────────────────────
PLATFORMS = ["twitter", "facebook", "instagram"]

# ── Review templates (mixed sentiment for VADER) ──────
REVIEW_TEXTS = [
    # positive
    "Absolutely love this product! Exceeded my expectations.",
    "Great quality, fast delivery. Will definitely buy again.",
    "Amazing value for money. Highly recommend to everyone.",
    "Perfectly packaged, works flawlessly. Very happy with purchase.",
    "Outstanding product. Customer service was also excellent.",
    # neutral
    "Product is okay. Nothing special but does the job.",
    "Decent quality for the price. Average experience overall.",
    "It works as described. No complaints, no praise either.",
    "Delivery was on time. Product is what I expected.",
    "Fine product, could be better but acceptable.",
    # negative
    "Very disappointed. Quality is much worse than advertised.",
    "Stopped working after 2 days. Complete waste of money.",
    "Terrible experience. Would not recommend to anyone.",
    "Product arrived damaged. Customer support was unhelpful.",
    "Extremely poor quality. Returning this immediately.",
]

SOCIAL_TEMPLATES = [
    # positive
    "Just received my order from this store and I'm loving it! #shopping #happy",
    "Best online shopping experience ever! Fast delivery and great quality.",
    "Totally obsessed with my new purchase. Worth every penny! #mustbuy",
    # neutral
    "My order arrived today. It's okay I guess. #shopping",
    "Ordered something online. Let's see how it goes. #shopping",
    "Got my package. Standard quality, nothing extraordinary.",
    # negative
    "Waited 2 weeks for my order and it came damaged. So frustrated! #badservice",
    "Worst shopping experience. Never ordering from here again. #disappointed",
    "Product quality is terrible. Complete scam. #angry #donotbuy",
]
