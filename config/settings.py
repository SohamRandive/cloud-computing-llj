import random
from faker import Faker

fake = Faker()
Faker.seed(99)
random.seed(99)

# ── Kafka ─────────────────────────────────────────────
KAFKA_BROKER = "localhost:9092"

TOPICS = {
    "clickstream":   "clickstream",
    "purchases":     "purchases",
    "reviews":       "product-reviews",
    "social":        "social-buzz",
}

PRODUCER_CONFIG = {
    "bootstrap_servers": KAFKA_BROKER,
    "acks": "all",
    "retries": 3,
    "linger_ms": 5,
    "batch_size": 32768,
    "compression_type": "snappy",
}

# ── Shared user pool ──────────────────────────────────
NUM_USERS = 12000

USERS = [
    {
        "user_id":   f"USR-{str(i).zfill(6)}",
        "name":      fake.name(),
        "email":     fake.email(),
        "country":   fake.country_code(),
        "age_group": random.choice(["18-24", "25-34", "35-44", "45-54", "55+"]),
        "tier":      random.choice(["platinum", "gold", "silver", "basic"]),
    }
    for i in range(1, NUM_USERS + 1)
]

USER_IDS = [u["user_id"] for u in USERS]

# ── Product catalogue ─────────────────────────────────
PRODUCT_CATEGORIES = [
    "Electronics", "Apparel", "Home & Living",
    "Books", "Fitness", "Beauty & Care", "Toys", "Groceries",
]

PRODUCTS = [
    {
        "sku":      f"SKU-{str(i).zfill(5)}",
        "title":    fake.catch_phrase(),
        "category": random.choice(PRODUCT_CATEGORIES),
        "price":    round(random.uniform(10.0, 1200.0), 2),
    }
    for i in range(1, 251)
]

SKU_IDS = [p["sku"] for p in PRODUCTS]

# ── Clickstream constants ─────────────────────────────
CLICK_EVENTS  = ["view", "search", "scroll", "add_to_wishlist", "add_to_cart", "remove_from_cart"]
PLATFORMS     = ["mobile_app", "desktop_web", "tablet_web"]
TRAFFIC_SRC   = ["organic", "paid_search", "email", "social", "referral", "direct"]

SITE_PAGES = [
    "/", "/shop", "/cart", "/checkout",
    "/account", "/search", "/offers", "/new-arrivals",
]

# ── Payment methods ───────────────────────────────────
PAYMENT_METHODS = ["card", "upi", "netbanking", "buy_now_pay_later", "wallet", "cod"]

# ── Purchase statuses ─────────────────────────────────
PURCHASE_STATUSES = ["placed", "confirmed", "dispatched", "delivered", "returned", "cancelled"]

# ── Social platforms ──────────────────────────────────
SOCIAL_PLATFORMS = ["instagram", "twitter", "facebook", "youtube"]

# ── Review texts ──────────────────────────────────────
REVIEW_TEXTS = [
    "Absolutely worth every rupee. Top-notch quality and fast shipping.",
    "Exceeded expectations. Packaging was excellent and product works great.",
    "Would highly recommend. Great value and quick delivery.",
    "Five stars! Exactly as described. Very happy with this purchase.",
    "Brilliant product. Customer support was also very responsive.",
    "It's okay for the price. Nothing extraordinary but does the job.",
    "Decent quality. Delivery was on schedule. Average overall.",
    "Product is fine. Could be better but acceptable for the cost.",
    "Neutral experience — neither impressed nor disappointed.",
    "Works as described. No issues so far.",
    "Very disappointed — quality is not what was shown in photos.",
    "Stopped functioning within a week. Complete waste of money.",
    "Arrived damaged and return process was a nightmare.",
    "Product is fake. Stay away from this seller.",
    "Extremely poor build quality. Returning immediately.",
]

SOCIAL_TEXTS = [
    "Just got my order and I am obsessed! Packaging was gorgeous 😍 #ShopStream #Haul",
    "Best online shopping experience in a long time. Super fast delivery! #ShopStream",
    "Can't stop buying from ShopStream. Quality never disappoints 🙌 #MustBuy",
    "My package arrived today. It's alright I guess. #ShopStream",
    "Ordered something new. Let's see if it's worth the hype. #OnlineShopping",
    "Got my delivery. Standard stuff, nothing special.",
    "Two weeks wait and the item came broken. So angry! #BadExperience",
    "Worst online shopping experience ever. No refund given. #Scam",
    "Quality is terrible. Completely misrepresented. #Disappointed",
]
