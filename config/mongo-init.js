// mongo-init.js — runs via /docker-entrypoint-initdb.d/ on first start

db = db.getSiblingDB("llj_cvs");

db.createCollection("customer_profiles", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["customer_id", "updated_at"],
      properties: {
        customer_id: { bsonType: "string" },
        updated_at:  { bsonType: "date"   },
        transactions: {
          bsonType: "object",
          properties: {
            total_orders:      { bsonType: "int"    },
            total_spent:       { bsonType: "double" },
            total_returns:     { bsonType: "int"    },
            last_order_at:     { bsonType: "date"   },
            preferred_payment: { bsonType: "string" }
          }
        },
        reviews: {
          bsonType: "object",
          properties: {
            avg_rating:      { bsonType: "double" },
            total_reviews:   { bsonType: "int"    },
            sentiment_score: { bsonType: "double" }
          }
        },
        web_behaviour: {
          bsonType: "object",
          properties: {
            total_sessions:  { bsonType: "int"   },
            total_clicks:    { bsonType: "int"   },
            total_searches:  { bsonType: "int"   },
            last_active_at:  { bsonType: "date"  },
            top_pages:       { bsonType: "array" }
          }
        },
        social: {
          bsonType: "object",
          properties: {
            post_count:    { bsonType: "int"    },
            avg_sentiment: { bsonType: "double" },
            platforms:     { bsonType: "array"  }
          }
        }
      }
    }
  },
  validationAction: "warn"
});

db.customer_profiles.createIndex({ customer_id: 1 }, { unique: true });
db.customer_profiles.createIndex({ updated_at: -1 });
db.customer_profiles.createIndex({ "transactions.total_spent": -1 });
db.customer_profiles.createIndex({ "reviews.sentiment_score": 1  });

db.createCollection("raw_events");
db.raw_events.createIndex({ customer_id: 1, event_type: 1, timestamp: -1 });

print("MongoDB: llj_cvs collections and indexes ready.");
