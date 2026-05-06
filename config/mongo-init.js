// mongo-init.js — auto-runs on first container start

db = db.getSiblingDB("shopstream_profiles");

db.createCollection("user_profiles", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["user_id", "updated_at"],
            properties: {
                user_id:    { bsonType: "string" },
                updated_at: { bsonType: "date" },
            }
        }
    }
});

db.user_profiles.createIndex({ "user_id": 1 }, { unique: true });
db.user_profiles.createIndex({ "updated_at": -1 });
db.user_profiles.createIndex({ "segment.label": 1 });
db.user_profiles.createIndex({ "segment.churn_score": -1 });
db.user_profiles.createIndex({ "purchases.last_purchase_at": -1 });

print("MongoDB: shopstream_profiles ready.");
