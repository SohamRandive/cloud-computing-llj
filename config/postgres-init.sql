CREATE TABLE IF NOT EXISTS gold_revenue_hourly (
    window_start        TIMESTAMPTZ     NOT NULL,
    window_end          TIMESTAMPTZ     NOT NULL,
    product_category    VARCHAR(100)    NOT NULL,
    total_revenue       NUMERIC(14, 2)  NOT NULL DEFAULT 0,
    total_orders        INTEGER         NOT NULL DEFAULT 0,
    total_returns       INTEGER         NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, product_category)
);

CREATE TABLE IF NOT EXISTS gold_search_trends_hourly (
    window_start        TIMESTAMPTZ     NOT NULL,
    window_end          TIMESTAMPTZ     NOT NULL,
    search_term         VARCHAR(255)    NOT NULL,
    search_count        INTEGER         NOT NULL DEFAULT 0,
    unique_users        INTEGER         NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, search_term)
);

CREATE TABLE IF NOT EXISTS gold_traffic_hourly (
    window_start        TIMESTAMPTZ     NOT NULL,
    window_end          TIMESTAMPTZ     NOT NULL,
    total_sessions      INTEGER         NOT NULL DEFAULT 0,
    total_clicks        INTEGER         NOT NULL DEFAULT 0,
    total_pageviews     INTEGER         NOT NULL DEFAULT 0,
    unique_users        INTEGER         NOT NULL DEFAULT 0,
    avg_session_dur_ms  BIGINT          NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start)
);

CREATE TABLE IF NOT EXISTS gold_sentiment_hourly (
    window_start        TIMESTAMPTZ     NOT NULL,
    window_end          TIMESTAMPTZ     NOT NULL,
    source              VARCHAR(20)     NOT NULL,
    avg_sentiment       NUMERIC(5, 4)   NOT NULL,
    positive_count      INTEGER         NOT NULL DEFAULT 0,
    neutral_count       INTEGER         NOT NULL DEFAULT 0,
    negative_count      INTEGER         NOT NULL DEFAULT 0,
    total_count         INTEGER         NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, source)
);

CREATE TABLE IF NOT EXISTS gold_customer_segments (
    customer_id         VARCHAR(50)     NOT NULL,
    segment             VARCHAR(20)     NOT NULL,
    total_spent_30d     NUMERIC(12, 2)  NOT NULL DEFAULT 0,
    order_count_30d     INTEGER         NOT NULL DEFAULT 0,
    last_order_date     DATE,
    avg_review_rating   NUMERIC(3, 2),
    churn_risk_score    NUMERIC(5, 4),
    snapshot_date       DATE            NOT NULL DEFAULT CURRENT_DATE,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (customer_id, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_revenue_window   ON gold_revenue_hourly      (window_start DESC);
CREATE INDEX IF NOT EXISTS idx_search_window    ON gold_search_trends_hourly(window_start DESC);
CREATE INDEX IF NOT EXISTS idx_traffic_window   ON gold_traffic_hourly      (window_start DESC);
CREATE INDEX IF NOT EXISTS idx_sentiment_source ON gold_sentiment_hourly    (source, window_start DESC);
CREATE INDEX IF NOT EXISTS idx_segments_churn   ON gold_customer_segments   (churn_risk_score DESC);
