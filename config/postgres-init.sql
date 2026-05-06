-- ShopStream Data Platform — PostgreSQL Gold Layer
-- Tables store windowed aggregations from PySpark gold job

CREATE TABLE IF NOT EXISTS hourly_sales (
    window_start        TIMESTAMPTZ     NOT NULL,
    window_end          TIMESTAMPTZ     NOT NULL,
    category            VARCHAR(120)    NOT NULL,
    gross_revenue       NUMERIC(16, 2)  NOT NULL DEFAULT 0,
    num_orders          INTEGER         NOT NULL DEFAULT 0,
    num_returns         INTEGER         NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, category)
);

CREATE TABLE IF NOT EXISTS hourly_search_trends (
    window_start        TIMESTAMPTZ     NOT NULL,
    window_end          TIMESTAMPTZ     NOT NULL,
    keyword             VARCHAR(280)    NOT NULL,
    search_count        INTEGER         NOT NULL DEFAULT 0,
    unique_visitors     INTEGER         NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, keyword)
);

CREATE TABLE IF NOT EXISTS hourly_site_traffic (
    window_start        TIMESTAMPTZ     NOT NULL,
    window_end          TIMESTAMPTZ     NOT NULL,
    num_sessions        INTEGER         NOT NULL DEFAULT 0,
    num_clicks          INTEGER         NOT NULL DEFAULT 0,
    num_pageviews       INTEGER         NOT NULL DEFAULT 0,
    unique_visitors     INTEGER         NOT NULL DEFAULT 0,
    avg_session_ms      BIGINT          NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start)
);

CREATE TABLE IF NOT EXISTS hourly_sentiment (
    window_start        TIMESTAMPTZ     NOT NULL,
    window_end          TIMESTAMPTZ     NOT NULL,
    data_source         VARCHAR(60)     NOT NULL,
    mean_sentiment      NUMERIC(7, 4)   NOT NULL DEFAULT 0,
    positive_cnt        INTEGER         NOT NULL DEFAULT 0,
    neutral_cnt         INTEGER         NOT NULL DEFAULT 0,
    negative_cnt        INTEGER         NOT NULL DEFAULT 0,
    total_cnt           INTEGER         NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, data_source)
);

CREATE TABLE IF NOT EXISTS user_segments (
    window_start        TIMESTAMPTZ     NOT NULL,
    window_end          TIMESTAMPTZ     NOT NULL,
    user_id             VARCHAR(20)     NOT NULL,
    segment             VARCHAR(20)     NOT NULL,
    revenue_30d         NUMERIC(16, 2)  NOT NULL DEFAULT 0,
    order_count_30d     INTEGER         NOT NULL DEFAULT 0,
    last_purchase_date  DATE,
    avg_review_score    NUMERIC(4, 2),
    churn_score         NUMERIC(6, 4)   NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (window_start, user_id)
);

-- Indexes for dashboard query performance
CREATE INDEX IF NOT EXISTS idx_hourly_sales_window ON hourly_sales (window_start DESC);
CREATE INDEX IF NOT EXISTS idx_hourly_sales_cat    ON hourly_sales (category);
CREATE INDEX IF NOT EXISTS idx_search_trends_kw    ON hourly_search_trends (keyword);
CREATE INDEX IF NOT EXISTS idx_sentiment_src       ON hourly_sentiment (data_source);
CREATE INDEX IF NOT EXISTS idx_user_seg_segment    ON user_segments (segment);
CREATE INDEX IF NOT EXISTS idx_user_seg_churn      ON user_segments (churn_score DESC);
