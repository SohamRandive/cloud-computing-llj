# dashboard/app.py — Cloud Computing LLJ Dashboard
import sys
import time
from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

sys.path.insert(0, str(Path(__file__).parent.parent))

st.set_page_config(
    page_title="Cloud Computing LLJ",
    page_icon="☁️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    [data-testid="metric-container"] {
        background: #1a1a2e;
        border-radius: 10px;
        padding: 14px 18px;
        border: 1px solid #2d2d45;
        border-left: 4px solid #7c3aed;
    }
    div[data-testid="stSidebarNav"] { display: none; }
    .live-badge {
        display: inline-block;
        background: #22c55e;
        color: white;
        font-size: 0.75rem;
        font-weight: 700;
        padding: 3px 10px;
        border-radius: 999px;
        margin-right: 10px;
        animation: pulse 1.5s infinite;
    }
    .dead-badge {
        display: inline-block;
        background: #ef4444;
        color: white;
        font-size: 0.75rem;
        font-weight: 700;
        padding: 3px 10px;
        border-radius: 999px;
        margin-right: 10px;
    }
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50%       { opacity: 0.35; }
    }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────
#  DB HELPERS — fresh connection each render,
#  graceful fallback if service is down
# ─────────────────────────────────────────────────────

def get_pg_conn():
    """Returns a live psycopg2 connection or None."""
    try:
        import psycopg2
        conn = psycopg2.connect(
            host="localhost", port=5432,
            dbname="llj_gold",
            user="llj_user",
            password="llj_pg_pass",
            connect_timeout=3,
        )
        return conn
    except Exception:
        return None

def get_mongo_col():
    """Returns a live MongoDB collection or None."""
    try:
        from pymongo import MongoClient
        client = MongoClient(
            "mongodb://admin:llj_mongo_pass@localhost:27017/",
            serverSelectionTimeoutMS=2000,
            connectTimeoutMS=2000,
        )
        client.admin.command("ping")   # verify connection is live
        return client["llj_cvs"]["customer_profiles"]
    except Exception:
        return None

def qpg(sql: str) -> pd.DataFrame:
    """Run a PostgreSQL query. Returns empty DataFrame on any error."""
    conn = get_pg_conn()
    if conn is None:
        return pd.DataFrame()
    try:
        df = pd.read_sql(sql, conn)
        conn.close()
        return df
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        return pd.DataFrame()

def mongo_query(fn):
    """
    Run fn(col) safely. Returns None if MongoDB is down.
    Usage: mongo_query(lambda col: col.count_documents({}))
    """
    col = get_mongo_col()
    if col is None:
        return None
    try:
        return fn(col)
    except Exception:
        return None

# ─────────────────────────────────────────────────────
#  SERVICE STATUS CHECK
# ─────────────────────────────────────────────────────

pg_ok    = get_pg_conn() is not None
mongo_ok = get_mongo_col() is not None

# ─────────────────────────────────────────────────────
#  SIDEBAR
# ─────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ☁️ Cloud Computing LLJ")
    st.caption("Real-time Analytics Dashboard")
    st.divider()

    page = st.radio(
        "Navigate",
        ["📈 Overview", "🔍 Search Trends",
         "😊 Sentiment", "👤 Customer 360", "🚨 Churn Risk"],
        label_visibility="collapsed",
    )

    st.divider()
    st.markdown("### ⚡ Live Refresh")
    auto_refresh  = st.toggle("Auto-refresh", value=True)
    interval_secs = st.select_slider(
        "Interval (seconds)", options=[3, 5, 10, 15, 30], value=5
    )
    if st.button("🔄 Refresh Now", use_container_width=True):
        st.rerun()

    st.divider()

    # service health
    st.markdown("### 🔌 Services")
    st.markdown(f"{'✅' if pg_ok else '❌'} PostgreSQL")
    st.markdown(f"{'✅' if mongo_ok else '❌'} MongoDB")

    if not pg_ok:
        st.warning("PostgreSQL offline.\nRun: `make up`")
    if not mongo_ok:
        st.warning("MongoDB offline.\nRun: `make up`")

    st.divider()

    # live MongoDB sidebar stats
    if mongo_ok:
        count = mongo_query(lambda c: c.count_documents({})) or 0
        segs  = mongo_query(lambda c: {
            s["_id"]: s["count"]
            for s in c.aggregate([
                {"$group": {"_id": "$segment.label", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ])
        }) or {}

        st.markdown("### 📊 Live MongoDB Stats")
        st.metric("Customer Profiles", f"{count:,}")
        ICONS = {"vip":"🟣","regular":"🔵","at_risk":"🔴","new":"🟢","unknown":"⚪"}
        for label, cnt in segs.items():
            st.markdown(f"{ICONS.get(label,'⚪')} **{label.title()}**: {cnt:,}")
    else:
        st.markdown("### 📊 MongoDB Stats")
        st.info("Unavailable — start Docker stack")

# ─────────────────────────────────────────────────────
#  AUTO-REFRESH (JS timer — never blocks Python)
# ─────────────────────────────────────────────────────
refresh_count = 0
if auto_refresh:
    refresh_count = st_autorefresh(
        interval=interval_secs * 1000,
        limit=None,
        key="dashboard_autorefresh",
    )

# status badge
if pg_ok and mongo_ok:
    st.markdown(
        f'<span class="live-badge">● LIVE</span>'
        f'<span style="font-size:0.8rem;color:#888">'
        f'Auto-refresh {"ON" if auto_refresh else "OFF"} — '
        f'{interval_secs}s — refresh #{refresh_count}</span>',
        unsafe_allow_html=True
    )
else:
    services_down = []
    if not pg_ok:    services_down.append("PostgreSQL")
    if not mongo_ok: services_down.append("MongoDB")
    st.markdown(
        f'<span class="dead-badge">● OFFLINE</span>'
        f'<span style="font-size:0.8rem;color:#ef4444">'
        f'{", ".join(services_down)} unreachable — run `make up`</span>',
        unsafe_allow_html=True
    )

# ─────────────────────────────────────────────────────
#  EMPTY STATE HELPER
# ─────────────────────────────────────────────────────
def no_data(msg="No data available yet."):
    st.info(f"ℹ️ {msg}")

# ═════════════════════════════════════════════════════
#  PAGE 1: OVERVIEW
# ═════════════════════════════════════════════════════
if page == "📈 Overview":
    st.title("📈 Platform Overview")

    if not pg_ok and not mongo_ok:
        st.error("Both PostgreSQL and MongoDB are offline. Run `make up` to start the stack.")
        st.stop()

    rev_df  = qpg("SELECT SUM(total_revenue) as rev, SUM(total_orders) as orders, SUM(total_returns) as returns FROM gold_revenue_hourly")
    traf_df = qpg("SELECT SUM(total_sessions) as sessions, SUM(unique_users) as users, AVG(avg_session_dur_ms) as avg_dur FROM gold_traffic_hourly")
    seg_df  = qpg("SELECT segment, COUNT(*) as count FROM gold_customer_segments GROUP BY segment ORDER BY count DESC")

    def _val(df, col, default=0, cast=float):
        try:
            v = df[col].iloc[0]
            return cast(v) if v is not None else default
        except Exception:
            return default

    total_rev    = _val(rev_df,  "rev",      0, float)
    total_orders = _val(rev_df,  "orders",   0, int)
    tot_returns  = _val(rev_df,  "returns",  0, int)
    ret_rate     = round((tot_returns/total_orders*100) if total_orders else 0, 1)
    sessions     = _val(traf_df, "sessions", 0, int)
    avg_dur      = round(_val(traf_df, "avg_dur", 0, float)/1000, 1)
    mongo_count  = mongo_query(lambda c: c.count_documents({})) or 0

    c1,c2,c3,c4,c5,c6 = st.columns(6)
    c1.metric("💰 Revenue",      f"₹{total_rev:,.0f}"   if pg_ok    else "—")
    c2.metric("📦 Orders",       f"{total_orders:,}"    if pg_ok    else "—")
    c3.metric("↩️ Return Rate",  f"{ret_rate}%"         if pg_ok    else "—")
    c4.metric("👥 Sessions",     f"{sessions:,}"        if pg_ok    else "—")
    c5.metric("⏱️ Avg Session",  f"{avg_dur}s"          if pg_ok    else "—")
    c6.metric("🗃️ SCV Profiles", f"{mongo_count:,}"     if mongo_ok else "—")

    st.divider()
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Revenue by Category")
        if not pg_ok:
            no_data("PostgreSQL offline.")
        else:
            cat_df = qpg("""
                SELECT product_category,
                       SUM(total_revenue) as revenue,
                       SUM(total_orders)  as orders
                FROM gold_revenue_hourly
                GROUP BY product_category ORDER BY revenue DESC
            """)
            if not cat_df.empty:
                st.bar_chart(cat_df.set_index("product_category")["revenue"])
                st.dataframe(
                    cat_df.style.format({"revenue":"₹{:,.2f}","orders":"{:,}"}),
                    use_container_width=True, hide_index=True
                )
            else:
                no_data("Run `make pipeline` to generate gold data.")

    with col2:
        st.subheader("Customer Segments")
        if not pg_ok:
            no_data("PostgreSQL offline.")
        elif not seg_df.empty:
            ICONS = {"vip":"🟣","regular":"🔵","at_risk":"🔴","new":"🟢","unknown":"⚪"}
            seg_df["Label"]   = seg_df["segment"].map(lambda s: f"{ICONS.get(s,'⚪')} {s.title()}")
            seg_df["Share %"] = (seg_df["count"] / seg_df["count"].sum() * 100).round(1)
            st.bar_chart(seg_df.set_index("Label")["count"])
            st.dataframe(
                seg_df[["Label","count","Share %"]].rename(columns={"count":"Count"}),
                use_container_width=True, hide_index=True
            )
        else:
            no_data("Run `make pipeline` to generate segment data.")

    st.subheader("Traffic Metrics")
    if not pg_ok:
        no_data("PostgreSQL offline.")
    else:
        tr_df = qpg("""
            SELECT window_start, total_sessions, total_clicks,
                   total_pageviews, unique_users, avg_session_dur_ms
            FROM gold_traffic_hourly ORDER BY window_start
        """)
        if tr_df.empty:
            no_data("No traffic data yet. Run `make pipeline`.")
        elif len(tr_df) == 1:
            row = tr_df.iloc[0]
            st.caption(
                f"Only 1 hour window so far "
                f"({pd.to_datetime(row['window_start']).strftime('%Y-%m-%d %H:%M')} UTC). "
                f"Run generators longer and re-run `make pipeline` to see multi-hour trends."
            )
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Sessions",     int(row["total_sessions"]))
            c2.metric("Clicks",       int(row["total_clicks"]))
            c3.metric("Pageviews",    int(row["total_pageviews"]))
            c4.metric("Unique Users", int(row["unique_users"]))
            st.bar_chart(pd.DataFrame({
                "Event Type": ["Sessions", "Clicks", "Pageviews", "Unique Users"],
                "Count": [int(row["total_sessions"]), int(row["total_clicks"]),
                          int(row["total_pageviews"]), int(row["unique_users"])],
            }).set_index("Event Type"))
        else:
            tr_df["window_start"] = pd.to_datetime(tr_df["window_start"])
            st.line_chart(tr_df.set_index("window_start")[
                ["total_sessions","total_clicks","total_pageviews","unique_users"]
            ])

# ═════════════════════════════════════════════════════
#  PAGE 2: SEARCH TRENDS
# ═════════════════════════════════════════════════════
elif page == "🔍 Search Trends":
    st.title("🔍 Search Trends")

    if not pg_ok:
        st.error("PostgreSQL offline. Run `make up`.")
        st.stop()

    c1,c2 = st.columns(2)
    top_n = c1.slider("Top N terms", 5, 50, 20)
    min_s = c2.number_input("Min count", 1, 50, 2)

    trend_df = qpg(f"""
        SELECT search_term,
               SUM(search_count) as total_searches,
               SUM(unique_users) as total_users
        FROM gold_search_trends_hourly
        WHERE search_count >= {min_s}
        GROUP BY search_term
        ORDER BY total_searches DESC LIMIT {top_n}
    """)
    if not trend_df.empty:
        st.bar_chart(trend_df.set_index("search_term")["total_searches"])
        trend_df["searches/user"] = (trend_df["total_searches"]/trend_df["total_users"]).round(2)
        st.dataframe(
            trend_df.style.format({"total_searches":"{:,}","total_users":"{:,}"}),
            use_container_width=True, hide_index=True
        )
    else:
        no_data("No search data. Run `make pipeline`.")

# ═════════════════════════════════════════════════════
#  PAGE 3: SENTIMENT
# ═════════════════════════════════════════════════════
elif page == "😊 Sentiment":
    st.title("😊 Sentiment Analysis")
    st.caption("VADER compound scores — reviews vs social media")

    # live MongoDB sentiment summary
    if mongo_ok:
        live = mongo_query(lambda c: list(c.aggregate([
            {"$match": {"reviews.avg_sentiment_score": {"$exists": True}}},
            {"$group": {
                "_id":         None,
                "avg_review":  {"$avg": "$reviews.avg_sentiment_score"},
                "avg_social":  {"$avg": "$social.avg_sentiment"},
                "total":       {"$sum": 1},
            }}
        ])))
        if live:
            l = live[0]
            st.info(
                f"📡 **Live from MongoDB** — {l['total']:,} profiles | "
                f"Avg review sentiment: **{l.get('avg_review', 0):.3f}** | "
                f"Avg social sentiment: **{l.get('avg_social', 0):.3f}**"
            )
    else:
        st.warning("MongoDB offline — live stats unavailable.")

    if not pg_ok:
        st.error("PostgreSQL offline. Run `make up`.")
        st.stop()

    sent_df = qpg("SELECT source, avg_sentiment, positive_count, neutral_count, negative_count, total_count FROM gold_sentiment_hourly ORDER BY source")
    if not sent_df.empty:
        for _, row in sent_df.iterrows():
            source  = row["source"].title()
            total   = int(row["total_count"]) or 1
            score   = round(float(row["avg_sentiment"]), 3)
            emoji   = "😊" if score > 0.05 else ("😐" if score > -0.05 else "😞")
            pos_pct = round(row["positive_count"]/total*100, 1)
            neu_pct = round(row["neutral_count"] /total*100, 1)
            neg_pct = round(row["negative_count"]/total*100, 1)

            st.subheader(f"{emoji} {source}")
            c1,c2,c3,c4 = st.columns(4)
            c1.metric("Avg VADER", f"{score:+.3f}")
            c2.metric("😊 Positive", f"{pos_pct}% ({int(row['positive_count'])})")
            c3.metric("😐 Neutral",  f"{neu_pct}% ({int(row['neutral_count'])})")
            c4.metric("😞 Negative", f"{neg_pct}% ({int(row['negative_count'])})")
            st.bar_chart(pd.DataFrame({
                "Sentiment": ["Positive","Neutral","Negative"],
                "Count": [int(row["positive_count"]), int(row["neutral_count"]), int(row["negative_count"])],
            }).set_index("Sentiment"))
            st.divider()
    else:
        no_data("No sentiment data. Run `make pipeline`.")

# ═════════════════════════════════════════════════════
#  PAGE 4: CUSTOMER 360
# ═════════════════════════════════════════════════════
elif page == "👤 Customer 360":
    st.title("👤 Single Customer View")
    st.caption("Live from MongoDB — updates as stream writes profiles")

    if not mongo_ok:
        st.error("MongoDB offline. Run `make up`.")
        st.stop()

    # recently streamed ticker
    recent = mongo_query(lambda c: list(
        c.find({}, {"customer_id":1,"segment":1,"updated_at":1,"_id":0})
         .sort("updated_at", -1).limit(5)
    )) or []

    if recent:
        st.markdown("**🔴 Recently streamed profiles:**")
        cols = st.columns(len(recent))
        ICONS = {"vip":"🟣","regular":"🔵","at_risk":"🔴","new":"🟢"}
        for i, doc in enumerate(recent):
            label = doc.get("segment",{}).get("label","?")
            cols[i].markdown(
                f"{ICONS.get(label,'⚪')} **{doc['customer_id']}**  \n`{label}`"
            )
        st.divider()
    else:
        no_data("No profiles yet. Run `make scv` or `make scv-stream`.")

    c1,c2 = st.columns([3,1])
    customer_id = c1.text_input("Customer ID", placeholder="e.g. CUST-00001", value="CUST-00008")
    c2.write(""); c2.write("")
    search = c2.button("🔍 Look Up", use_container_width=True)

    if search or customer_id:
        doc = mongo_query(lambda c: c.find_one(
            {"customer_id": customer_id.strip()}, {"_id": 0}
        ))

        if doc:
            seg = doc.get("segment", {})
            txn = doc.get("transactions", {})
            rev = doc.get("reviews", {})
            web = doc.get("web_behaviour", {})
            soc = doc.get("social", {})

            BADGE = {"vip":"🟣 VIP","regular":"🔵 Regular","at_risk":"🔴 At Risk","new":"🟢 New"}
            badge = BADGE.get(seg.get("label",""), "⚪ Unknown")
            churn = seg.get("churn_risk_score", 0)
            churn_icon = "🔴" if churn > 0.65 else ("🟡" if churn > 0.4 else "🟢")
            updated = doc.get("updated_at")
            if updated:
                st.caption(f"Profile last updated: {updated}")

            st.subheader(f"{customer_id}  —  {badge}")
            c1,c2,c3,c4 = st.columns(4)
            c1.metric("Churn Risk",  f"{churn_icon} {churn:.2%}")
            c2.metric("Total Spent", f"₹{txn.get('total_spent',0):,.0f}")
            c3.metric("Orders",      txn.get("total_orders", 0))
            c4.metric("Avg Rating",  f"{rev.get('avg_rating', 0):.1f} ⭐")

            st.divider()
            cl,cr = st.columns(2)
            with cl:
                st.markdown("#### 💳 Transactions")
                st.json({"total_orders": txn.get("total_orders"), "total_spent": f"₹{txn.get('total_spent',0):,.2f}", "total_returns": txn.get("total_returns"), "return_rate": f"{txn.get('return_rate',0):.1%}", "preferred_payment": txn.get("preferred_payment"), "last_order_at": txn.get("last_order_at"), "revenue_bands": txn.get("revenue_band_distribution")})
                st.markdown("#### ⭐ Reviews")
                st.json({"avg_rating": rev.get("avg_rating"), "total_reviews": rev.get("total_reviews"), "avg_sentiment": rev.get("avg_sentiment_score"), "distribution": rev.get("sentiment_distribution")})
            with cr:
                st.markdown("#### 🌐 Web Behaviour")
                st.json({"total_sessions": web.get("total_sessions"), "total_clicks": web.get("total_clicks"), "total_searches": web.get("total_searches"), "total_add_to_cart": web.get("total_add_to_cart"), "top_device": web.get("top_device"), "last_active_at": web.get("last_active_at")})
                st.markdown("#### 📱 Social Media")
                st.json({"post_count": soc.get("post_count"), "avg_sentiment": soc.get("avg_sentiment"), "avg_engagement_score": soc.get("avg_engagement_score"), "high_engagement_posts": soc.get("high_engagement_posts"), "platforms": soc.get("platforms")})
        elif customer_id:
            st.warning(f"No profile for `{customer_id}`. Run `make scv` first.")

# ═════════════════════════════════════════════════════
#  PAGE 5: CHURN RISK
# ═════════════════════════════════════════════════════
elif page == "🚨 Churn Risk":
    st.title("🚨 Churn Risk Dashboard")

    if not pg_ok:
        st.error("PostgreSQL offline. Run `make up`.")
        st.stop()

    c1,c2,c3 = st.columns(3)
    risk_threshold = c1.slider("Min churn score", 0.0, 1.0, 0.5, 0.05)
    segment_filter = c2.multiselect("Segments", ["vip","regular","at_risk","new"], default=["at_risk","regular"])
    top_n          = c3.number_input("Top N", 10, 500, 50)

    seg_clause = ""
    if segment_filter:
        quoted = ",".join(f"'{s}'" for s in segment_filter)
        seg_clause = f"AND segment IN ({quoted})"

    churn_df = qpg(f"""
        SELECT customer_id, segment, churn_risk_score,
               total_spent_30d, order_count_30d,
               last_order_date, avg_review_rating
        FROM gold_customer_segments
        WHERE churn_risk_score >= {risk_threshold} {seg_clause}
        ORDER BY churn_risk_score DESC LIMIT {top_n}
    """)

    if mongo_ok:
        live_risk = mongo_query(lambda c: c.count_documents(
            {"segment.churn_risk_score": {"$gte": risk_threshold}}
        )) or 0
        live_vip = mongo_query(lambda c: c.count_documents(
            {"segment.label": "vip", "segment.churn_risk_score": {"$gte": 0.5}}
        )) or 0
        st.info(
            f"📡 **Live MongoDB** — {live_risk:,} profiles with churn ≥ {risk_threshold:.0%} "
            f"| {live_vip:,} VIP customers at risk"
        )

    if not churn_df.empty:
        c1,c2,c3 = st.columns(3)
        c1.metric("Matching",  len(churn_df))
        c2.metric("Avg Churn", f"{churn_df['churn_risk_score'].mean():.2%}")
        c3.metric("Avg Spend", f"₹{churn_df['total_spent_30d'].mean():,.0f}")

        st.divider()
        st.subheader("Score Distribution")
        st.bar_chart(churn_df["churn_risk_score"].value_counts(bins=10).sort_index())

        st.subheader("At-Risk Customers")
        d = churn_df.copy()
        d["churn_risk_score"] = d["churn_risk_score"].map(
            lambda x: f"{'🔴' if x>0.65 else '🟡'} {x:.2%}"
        )
        d["total_spent_30d"] = d["total_spent_30d"].map(lambda x: f"₹{x:,.0f}")
        d["segment"]         = d["segment"].str.title()
        st.dataframe(d.rename(columns={
            "customer_id":"Customer ID","segment":"Segment",
            "churn_risk_score":"Churn Risk","total_spent_30d":"30d Spend",
            "order_count_30d":"Orders (30d)","last_order_date":"Last Order",
            "avg_review_rating":"Avg Rating",
        }), use_container_width=True, hide_index=True)

        st.subheader("Spend vs Churn Risk")
        st.scatter_chart(
            churn_df[["total_spent_30d","churn_risk_score","segment"]],
            x="total_spent_30d", y="churn_risk_score", color="segment"
        )
    else:
        no_data("No customers match current filters. Run `make pipeline` first.")
