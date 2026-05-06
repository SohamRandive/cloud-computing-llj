# dashboard/app.py — ShopStream Real-Time Analytics Dashboard
import sys
import time
from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

sys.path.insert(0, str(Path(__file__).parent.parent))

st.set_page_config(
    page_title="ShopStream Analytics",
    page_icon="🛍️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    [data-testid="metric-container"] {
        background: #0f172a;
        border-radius: 8px;
        padding: 12px 16px;
        border: 1px solid #1e293b;
        border-left: 4px solid #0ea5e9;
    }
    div[data-testid="stSidebarNav"] { display: none; }
    .live-badge {
        display: inline-block; background: #16a34a; color: white;
        font-size: 0.75rem; font-weight: 700; padding: 3px 10px;
        border-radius: 999px; margin-right: 8px;
        animation: pulse 1.5s infinite;
    }
    .dead-badge {
        display: inline-block; background: #dc2626; color: white;
        font-size: 0.75rem; font-weight: 700; padding: 3px 10px;
        border-radius: 999px; margin-right: 8px;
    }
    @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.3} }
</style>
""", unsafe_allow_html=True)


def get_pg():
    try:
        import psycopg2
        return psycopg2.connect(
            host="localhost", port=5432,
            dbname="shopstream_gold",
            user="shopstream_user",
            password="shopstream_pg_pass",
            connect_timeout=3,
        )
    except Exception:
        return None


def get_mongo():
    try:
        from pymongo import MongoClient
        c = MongoClient("mongodb://admin:shopstream_mongo_pass@localhost:27017/",
                        serverSelectionTimeoutMS=2000, connectTimeoutMS=2000)
        c.admin.command("ping")
        return c["shopstream_profiles"]["user_profiles"]
    except Exception:
        return None


def qpg(sql):
    conn = get_pg()
    if conn is None:
        return pd.DataFrame()
    try:
        df = pd.read_sql(sql, conn); conn.close(); return df
    except Exception:
        try: conn.close()
        except Exception: pass
        return pd.DataFrame()


def qmongo(fn):
    col = get_mongo()
    if col is None: return None
    try: return fn(col)
    except Exception: return None


pg_ok    = get_pg()    is not None
mongo_ok = get_mongo() is not None

# ── Sidebar ───────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🛍️ ShopStream")
    st.caption("Real-Time E-Commerce Analytics")
    st.divider()

    page = st.radio("Navigate", [
        "📊 Overview", "🔍 Keyword Trends",
        "😊 Sentiment", "👤 User Profile", "⚠️ Churn Risk"
    ], label_visibility="collapsed")

    st.divider()
    st.markdown("### ⚡ Live Refresh")
    auto_ref = st.toggle("Auto-refresh", value=True)
    interval = st.select_slider("Interval (s)", [3, 5, 10, 15, 30], value=5)
    if st.button("🔄 Refresh Now", use_container_width=True):
        st.rerun()
    st.divider()

    st.markdown("### 🔌 Services")
    st.markdown(f"{'✅' if pg_ok else '❌'} PostgreSQL")
    st.markdown(f"{'✅' if mongo_ok else '❌'} MongoDB")
    if not pg_ok:    st.warning("PostgreSQL offline — run `make up`")
    if not mongo_ok: st.warning("MongoDB offline — run `make up`")
    st.divider()

    if mongo_ok:
        cnt  = qmongo(lambda c: c.count_documents({})) or 0
        segs = qmongo(lambda c: {
            s["_id"]: s["n"] for s in
            c.aggregate([{"$group": {"_id": "$segment.label", "n": {"$sum": 1}}},
                         {"$sort": {"n": -1}}])
        }) or {}
        st.markdown("### 📋 Live Profiles")
        st.metric("Total Users", f"{cnt:,}")
        ICONS = {"platinum":"🟡","gold":"🟠","silver":"⚪","basic":"🔵","at_risk":"🔴","new":"🟢","regular":"🔵"}
        for label, n in segs.items():
            st.markdown(f"{ICONS.get(label,'⚫')} **{label.title()}**: {n:,}")

# ── Auto-refresh ──────────────────────────────────────
rc = 0
if auto_ref:
    rc = st_autorefresh(interval=interval * 1000, limit=None, key="ar")

if pg_ok and mongo_ok:
    st.markdown(f'<span class="live-badge">● LIVE</span>'
                f'<span style="font-size:0.8rem;color:#888">Refresh {"ON" if auto_ref else "OFF"} — {interval}s — #{rc}</span>',
                unsafe_allow_html=True)
else:
    down = [s for s, ok in [("PostgreSQL", pg_ok), ("MongoDB", mongo_ok)] if not ok]
    st.markdown(f'<span class="dead-badge">● OFFLINE</span>'
                f'<span style="font-size:0.8rem;color:#ef4444">{", ".join(down)} unreachable</span>',
                unsafe_allow_html=True)


def no_data(msg="No data yet."):
    st.info(f"ℹ️ {msg}")


# ═══════════════════════════════════════════════
#  PAGE 1 — OVERVIEW
# ═══════════════════════════════════════════════
if page == "📊 Overview":
    st.title("📊 ShopStream Overview")

    rev_df  = qpg("SELECT SUM(gross_revenue) rev, SUM(num_orders) orders, SUM(num_returns) returns FROM hourly_sales")
    traf_df = qpg("SELECT SUM(num_sessions) sessions, SUM(unique_visitors) visitors, AVG(avg_session_ms) avg_ms FROM hourly_site_traffic")
    seg_df  = qpg("SELECT segment, COUNT(*) cnt FROM user_segments GROUP BY segment ORDER BY cnt DESC")

    def _v(df, col, default=0, t=float):
        try:
            v = df[col].iloc[0]; return t(v) if v is not None else default
        except Exception: return default

    rev      = _v(rev_df,  "rev",      0, float)
    orders   = _v(rev_df,  "orders",   0, int)
    returns  = _v(rev_df,  "returns",  0, int)
    ret_rate = round((returns / orders * 100) if orders else 0, 1)
    sessions = _v(traf_df, "sessions", 0, int)
    avg_s    = round(_v(traf_df, "avg_ms", 0, float) / 1000, 1)
    profiles = qmongo(lambda c: c.count_documents({})) or 0

    c1,c2,c3,c4,c5,c6 = st.columns(6)
    c1.metric("💰 Gross Revenue",  f"₹{rev:,.0f}"   if pg_ok    else "—")
    c2.metric("🛒 Total Orders",   f"{orders:,}"    if pg_ok    else "—")
    c3.metric("↩️ Return Rate",    f"{ret_rate}%"   if pg_ok    else "—")
    c4.metric("👥 Sessions",       f"{sessions:,}"  if pg_ok    else "—")
    c5.metric("⏱️ Avg Session",    f"{avg_s}s"      if pg_ok    else "—")
    c6.metric("🗃️ User Profiles",  f"{profiles:,}"  if mongo_ok else "—")

    st.divider()
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Revenue by Category")
        if not pg_ok:
            no_data("PostgreSQL offline.")
        else:
            df = qpg("SELECT category, SUM(gross_revenue) rev, SUM(num_orders) orders FROM hourly_sales GROUP BY category ORDER BY rev DESC")
            if not df.empty:
                st.bar_chart(df.set_index("category")["rev"])
                st.dataframe(df.style.format({"rev": "₹{:,.2f}", "orders": "{:,}"}),
                             use_container_width=True, hide_index=True)
            else:
                no_data("Run `make pipeline`.")

    with col2:
        st.subheader("User Segments")
        if not pg_ok:
            no_data("PostgreSQL offline.")
        elif not seg_df.empty:
            ICONS = {"platinum":"🟡","gold":"🟠","silver":"⚪","regular":"🔵","at_risk":"🔴","new":"🟢"}
            seg_df["Label"]   = seg_df["segment"].map(lambda s: f"{ICONS.get(s,'⚫')} {s.title()}")
            seg_df["Share %"] = (seg_df["cnt"] / seg_df["cnt"].sum() * 100).round(1)
            st.bar_chart(seg_df.set_index("Label")["cnt"])
            st.dataframe(seg_df[["Label","cnt","Share %"]].rename(columns={"cnt":"Count"}),
                         use_container_width=True, hide_index=True)
        else:
            no_data("Run `make pipeline`.")

    st.subheader("Site Traffic (Hourly)")
    if not pg_ok:
        no_data("PostgreSQL offline.")
    else:
        tr = qpg("SELECT window_start, num_sessions, num_clicks, num_pageviews, unique_visitors FROM hourly_site_traffic ORDER BY window_start")
        if tr.empty:
            no_data("Run `make pipeline`.")
        elif len(tr) == 1:
            r = tr.iloc[0]
            c1,c2,c3,c4 = st.columns(4)
            c1.metric("Sessions",  int(r["num_sessions"]))
            c2.metric("Clicks",    int(r["num_clicks"]))
            c3.metric("Pageviews", int(r["num_pageviews"]))
            c4.metric("Visitors",  int(r["unique_visitors"]))
        else:
            tr["window_start"] = pd.to_datetime(tr["window_start"])
            st.line_chart(tr.set_index("window_start")[["num_sessions","num_clicks","num_pageviews","unique_visitors"]])


# ═══════════════════════════════════════════════
#  PAGE 2 — KEYWORD TRENDS
# ═══════════════════════════════════════════════
elif page == "🔍 Keyword Trends":
    st.title("🔍 Search Keyword Trends")
    if not pg_ok:
        st.error("PostgreSQL offline."); st.stop()

    c1,c2 = st.columns(2)
    top_n  = c1.slider("Top N keywords", 5, 50, 20)
    min_c  = c2.number_input("Min count", 1, 50, 2)

    df = qpg(f"""
        SELECT keyword, SUM(search_count) total_searches, SUM(unique_visitors) total_users
        FROM hourly_search_trends WHERE search_count >= {min_c}
        GROUP BY keyword ORDER BY total_searches DESC LIMIT {top_n}
    """)
    if not df.empty:
        st.bar_chart(df.set_index("keyword")["total_searches"])
        df["searches/user"] = (df["total_searches"] / df["total_users"]).round(2)
        st.dataframe(df.style.format({"total_searches": "{:,}", "total_users": "{:,}"}),
                     use_container_width=True, hide_index=True)
    else:
        no_data("Run `make pipeline`.")


# ═══════════════════════════════════════════════
#  PAGE 3 — SENTIMENT
# ═══════════════════════════════════════════════
elif page == "😊 Sentiment":
    st.title("😊 Sentiment Analysis")
    st.caption("VADER compound scores — product reviews vs social buzz")

    if mongo_ok:
        live = qmongo(lambda c: list(c.aggregate([
            {"$match": {"reviews.avg_sentiment": {"$exists": True}}},
            {"$group": {"_id": None,
                        "avg_rev":  {"$avg": "$reviews.avg_sentiment"},
                        "avg_soc":  {"$avg": "$social.avg_sentiment"},
                        "total":    {"$sum": 1}}}
        ])))
        if live:
            l = live[0]
            st.info(f"📡 **Live MongoDB** — {l['total']:,} profiles | "
                    f"Avg review sentiment: **{l.get('avg_rev',0):.3f}** | "
                    f"Avg social sentiment: **{l.get('avg_soc',0):.3f}**")
    else:
        st.warning("MongoDB offline.")

    if not pg_ok:
        st.error("PostgreSQL offline."); st.stop()

    df = qpg("SELECT data_source, mean_sentiment, positive_cnt, neutral_cnt, negative_cnt, total_cnt FROM hourly_sentiment ORDER BY data_source")
    if not df.empty:
        for _, row in df.iterrows():
            src   = row["data_source"].replace("_", " ").title()
            total = int(row["total_cnt"]) or 1
            score = round(float(row["mean_sentiment"]), 3)
            emoji = "😊" if score > 0.05 else ("😐" if score > -0.05 else "😞")
            pos_p = round(row["positive_cnt"] / total * 100, 1)
            neu_p = round(row["neutral_cnt"]  / total * 100, 1)
            neg_p = round(row["negative_cnt"] / total * 100, 1)
            st.subheader(f"{emoji} {src}")
            c1,c2,c3,c4 = st.columns(4)
            c1.metric("VADER Score", f"{score:+.3f}")
            c2.metric("😊 Positive", f"{pos_p}%")
            c3.metric("😐 Neutral",  f"{neu_p}%")
            c4.metric("😞 Negative", f"{neg_p}%")
            st.bar_chart(pd.DataFrame({
                "Sentiment": ["Positive","Neutral","Negative"],
                "Count": [int(row["positive_cnt"]), int(row["neutral_cnt"]), int(row["negative_cnt"])],
            }).set_index("Sentiment"))
            st.divider()
    else:
        no_data("Run `make pipeline`.")


# ═══════════════════════════════════════════════
#  PAGE 4 — USER PROFILE
# ═══════════════════════════════════════════════
elif page == "👤 User Profile":
    st.title("👤 Unified User Profile")
    st.caption("Live from MongoDB — ShopStream Single User View")

    if not mongo_ok:
        st.error("MongoDB offline."); st.stop()

    recent = qmongo(lambda c: list(
        c.find({}, {"user_id":1,"segment":1,"updated_at":1,"_id":0})
         .sort("updated_at",-1).limit(5)
    )) or []

    if recent:
        st.markdown("**🔴 Recently updated profiles:**")
        cols = st.columns(len(recent))
        ICONS = {"platinum":"🟡","gold":"🟠","silver":"⚪","regular":"🔵","at_risk":"🔴","new":"🟢"}
        for i, doc in enumerate(recent):
            label = doc.get("segment",{}).get("label","?")
            cols[i].markdown(f"{ICONS.get(label,'⚫')} **{doc['user_id']}**  \n`{label}`")
        st.divider()

    c1,c2 = st.columns([3,1])
    uid    = c1.text_input("User ID", placeholder="e.g. USR-000001", value="USR-000001")
    c2.write(""); c2.write("")
    lookup = c2.button("🔍 Look Up", use_container_width=True)

    if lookup or uid:
        doc = qmongo(lambda c: c.find_one({"user_id": uid.strip()}, {"_id":0}))
        if doc:
            seg = doc.get("segment",  {})
            pur = doc.get("purchases", {})
            rev = doc.get("reviews",   {})
            brw = doc.get("browsing",  {})
            soc = doc.get("social",    {})

            BADGE = {"platinum":"🟡 Platinum","gold":"🟠 Gold","silver":"⚪ Silver",
                     "regular":"🔵 Regular","at_risk":"🔴 At Risk","new":"🟢 New"}
            badge  = BADGE.get(seg.get("label",""), "⚫ Unknown")
            churn  = seg.get("churn_score", 0)
            c_icon = "🔴" if churn > 0.65 else ("🟡" if churn > 0.4 else "🟢")

            if doc.get("updated_at"):
                st.caption(f"Last updated: {doc['updated_at']}")
            st.subheader(f"{uid}  —  {badge}")
            c1,c2,c3,c4 = st.columns(4)
            c1.metric("Churn Risk",   f"{c_icon} {churn:.2%}")
            c2.metric("Total Spent",  f"₹{pur.get('total_spent',0):,.0f}")
            c3.metric("Orders",       pur.get("total_orders", 0))
            c4.metric("Avg Rating",   f"{rev.get('avg_rating',0):.1f} ⭐")

            st.divider()
            cl, cr = st.columns(2)
            with cl:
                st.markdown("#### 🛒 Purchases")
                st.json({"total_orders": pur.get("total_orders"),
                         "total_spent": f"₹{pur.get('total_spent',0):,.2f}",
                         "return_rate": f"{pur.get('return_rate',0):.1%}",
                         "preferred_payment": pur.get("preferred_payment"),
                         "spend_distribution": pur.get("spend_distribution")})
                st.markdown("#### ⭐ Reviews")
                st.json({"avg_rating": rev.get("avg_rating"),
                         "total_reviews": rev.get("total_reviews"),
                         "avg_sentiment": rev.get("avg_sentiment"),
                         "breakdown": rev.get("breakdown")})
            with cr:
                st.markdown("#### 🌐 Browsing")
                st.json({"total_sessions": brw.get("total_sessions"),
                         "total_views": brw.get("total_views"),
                         "total_searches": brw.get("total_searches"),
                         "total_cart_adds": brw.get("total_cart_adds"),
                         "top_platform": brw.get("top_platform"),
                         "last_active_at": brw.get("last_active_at")})
                st.markdown("#### 📱 Social Buzz")
                st.json({"post_count": soc.get("post_count"),
                         "avg_sentiment": soc.get("avg_sentiment"),
                         "avg_engagement": soc.get("avg_engagement"),
                         "viral_posts": soc.get("viral_posts"),
                         "platforms": soc.get("platforms")})
        elif uid:
            st.warning(f"No profile for `{uid}`. Run `make profile` first.")


# ═══════════════════════════════════════════════
#  PAGE 5 — CHURN RISK
# ═══════════════════════════════════════════════
elif page == "⚠️ Churn Risk":
    st.title("⚠️ Churn Risk Dashboard")
    if not pg_ok:
        st.error("PostgreSQL offline."); st.stop()

    c1,c2,c3 = st.columns(3)
    threshold = c1.slider("Min churn score", 0.0, 1.0, 0.5, 0.05)
    seg_filter= c2.multiselect("Segments", ["platinum","gold","silver","regular","at_risk","new"], default=["at_risk","regular"])
    top_n     = c3.number_input("Top N", 10, 500, 50)

    seg_clause = ""
    if seg_filter:
        quoted    = ",".join(f"'{s}'" for s in seg_filter)
        seg_clause= f"AND segment IN ({quoted})"

    df = qpg(f"""
        SELECT user_id, segment, churn_score, revenue_30d,
               order_count_30d, last_purchase_date, avg_review_score
        FROM user_segments
        WHERE churn_score >= {threshold} {seg_clause}
        ORDER BY churn_score DESC LIMIT {top_n}
    """)

    if mongo_ok:
        live_risk = qmongo(lambda c: c.count_documents({"segment.churn_score": {"$gte": threshold}})) or 0
        st.info(f"📡 **Live MongoDB** — {live_risk:,} users with churn score ≥ {threshold:.0%}")

    if not df.empty:
        c1,c2,c3 = st.columns(3)
        c1.metric("Matching",   len(df))
        c2.metric("Avg Churn",  f"{df['churn_score'].mean():.2%}")
        c3.metric("Avg Revenue",f"₹{df['revenue_30d'].mean():,.0f}")
        st.divider()

        st.subheader("Score Distribution")
        st.bar_chart(df["churn_score"].value_counts(bins=10).sort_index())

        st.subheader("At-Risk Users")
        d = df.copy()
        d["churn_score"]  = d["churn_score"].map(lambda x: f"{'🔴' if x>0.65 else '🟡'} {x:.2%}")
        d["revenue_30d"]  = d["revenue_30d"].map(lambda x: f"₹{x:,.0f}")
        d["segment"]      = d["segment"].str.title()
        st.dataframe(d.rename(columns={
            "user_id":"User ID","segment":"Segment","churn_score":"Churn Risk",
            "revenue_30d":"30d Revenue","order_count_30d":"Orders (30d)",
            "last_purchase_date":"Last Purchase","avg_review_score":"Avg Rating",
        }), use_container_width=True, hide_index=True)

        st.subheader("Revenue vs Churn Risk")
        st.scatter_chart(df[["revenue_30d","churn_score","segment"]],
                         x="revenue_30d", y="churn_score", color="segment")
    else:
        no_data("No users match filters. Run `make pipeline`.")
