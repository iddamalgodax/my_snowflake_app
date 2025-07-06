import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

session = get_active_session()

def run_query(sql: str) -> pd.DataFrame:
    try:
        return session.sql(sql).to_pandas()
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()

st.title("Keells Supermarket - Interactive Enterprise Dashboard")

tabs = st.tabs([
    "Overview",
    "Store Performance",
    "Product Sales",
    "Customer Insights",
    "Inventory",
    "Promotions",
    "Search"
])

# --- Overview ---
with tabs[0]:
    st.header("Overview Metrics")
    total_sales = run_query("SELECT COALESCE(SUM(total_value),0) AS total_sales FROM FACT_ORDERS")
    total_orders = run_query("SELECT COUNT(order_id) AS total_orders FROM FACT_ORDERS")
    active_customers = run_query("SELECT COUNT(DISTINCT customer_id) AS active_customers FROM FACT_ORDERS")

    ts = total_sales.at[0, "TOTAL_SALES"] if not total_sales.empty else 0
    to = total_orders.at[0, "TOTAL_ORDERS"] if not total_orders.empty else 0
    ac = active_customers.at[0, "ACTIVE_CUSTOMERS"] if not active_customers.empty else 0

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Sales (LKR)", f"{ts:,.2f}")
    c2.metric("Total Orders", f"{to}")
    c3.metric("Active Customers", f"{ac}")

    st.markdown("### Sales Over Time")
    sales_over_time = run_query("""
        SELECT dd.full_date, SUM(fol.quantity * fol.unit_price) AS sales
        FROM FACT_ORDER_LINES fol
        JOIN FACT_ORDERS fo ON fol.order_id = fo.order_id
        JOIN DIM_DATE dd ON fo.order_date_id = dd.date_id
        GROUP BY dd.full_date
        ORDER BY dd.full_date
        LIMIT 100
    """)
    if not sales_over_time.empty:
        sales_over_time['FULL_DATE'] = pd.to_datetime(sales_over_time['FULL_DATE'])
        sales_over_time = sales_over_time.set_index('FULL_DATE')
        st.line_chart(sales_over_time['SALES'])
    else:
        st.info("No sales data available.")

# --- Store Performance ---
with tabs[1]:
    st.header("Store Sales Performance")

    stores_df = run_query("SELECT store_name FROM DIM_STORE ORDER BY store_name")
    stores = stores_df["STORE_NAME"].tolist()
    selected_stores = st.multiselect("Select Stores", options=stores, default=stores)

    if selected_stores:
        store_list_sql = ", ".join([f"'{s}'" for s in selected_stores])
        sales_data = run_query(f"""
            SELECT ds.store_name, dd.full_date, SUM(fol.quantity * fol.unit_price) AS sales
            FROM FACT_ORDER_LINES fol
            JOIN FACT_ORDERS fo ON fol.order_id = fo.order_id
            JOIN DIM_DATE dd ON fo.order_date_id = dd.date_id
            JOIN DIM_STORE ds ON fo.store_id = ds.store_id
            WHERE ds.store_name IN ({store_list_sql})
            GROUP BY ds.store_name, dd.full_date
            ORDER BY dd.full_date
        """)
        if not sales_data.empty:
            pivot_df = sales_data.pivot(index='FULL_DATE', columns='STORE_NAME', values='SALES').fillna(0)
            pivot_df.index = pd.to_datetime(pivot_df.index)
            st.line_chart(pivot_df)
            
            # Pie chart of total sales per store
            total_sales_store = sales_data.groupby("STORE_NAME")["SALES"].sum().reset_index()
            st.markdown("#### Total Sales Distribution by Store")
            st.pyplot(total_sales_store.plot.pie(y='SALES', labels=total_sales_store['STORE_NAME'], autopct='%1.1f%%', legend=False).get_figure())
        else:
            st.info("No sales data found for selected stores.")
    else:
        st.info("Please select at least one store.")

# --- Product Sales ---
with tabs[2]:
    st.header("Product Sales Analysis")

    categories_df = run_query("SELECT DISTINCT category FROM DIM_PRODUCT ORDER BY category")
    categories = categories_df["CATEGORY"].dropna().tolist()
    selected_categories = st.multiselect("Select Categories", options=categories, default=categories)

    if selected_categories:
        categories_sql = ", ".join([f"'{c}'" for c in selected_categories])
        prod_sales = run_query(f"""
            SELECT dp.product_name, dp.category, SUM(fol.quantity) AS quantity_sold, SUM(fol.quantity * fol.unit_price) AS revenue
            FROM FACT_ORDER_LINES fol
            JOIN DIM_PRODUCT dp ON fol.product_id = dp.product_id
            WHERE dp.category IN ({categories_sql})
            GROUP BY dp.product_name, dp.category
            ORDER BY revenue DESC
            LIMIT 50
        """)
        if not prod_sales.empty:
            st.dataframe(prod_sales)

            # Bar chart revenue by product
            st.markdown("### Top Products by Revenue")
            st.bar_chart(prod_sales.set_index("PRODUCT_NAME")["REVENUE"])

            # Pie chart of revenue by category
            category_rev = prod_sales.groupby("CATEGORY")["REVENUE"].sum().reset_index()
            st.markdown("### Revenue Distribution by Category")
            st.pyplot(category_rev.plot.pie(y='REVENUE', labels=category_rev['CATEGORY'], autopct='%1.1f%%', legend=False).get_figure())
        else:
            st.info("No sales data for selected categories.")
    else:
        st.info("Please select at least one category.")

# --- Customer Insights ---
with tabs[3]:
    st.header("Customer Insights")

    tiers_df = run_query("SELECT DISTINCT loyalty_tier FROM FACT_ORDERS WHERE loyalty_tier IS NOT NULL ORDER BY loyalty_tier")
    tiers = tiers_df["LOYALTY_TIER"].tolist()
    selected_tiers = st.multiselect("Select Loyalty Tiers", options=tiers, default=tiers)

    if selected_tiers:
        tiers_sql = ", ".join([f"'{t}'" for t in selected_tiers])
        cust_data = run_query(f"""
            SELECT fc.region, COUNT(DISTINCT fc.customer_id) AS customer_count, AVG(fo.total_value) AS avg_spent
            FROM DIM_CUSTOMER fc
            JOIN FACT_ORDERS fo ON fc.customer_id = fo.customer_id
            WHERE fo.loyalty_tier IN ({tiers_sql})
            GROUP BY fc.region
            ORDER BY customer_count DESC
        """)
        if not cust_data.empty:
            st.dataframe(cust_data)

            # Bar chart customer count by region
            st.markdown("### Customer Count by Region")
            st.bar_chart(cust_data.set_index("REGION")["CUSTOMER_COUNT"])

            # Pie chart loyalty tier distribution
            loyalty_dist = run_query(f"""
                SELECT loyalty_tier, COUNT(DISTINCT customer_id) AS customer_count
                FROM FACT_ORDERS
                WHERE loyalty_tier IN ({tiers_sql})
                GROUP BY loyalty_tier
            """)
            if not loyalty_dist.empty:
                st.markdown("### Loyalty Tier Distribution")
                st.pyplot(loyalty_dist.plot.pie(y='CUSTOMER_COUNT', labels=loyalty_dist['LOYALTY_TIER'], autopct='%1.1f%%', legend=False).get_figure())
        else:
            st.info("No customer data found.")
    else:
        st.info("Please select at least one loyalty tier.")

# --- Inventory ---
with tabs[4]:
    st.header("Inventory Status")

    stores_df = run_query("SELECT store_name FROM DIM_STORE ORDER BY store_name")
    stores = stores_df["STORE_NAME"].tolist()
    selected_store = st.selectbox("Select Store", options=stores)

    if selected_store:
        inv_data = run_query(f"""
            SELECT dp.product_name, fi.stock_level, fi.on_order_qty, fi.safety_stock
            FROM FACT_INVENTORY fi
            JOIN DIM_SKU sku ON fi.sku_id = sku.sku_id
            JOIN DIM_PRODUCT dp ON sku.product_id = dp.product_id
            JOIN DIM_STORE ds ON fi.store_id = ds.store_id
            WHERE ds.store_name = '{selected_store}'
            ORDER BY fi.stock_level ASC
            LIMIT 100
        """)
        if not inv_data.empty:
            st.dataframe(inv_data)

            # Bar chart of stock levels for top 20 products
            top_stock = inv_data.head(20)
            st.markdown("### Stock Levels for Top 20 Products")
            st.bar_chart(top_stock.set_index("PRODUCT_NAME")["STOCK_LEVEL"])
        else:
            st.info(f"No inventory data for {selected_store}")

# --- Promotions ---
with tabs[5]:
    st.header("Promotions Overview")

    promo_data = run_query("""
        SELECT fp.promo_id, dp.product_name, fp.promo_type, fp.discount_rate, fp.expected_uplift, fp.start_date_id, fp.end_date_id
        FROM FACT_PROMOTION fp
        JOIN DIM_SKU sku ON fp.sku_id = sku.sku_id
        JOIN DIM_PRODUCT dp ON sku.product_id = dp.product_id
        ORDER BY fp.start_date_id DESC
        LIMIT 50
    """)
    if not promo_data.empty:
        st.dataframe(promo_data)

        # Bar chart discount rates per promo type
        promo_type_df = promo_data.groupby("PROMO_TYPE")["DISCOUNT_RATE"].mean().reset_index()
        st.markdown("### Average Discount Rate by Promotion Type")
        st.bar_chart(promo_type_df.set_index("PROMO_TYPE")["DISCOUNT_RATE"])
    else:
        st.info("No promotion data available.")

# --- Search ---
with tabs[6]:
    st.header("Search Products & Orders")

    search_term = st.text_input("Enter Product Name or Order ID")

    if search_term:
        product_results = run_query(f"""
            SELECT dp.product_id, dp.product_name, dp.category
            FROM DIM_PRODUCT dp
            WHERE LOWER(dp.product_name) LIKE '%{search_term.lower()}%'
            LIMIT 20
        """)
        st.subheader("Matching Products")
        if not product_results.empty:
            st.dataframe(product_results)
        else:
            st.info("No matching products found.")

        if search_term.isdigit():
            order_results = run_query(f"""
                SELECT order_id, order_date_id, total_value
                FROM FACT_ORDERS
                WHERE order_id = {int(search_term)}
            """)
            st.subheader("Order Details")
            if not order_results.empty:
                st.dataframe(order_results)
            else:
                st.info("No order found with that ID.")
