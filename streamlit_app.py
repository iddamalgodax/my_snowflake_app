import streamlit as st
import snowflake.snowpark as snowpark

def main():
    session = snowpark.Session.builder.configs(st.secrets["snowflake"]).create()
    df = session.sql("SELECT * FROM MY_DB.MY_SCHEMA.MY_TABLE").to_pandas()

    st.title("🚀 External Streamlit App")
    st.write(df)

if __name__ == "__main__":
    main()
