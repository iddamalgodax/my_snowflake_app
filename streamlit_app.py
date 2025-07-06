import streamlit as st
import pandas as pd
import snowflake.snowpark as snowpark

def main():
    session = snowpark.Session.builder.configs(st.secrets["snowflake"]).create()
    df = session.sql("SELECT CURRENT_DATE as today").to_pandas()
    
    st.title("🚀 My Streamlit App in Snowflake")
    st.write("Today's date from Snowflake:")
    st.dataframe(df)

if __name__ == "__main__":
    main()
