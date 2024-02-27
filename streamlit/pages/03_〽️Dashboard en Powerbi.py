import streamlit as st

# Título e introducción
st.title("Dashboard de Power BI")
st.markdown("***")


# Insertar el dashboard de Power BI utilizando un iframe
st.components.v1.iframe("https://app.powerbi.com/view?r=eyJrIjoiNDIwZWM4ZmItZWVkOC00MzNjLWE0ZDQtODgwMzVjZjAxMDdkIiwidCI6IjRhMmE4MWRjLTE0MWQtNDM3My05MDgzLWQxNDY4YmRjYjE3NSIsImMiOjR9",
                        width=800,
                        height=600)
