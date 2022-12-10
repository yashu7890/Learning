import streamlit as st
import pandas as pd
import numpy as np
import openpyxl
import os
from pathlib import Path

st.write("Here's our first attempt at using data to create a table:")
st.write(pd.DataFrame({
    'first column': [1, 2, 3, 4],
    'second column': [10, 20, 30, 40]
}))

dataframe = pd.DataFrame(
    np.random.randn(10, 20),
    columns=('col %d' % i for i in range(20)))

my_file = Path('pandas_to_excel.xlsx')
if my_file.is_file():
    df= pd.read_excel('pandas_to_excel.xlsx')
    st.dataframe(df)
# dataframe1 =pd.concat(df,dataframe)

dataframe.to_excel('pandas_to_excel.xlsx', sheet_name='Sales_data',index=False,header=True)
st.dataframe(dataframe)

st.dataframe(dataframe.style.highlight_max(axis=0))

chart_data = pd.DataFrame(
     np.random.randn(20, 3),
     columns=['a', 'b', 'c'])

st.line_chart(chart_data)

map_data = pd.DataFrame(
    np.random.randn(1000, 2) / [50, 50] + [37.76, -122.4],
    columns=['lat', 'lon'])

st.map(map_data)
