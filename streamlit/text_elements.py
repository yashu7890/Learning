import streamlit as st
import pandas as pd
import numpy as np
import time


st.title('title: This is a string that explains something above.',"https://zoro.to/")

st.header('header: This is a string that explains something above.')

st.subheader('subheader: This is a string that explains something above.')

st.caption('caption: This is a string that explains something above.')

def add(a,b):
    return a+b

st.code(str(add),language="python")

code = '''def hello():
    print("Hello, Streamlit!")'''
st.code(code, language='python')



st.latex(r'''
    a + ar + a r^2 + a r^3 + \cdots + a r^{n-1} =
    \sum_{k=0}^{n-1} ar^k =
    a \left(\frac{1-r^{n}}{1-r}\right)
    ''')