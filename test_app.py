import streamlit as st

st.title("테스트 앱")
st.write("Streamlit이 정상적으로 작동합니다!")

if st.button("테스트 버튼"):
    st.success("버튼이 정상적으로 작동합니다!")
