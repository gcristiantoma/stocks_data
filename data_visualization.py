# data_visualization.py

import streamlit as st
from pygwalker.api.streamlit import StreamlitRenderer

@st.cache_resource
def get_pyg_renderer(data) -> "StreamlitRenderer":
    renderer = StreamlitRenderer(
        data,
        spec="./gw_config.json",
        spec_io_mode="manual"
    )
    return renderer

def visualize_data_with_pygwalker(data):
    """
    Visualizes the given pandas DataFrame using PyGWalker in Streamlit.
    """
    renderer = get_pyg_renderer(data)
    if not data.empty:
        with st.container():
            renderer.explorer()