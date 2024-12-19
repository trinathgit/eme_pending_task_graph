import streamlit as st
import pending_task_graph
import site_metrics_graph
# import file3
from flask import Flask, redirect, url_for
import subprocess
import threading

app = Flask(__name__)

@app.route('/')
def home():
    return redirect(url_for('pending_task_graph'))

@app.route('/pending_task_graph')
def pending_task_graph():
    return redirect("0.0.0.0:8501/pending_task_graph")

@app.route('/site_metrics_graph')
def site_graph():
    return redirect("0.0.0.0:8501/site_metrics_graph")

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0")
