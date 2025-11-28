"""
Kafka consumer that visualizes streaming K-Means clustering data in real-time
"""

import json
import threading
import time
from collections import deque
from datetime import datetime
import pandas as pd
import numpy as np
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash
from dash import dcc, html, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
from sklearn.decomposition import PCA
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
CLUSTER_COLORS = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', 
                  '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
MAX_BUFFER_SIZE = 10000
UPDATE_INTERVAL = 1000  # ms

class StreamingDataBuffer:
    """Thread-safe buffer for streaming data"""
    
    def __init__(self, maxlen=10000):
        self.data = deque(maxlen=maxlen)
        self.lock = threading.Lock()
        self.metrics = {
            'total_received': 0,
            'last_update': None,
            'throughput': 0,
            'cluster_counts': {}
        }
    
    def add_record(self, record):
        """Add a record to the buffer"""
        with self.lock:
            self.data.append(record)
            self.metrics['total_received'] += 1
            self.metrics['last_update'] = datetime.now()
            
            # Update cluster counts
            cluster = record.get('cluster', -1)
            self.metrics['cluster_counts'][cluster] = \
                self.metrics['cluster_counts'].get(cluster, 0) + 1
    
    def get_data(self):
        """Get current data as DataFrame"""
        with self.lock:
            if not self.data:
                return pd.DataFrame()
            return pd.DataFrame(list(self.data))
    
    def get_metrics(self):
        """Get current metrics"""
        with self.lock:
            return self.metrics.copy()
    
    def clear(self):
        """Clear the buffer"""
        with self.lock:
            self.data.clear()
            self.metrics = {
                'total_received': 0,
                'last_update': None,
                'throughput': 0,
                'cluster_counts': {}
            }

class KafkaStreamConsumer:
    """Kafka consumer for streaming review data"""
    
    def __init__(self, 
                 bootstrap_servers='localhost:9092',
                 topic='review-clusters',
                 buffer_size=10000):
        """Initialize Kafka consumer"""
        self.topic = topic
        self.buffer = StreamingDataBuffer(maxlen=buffer_size)
        self.is_consuming = False
        self.consumer_thread = None
        
        # Create Kafka consumer WITHOUT consumer group
        try:
            self.consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='earliest',  # Read all messages from beginning
                enable_auto_commit=False,  # No group management needed
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=1000
            )
            logger.info(f"✅ Kafka consumer initialized for topic '{topic}' (no group)")
            logger.info(f"📍 Bootstrap servers: {bootstrap_servers}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kafka consumer: {e}")
            self.consumer = None
    
    def _consume_loop(self):
        """Main consumption loop (runs in separate thread)"""
        if not self.consumer:
            logger.error("❌ Consumer not initialized")
            return
            
        logger.info(f"🚀 Starting consumption loop for topic: {self.topic}")
        
        batch_start = time.time()
        batch_count = 0
        
        try:
            for message in self.consumer:
                if not self.is_consuming:
                    break
                
                # ADDED: Debug logging for first few messages
                if batch_count < 3:
                    logger.info(f"📨 Sample message received: {str(message.value)[:100]}...")
                
                record = message.value
                self.buffer.add_record(record)
                batch_count += 1
                
                # Calculate throughput every 100 messages
                if batch_count % 100 == 0:
                    elapsed = time.time() - batch_start
                    throughput = batch_count / elapsed if elapsed > 0 else 0
                    self.buffer.metrics['throughput'] = throughput
                    
                    logger.info(
                        f"📊 Consumed {batch_count} messages, "
                        f"throughput: {throughput:.1f} msg/s, "
                        f"total: {self.buffer.metrics['total_received']}"
                    )
                    
                    batch_start = time.time()
                    batch_count = 0
                    
        except Exception as e:
            logger.error(f"❌ Consumption error: {e}")
        finally:
            logger.info("⏹️  Consumption loop ended")
    
    def start(self):
        """Start consuming in background thread"""
        if not self.consumer:
            logger.error("❌ Cannot start - consumer not initialized")
            return
            
        if not self.is_consuming:
            self.is_consuming = True
            self.consumer_thread = threading.Thread(
                target=self._consume_loop,
                daemon=True
            )
            self.consumer_thread.start()
            logger.info("✅ Consumer started and actively listening...")
    
    def stop(self):
        """Stop consuming"""
        self.is_consuming = False
        if self.consumer_thread:
            self.consumer_thread.join(timeout=5)
        logger.info("⏸️  Consumer stopped")
    
    def close(self):
        """Close consumer"""
        self.stop()
        if self.consumer:
            self.consumer.close()
        logger.info("🔒 Consumer closed")

# Initialize global consumer
consumer = KafkaStreamConsumer(
    bootstrap_servers='localhost:9092',
    topic='review-clusters',
    buffer_size=MAX_BUFFER_SIZE
)

# Initialize Dash app
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H1("Real-time K-Means Clustering Dashboard", 
                   className="text-center mb-4 mt-3"),
            html.H5("Powered by Apache Kafka", className="text-center text-muted mb-4")
        ])
    ]),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H5("🎛️ Consumer Controls", className="card-title"),
                    dbc.ButtonGroup([
                        dbc.Button("▶️ Start", id="btn-start", color="success", n_clicks=0),
                        dbc.Button("⏸️ Stop", id="btn-stop", color="danger", n_clicks=0),
                        dbc.Button("🗑️ Clear", id="btn-clear", color="warning", n_clicks=0),
                    ]),
                ])
            ])
        ], width=4),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H5("📊 Stream Metrics", className="card-title"),
                    html.Div(id="stream-metrics")
                ])
            ])
        ], width=8)
    ], className="mb-3"),
    
    dbc.Row([
        dbc.Col([
            dcc.Graph(id="live-scatter", style={'height': '500px'})
        ], width=8),
        dbc.Col([
            dcc.Graph(id="cluster-bars", style={'height': '500px'})
        ], width=4)
    ], className="mb-3"),
    
    dbc.Row([
        dbc.Col([
            dcc.Graph(id="rating-heatmap", style={'height': '400px'})
        ], width=6),
        dbc.Col([
            dcc.Graph(id="throughput-chart", style={'height': '400px'})
        ], width=6)
    ]),
    
    # Store for throughput history
    dcc.Store(id='throughput-history', data={'time': [], 'throughput': []}),
    dcc.Interval(id='update-interval', interval=UPDATE_INTERVAL, n_intervals=0),
    
], fluid=True, style={'backgroundColor': '#f8f9fa'})

@app.callback(
    Output('stream-metrics', 'children'),
    [Input('btn-start', 'n_clicks'),
     Input('btn-stop', 'n_clicks'),
     Input('btn-clear', 'n_clicks'),
     Input('update-interval', 'n_intervals')]
)
def handle_controls(start_clicks, stop_clicks, clear_clicks, n_intervals):
    ctx = dash.callback_context
    
    if ctx.triggered:
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        
        if button_id == 'btn-start':
            consumer.start()
        elif button_id == 'btn-stop':
            consumer.stop()
        elif button_id == 'btn-clear':
            consumer.buffer.clear()
    
    # Get current metrics
    metrics = consumer.buffer.get_metrics()
    
    status_color = "success" if consumer.is_consuming else "secondary"
    status_text = "🟢 CONSUMING" if consumer.is_consuming else "⚪ STOPPED"
    
    return html.Div([
        dbc.Badge(status_text, color=status_color, className="mb-2 me-2"),
        html.P(f"📦 Total Records: {metrics['total_received']:,}", className="mb-1"),
        html.P(f"⚡ Throughput: {metrics['throughput']:.1f} msg/s", className="mb-1"),
        html.P(f"🕐 Last Update: {metrics['last_update'].strftime('%H:%M:%S') if metrics['last_update'] else 'N/A'}", 
               className="mb-1"),
    ])

@app.callback(
    Output('live-scatter', 'figure'),
    Input('update-interval', 'n_intervals')
)
def update_scatter(n):
    df = consumer.buffer.get_data()
    
    if df.empty:
        fig = go.Figure()
        fig.update_layout(
            title="Cluster Scatter Plot (Waiting for data...)",
            template="plotly_white"
        )
        return fig
    
    # Check if required columns exist
    if 'text' not in df.columns or 'rating' not in df.columns:
        fig = go.Figure()
        fig.update_layout(
            title="Cluster Scatter Plot (Invalid data format)",
            template="plotly_white"
        )
        return fig
    
    # FIXED: Handle NaN values in cluster column
    if 'cluster' not in df.columns:
        df['cluster'] = 0
    
    # Convert cluster to numeric and fill NaN with -1
    df['cluster'] = pd.to_numeric(df['cluster'], errors='coerce').fillna(-1).astype(int)
    
    # Simple 2D projection using rating and text length
    df['text_len'] = df['text'].astype(str).str.len()
    
    # If we have enough data, use PCA
    if len(df) > 10:
        features = df[['rating', 'text_len']].fillna(0).values
        
        if len(df) >= 2:
            try:
                pca = PCA(n_components=2)
                coords = pca.fit_transform(features)
                df['x'] = coords[:, 0]
                df['y'] = coords[:, 1]
            except:
                df['x'] = df['rating'].fillna(0)
                df['y'] = df['text_len']
        else:
            df['x'] = df['rating'].fillna(0)
            df['y'] = df['text_len']
    else:
        df['x'] = df['rating'].fillna(0)
        df['y'] = df['text_len']
    
    fig = go.Figure()
    
    for cluster_id in sorted(df['cluster'].unique()):
        cluster_df = df[df['cluster'] == cluster_id]
        
        # Safely convert cluster_id to int
        try:
            cluster_idx = int(cluster_id) if cluster_id >= 0 else 0
        except:
            cluster_idx = 0
        
        fig.add_trace(go.Scatter(
            x=cluster_df['x'],
            y=cluster_df['y'],
            mode='markers',
            name=f'Cluster {cluster_id}' if cluster_id >= 0 else 'Unknown',
            marker=dict(
                size=8,
                color=CLUSTER_COLORS[cluster_idx % len(CLUSTER_COLORS)],
                opacity=0.6,
                line=dict(width=0.5, color='white')
            ),
            text=[f"Rating: {r}<br>Cluster: {c}" 
                  for r, c in zip(cluster_df['rating'], cluster_df['cluster'])],
            hovertemplate='%{text}<extra></extra>'
        ))
    
    fig.update_layout(
        title=f"Live Cluster Visualization ({len(df)} records)",
        xaxis_title="PCA Component 1",
        yaxis_title="PCA Component 2",
        template="plotly_white",
        showlegend=True
    )
    
    return fig

@app.callback(
    Output('cluster-bars', 'figure'),
    Input('update-interval', 'n_intervals')
)
def update_bars(n):
    df = consumer.buffer.get_data()
    
    if df.empty or 'cluster' not in df.columns:
        return go.Figure().update_layout(title="Cluster Distribution")
    
    cluster_counts = df['cluster'].value_counts().sort_index()
    
    fig = go.Figure(data=[
        go.Bar(
            x=[f"C{int(i)}" for i in cluster_counts.index],
            y=cluster_counts.values,
            marker_color=[CLUSTER_COLORS[int(i) % len(CLUSTER_COLORS)] 
                         for i in cluster_counts.index],
            text=cluster_counts.values,
            textposition='auto',
        )
    ])
    
    fig.update_layout(
        title=f"Cluster Distribution (n={len(df)})",
        xaxis_title="Cluster",
        yaxis_title="Count",
        template="plotly_white"
    )
    
    return fig

@app.callback(
    Output('rating-heatmap', 'figure'),
    Input('update-interval', 'n_intervals')
)
def update_heatmap(n):
    df = consumer.buffer.get_data()
    
    if df.empty or len(df) < 5 or 'cluster' not in df.columns or 'rating' not in df.columns:
        return go.Figure().update_layout(title="Rating Distribution by Cluster")
    
    # Create heatmap of rating distribution per cluster
    try:
        pivot = pd.crosstab(df['cluster'], df['rating'], normalize='index') * 100
        
        fig = go.Figure(data=go.Heatmap(
            z=pivot.values,
            x=[f"{int(col)}⭐" for col in pivot.columns],
            y=[f"Cluster {int(idx)}" for idx in pivot.index],
            colorscale='RdYlGn',
            text=pivot.values.round(1),
            texttemplate='%{text}%',
            textfont={"size": 10},
            colorbar=dict(title="Percentage")
        ))
        
        fig.update_layout(
            title="Rating Distribution by Cluster (%)",
            xaxis_title="Rating",
            yaxis_title="Cluster",
            template="plotly_white"
        )
        
        return fig
    except:
        return go.Figure().update_layout(title="Rating Distribution by Cluster")

@app.callback(
    [Output('throughput-chart', 'figure'),
     Output('throughput-history', 'data')],
    [Input('update-interval', 'n_intervals')],
    [State('throughput-history', 'data')]
)
def update_throughput(n, history):
    metrics = consumer.buffer.get_metrics()
    
    # Update history
    current_time = datetime.now()
    history['time'].append(current_time.strftime('%H:%M:%S'))
    history['throughput'].append(metrics['throughput'])
    
    # Keep last 60 data points
    if len(history['time']) > 60:
        history['time'] = history['time'][-60:]
        history['throughput'] = history['throughput'][-60:]
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=history['time'],
        y=history['throughput'],
        mode='lines+markers',
        name='Throughput',
        line=dict(color='#1f77b4', width=2),
        fill='tozeroy'
    ))
    
    fig.update_layout(
        title="Streaming Throughput Over Time",
        xaxis_title="Time",
        yaxis_title="Messages/Second",
        template="plotly_white",
        showlegend=False
    )
    
    return fig, history

if __name__ == '__main__':
    print("=" * 80)
    print("KAFKA STREAMING VISUALIZATION DASHBOARD")
    print("=" * 80)
    print("\n⚠️  Make sure Kafka is running and producer is sending data!")
    print("\n🚀 Starting consumer automatically...")
    
    # FIXED: Auto-start the consumer
    consumer.start()
    time.sleep(1)  # Give it a moment to connect
    
    print("✅ Consumer is now listening for messages")
    print("\nStarting dashboard server...")
    print("🌐 Open your browser and navigate to: http://127.0.0.1:8050")
    print("\nPress Ctrl+C to stop the server")
    print("=" * 80)
    
    try:
        app.run(debug=True, host='127.0.0.1', port=8050)
    finally:
        consumer.close()