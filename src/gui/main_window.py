import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
import numpy as np
from streamlit_option_menu import option_menu
from streamlit_autorefresh import st_autorefresh
import base64

from .gui_adapter import GUIAdapter


class MainWindow:
    """Main GUI window for Atlas database performance monitoring."""
    
    def __init__(self, gui_adapter: GUIAdapter):
        self.adapter = gui_adapter
        self._setup_page_config()
        self._apply_dark_theme()
    
    def _setup_page_config(self):
        """Configure Streamlit page settings."""
        st.set_page_config(
            page_title="Atlas - DB Performance Monitor",
            page_icon="📊",
            layout="wide",
            initial_sidebar_state="expanded"
        )
    
    def _apply_dark_theme(self):
        """Apply custom dark theme with modern styling."""
        st.markdown("""
        <style>
        /* Main background */
        .stApp {
            background: linear-gradient(135deg, #1e1e2e 0%, #2d2d42 100%);
        }
        
        /* Sidebar styling */
        .css-1d391kg {
            background: linear-gradient(180deg, #242438 0%, #1a1a2e 100%);
        }
        
        /* Metric cards */
        [data-testid="metric-container"] {
            background: linear-gradient(145deg, #2a2a3e 0%, #1f1f32 100%);
            border: 1px solid #3a3a54;
            border-radius: 12px;
            box-shadow: 0 4px 15px rgba(0, 0, 0, 0.3);
            padding: 1rem;
        }
        
        /* Headers */
        h1, h2, h3 {
            color: #e1e1e6 !important;
            font-weight: 600 !important;
        }
        
        /* Charts background */
        .plotly-graph-div {
            background: transparent !important;
        }
        
        /* Dataframe styling */
        .stDataFrame {
            background: #2a2a3e;
            border-radius: 8px;
        }
        
        /* Custom metric styling */
        .metric-card {
            background: linear-gradient(145deg, #2a2a3e 0%, #1f1f32 100%);
            border-radius: 12px;
            padding: 20px;
            border: 1px solid #3a3a54;
            box-shadow: 0 4px 15px rgba(0, 0, 0, 0.3);
            margin: 10px 0;
        }
        
        .metric-title {
            color: #a78bfa;
            font-size: 14px;
            font-weight: 500;
            margin-bottom: 8px;
        }
        
        .metric-value {
            color: #e1e1e6;
            font-size: 24px;
            font-weight: 700;
            margin-bottom: 4px;
        }
        
        .metric-delta {
            font-size: 12px;
            font-weight: 500;
        }
        
        /* Button styling */
        .stButton > button {
            background: linear-gradient(145deg, #6366f1 0%, #4f46e5 100%);
            color: white;
            border: none;
            border-radius: 8px;
            font-weight: 600;
            transition: all 0.3s ease;
        }
        
        .stButton > button:hover {
            background: linear-gradient(145deg, #4f46e5 0%, #3730a3 100%);
            box-shadow: 0 4px 15px rgba(99, 102, 241, 0.3);
        }
        </style>
        """, unsafe_allow_html=True)
    
    def run(self):
        """Main application entry point."""
        # Auto-refresh every 30 seconds
        count = st_autorefresh(interval=30000, limit=None, key="atlas_refresh")
        
        # Sidebar navigation
        with st.sidebar:
            st.markdown("""
                    <div style="text-align: center;">
                    <img src="data:image/jpeg;base64,{}" style="width: 200px; height: auto; image-rendering: -webkit-optimize-contrast; image-rendering: crisp-edges;">
                    </div>
                    """.format(self.get_base64_of_image("atlas.jpg")), unsafe_allow_html=True)
            st.markdown("---")
            
            selected = option_menu(
                menu_title="Navigation",
                options=["Dashboard", "Query Analysis", "Performance Trends", "System Metrics", "Query Details"],
                icons=["speedometer2", "search", "graph-up", "cpu", "list-task"],
                menu_icon="cast",
                default_index=0,
                styles={
                    "container": {"padding": "0!important", "background-color": "transparent"},
                    "icon": {"color": "#a78bfa", "font-size": "18px"},
                    "nav-link": {
                        "font-size": "16px",
                        "text-align": "left",
                        "margin": "0px",
                        "color": "#e1e1e6",
                        "background-color": "transparent",
                    },
                    "nav-link-selected": {
                        "background": "linear-gradient(145deg, #6366f1 0%, #4f46e5 100%)",
                        "color": "white",
                        "border-radius": "8px",
                    },
                }
            )
            
            st.markdown("---")
            self._render_sidebar_stats()
        
        # Main content
        if selected == "Dashboard":
            self._render_dashboard()
        elif selected == "Query Analysis":
            self._render_query_analysis()
        elif selected == "Performance Trends":
            self._render_performance_trends()
        elif selected == "System Metrics":
            self._render_system_metrics()
        elif selected == "Query Details":
            self._render_query_details()
    
    def get_base64_of_image(self, path):
        """Convert image to base64 for better quality control."""
        try:
            with open(path, "rb") as img_file:
                return base64.b64encode(img_file.read()).decode()
        except FileNotFoundError:
            return ""
    
    def _render_sidebar_stats(self):
        """Render sidebar statistics."""
        stats = self.adapter.get_summary_stats()
        
        if stats:
            st.markdown("### 📊 Quick Stats")
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Queries", f"{stats.get('total_queries', 0):,}")
                st.metric("Avg Response", f"{stats.get('avg_response_time', 0):.2f}ms")
            
            with col2:
                st.metric("Total Executions", f"{stats.get('total_executions', 0):,}")
                st.metric("Avg CPU Time", f"{stats.get('avg_cpu_time', 0):.2f}ms")
            
            if stats.get('last_collection'):
                st.markdown(f"**Last Update:** {stats['last_collection'].strftime('%H:%M:%S')}")
        else:
            st.info("No data available")
    
    def _render_dashboard(self):
        """Render main dashboard with overview metrics."""
        st.title("📊 Atlas Performance Dashboard")
        
        df = self.adapter.get_dataframe()
        if df.empty:
            st.warning("No performance data available. Please ensure the collector is running.")
            return
        
        # Key metrics row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            avg_response = df['avg_elapsed_time_ms'].mean()
            st.metric(
                "Avg Response Time",
                f"{avg_response:.2f}ms",
                delta=f"{avg_response - df['avg_elapsed_time_ms'].median():.2f}ms"
            )
        
        with col2:
            avg_cpu = df['avg_cpu_time_ms'].mean()
            st.metric(
                "Avg CPU Time",
                f"{avg_cpu:.2f}ms",
                delta=f"{avg_cpu - df['avg_cpu_time_ms'].median():.2f}ms"
            )
        
        with col3:
            avg_reads = df['avg_logical_reads'].mean()
            st.metric(
                "Avg Logical Reads",
                f"{avg_reads:,.0f}",
                delta=f"{avg_reads - df['avg_logical_reads'].median():,.0f}"
            )
        
        with col4:
            total_exec = df['execution_count'].sum()
            st.metric(
                "Total Executions",
                f"{total_exec:,}",
                delta=f"+{len(df)} queries"
            )
        
        st.markdown("---")
        
        # Charts row
        col1, col2 = st.columns(2)
        
        with col1:
            self._render_response_time_distribution(df)
        
        with col2:
            self._render_top_queries_chart(df)
        
        # Second charts row
        col1, col2 = st.columns(2)
        
        with col1:
            self._render_cpu_vs_io_scatter(df)
        
        with col2:
            self._render_execution_frequency_chart(df)
    
    def _render_response_time_distribution(self, df: pd.DataFrame):
        """Render response time distribution histogram."""
        st.subheader("⏱️ Response Time Distribution")
        
        fig = px.histogram(
            df, 
            x='avg_elapsed_time_ms',
            nbins=30,
            title="Query Response Time Distribution",
            color_discrete_sequence=['#6366f1']
        )
        
        fig.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font_color='#e1e1e6',
            title_font_color='#e1e1e6'
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def _render_top_queries_chart(self, df: pd.DataFrame):
        """Render top queries by execution time."""
        st.subheader("🔥 Top Queries by Total Time")
        
        top_queries = df.nlargest(10, 'total_elapsed_time_ms')
        
        fig = px.bar(
            top_queries,
            x='total_elapsed_time_ms',
            y=range(len(top_queries)),
            orientation='h',
            title="Top 10 Queries by Total Elapsed Time",
            color='total_elapsed_time_ms',
            color_continuous_scale='Viridis'
        )
        
        fig.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font_color='#e1e1e6',
            title_font_color='#e1e1e6',
            yaxis_title="Query Rank",
            xaxis_title="Total Elapsed Time (ms)"
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def _render_cpu_vs_io_scatter(self, df: pd.DataFrame):
        """Render CPU vs I/O scatter plot."""
        st.subheader("🖥️ CPU vs I/O Analysis")
        
        fig = px.scatter(
            df,
            x='avg_logical_reads',
            y='avg_cpu_time_ms',
            size='execution_count',
            color='avg_elapsed_time_ms',
            title="CPU Time vs Logical Reads",
            hover_data=['execution_count'],
            color_continuous_scale='Plasma'
        )
        
        fig.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font_color='#e1e1e6',
            title_font_color='#e1e1e6'
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def _render_execution_frequency_chart(self, df: pd.DataFrame):
        """Render execution frequency analysis."""
        st.subheader("📈 Execution Frequency")
        
        # Create bins for execution count ranges
        df_copy = df.copy()
        df_copy['exec_range'] = pd.cut(df_copy['execution_count'], 
                                      bins=[0, 10, 50, 100, 500, float('inf')], 
                                      labels=['1-10', '11-50', '51-100', '101-500', '500+'])
        
        freq_counts = df_copy['exec_range'].value_counts()
        
        fig = px.pie(
            values=freq_counts.values,
            names=freq_counts.index,
            title="Query Execution Frequency Distribution",
            color_discrete_sequence=px.colors.qualitative.Plotly
        )
        
        fig.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font_color='#e1e1e6',
            title_font_color='#e1e1e6'
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def _render_query_analysis(self):
        """Render detailed query analysis page."""
        st.title("🔍 Query Analysis")
        
        df = self.adapter.get_dataframe()
        if df.empty:
            st.warning("No performance data available.")
            return
        
        # Analysis controls
        col1, col2, col3 = st.columns(3)
        
        with col1:
            metric_options = ['avg_elapsed_time_ms', 'avg_cpu_time_ms', 'avg_logical_reads', 
                            'avg_physical_reads', 'total_elapsed_time_ms']
            selected_metric = st.selectbox("Sort by:", metric_options)
        
        with col2:
            limit = st.number_input("Number of queries:", min_value=5, max_value=100, value=20)
        
        with col3:
            min_executions = st.number_input("Min executions:", min_value=1, value=1)
        
        # Filter and sort data
        filtered_df = df[df['execution_count'] >= min_executions]
        top_queries = filtered_df.nlargest(limit, selected_metric)
        
        # Display results
        st.subheader(f"Top {limit} Queries by {selected_metric}")
        
        # Create a summary table
        display_columns = [
            'avg_elapsed_time_ms', 'avg_cpu_time_ms', 'avg_logical_reads',
            'execution_count', 'query_text'
        ]
        
        summary_df = top_queries[display_columns].copy()
        summary_df['query_text'] = summary_df['query_text'].str[:100] + '...'
        
        st.dataframe(
            summary_df,
            use_container_width=True,
            hide_index=True
        )
        
        # Detailed query examination
        st.subheader("🔎 Query Details")
        
        if not top_queries.empty:
            query_index = st.selectbox(
                "Select query for details:",
                range(len(top_queries)),
                format_func=lambda x: f"Query {x+1}: {top_queries.iloc[x]['query_text'][:50]}..."
            )
            
            selected_query = top_queries.iloc[query_index]
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Avg Elapsed Time", f"{selected_query['avg_elapsed_time_ms']:.2f} ms")
                st.metric("Avg CPU Time", f"{selected_query['avg_cpu_time_ms']:.2f} ms")
                st.metric("Execution Count", f"{selected_query['execution_count']:,}")
            
            with col2:
                st.metric("Avg Logical Reads", f"{selected_query['avg_logical_reads']:,.0f}")
                st.metric("Avg Physical Reads", f"{selected_query['avg_physical_reads']:,.0f}")
                if 'avg_rows_returned' in selected_query:
                    st.metric("Avg Rows Returned", f"{selected_query['avg_rows_returned']:,.0f}")
            
            st.subheader("SQL Query Text")
            st.code(selected_query['query_text'], language='sql')
    
    def _render_performance_trends(self):
        """Render performance trends over time."""
        st.title("📈 Performance Trends")
        
        trends = self.adapter.get_performance_trends(24)  # Last 24 hours
        
        if trends.empty:
            st.warning("No trend data available for the selected time period.")
            return
        
        # Time range selector
        time_options = {"Last 6 hours": 6, "Last 12 hours": 12, "Last 24 hours": 24, "Last 48 hours": 48}
        selected_hours = st.selectbox("Time Range:", list(time_options.keys()), index=2)
        
        trends = self.adapter.get_performance_trends(time_options[selected_hours])
        
        if not trends.empty:
            # Multi-metric time series
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=('Response Time Trend', 'CPU Time Trend', 
                              'Logical Reads Trend', 'Execution Count Trend'),
                specs=[[{"secondary_y": False}, {"secondary_y": False}],
                       [{"secondary_y": False}, {"secondary_y": False}]]
            )
            
            # Response time
            fig.add_trace(
                go.Scatter(x=trends['hour'], y=trends['avg_elapsed_time_ms'],
                          name='Avg Response Time', line=dict(color='#6366f1')),
                row=1, col=1
            )
            
            # CPU time
            fig.add_trace(
                go.Scatter(x=trends['hour'], y=trends['avg_cpu_time_ms'],
                          name='Avg CPU Time', line=dict(color='#f59e0b')),
                row=1, col=2
            )
            
            # Logical reads
            fig.add_trace(
                go.Scatter(x=trends['hour'], y=trends['avg_logical_reads'],
                          name='Avg Logical Reads', line=dict(color='#10b981')),
                row=2, col=1
            )
            
            # Execution count
            fig.add_trace(
                go.Scatter(x=trends['hour'], y=trends['execution_count'],
                          name='Total Executions', line=dict(color='#ef4444')),
                row=2, col=2
            )
            
            fig.update_layout(
                height=600,
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='#e1e1e6',
                title_font_color='#e1e1e6',
                showlegend=False
            )
            
            st.plotly_chart(fig, use_container_width=True)
    
    def _render_system_metrics(self):
        """Render system-level metrics."""
        st.title("🖥️ System Metrics")
        
        df = self.adapter.get_dataframe()
        if df.empty:
            st.warning("No system metrics available.")
            return
        
        # Memory and threading metrics
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("💾 Memory Usage")
            if 'avg_grant_kb' in df.columns:
                fig = px.box(df, y='avg_grant_kb', title="Memory Grant Distribution (KB)")
                fig.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='#e1e1e6'
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("🧵 Threading Metrics")
            if 'total_used_threads' in df.columns:
                fig = px.histogram(df, x='total_used_threads', title="Thread Usage Distribution")
                fig.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font_color='#e1e1e6'
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # I/O metrics
        st.subheader("📊 I/O Performance")
        
        if all(col in df.columns for col in ['avg_logical_reads', 'avg_physical_reads', 'avg_logical_writes']):
            io_metrics = df[['avg_logical_reads', 'avg_physical_reads', 'avg_logical_writes']].mean()
            
            fig = px.bar(
                x=io_metrics.index,
                y=io_metrics.values,
                title="Average I/O Operations",
                color=io_metrics.values,
                color_continuous_scale='Viridis'
            )
            
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='#e1e1e6'
            )
            
            st.plotly_chart(fig, use_container_width=True)
    
    def _render_query_details(self):
        """Render detailed query information."""
        st.title("📝 Query Details")
        
        df = self.adapter.get_dataframe()
        if df.empty:
            st.warning("No query details available.")
            return
        
        # Search and filter
        search_term = st.text_input("🔍 Search queries:", placeholder="Enter keywords to search...")
        
        if search_term:
            mask = df['query_text'].str.contains(search_term, case=False, na=False)
            filtered_df = df[mask]
        else:
            filtered_df = df
        
        st.write(f"Showing {len(filtered_df)} of {len(df)} queries")
        
        # Pagination
        queries_per_page = 10
        total_pages = (len(filtered_df) - 1) // queries_per_page + 1
        
        if total_pages > 1:
            page = st.number_input("Page:", min_value=1, max_value=total_pages, value=1)
            start_idx = (page - 1) * queries_per_page
            end_idx = start_idx + queries_per_page
            page_df = filtered_df.iloc[start_idx:end_idx]
        else:
            page_df = filtered_df
        
        # Display queries
        for idx, row in page_df.iterrows():
            with st.expander(f"Query {idx + 1}: {row['query_text'][:80]}..."):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Avg Response Time", f"{row['avg_elapsed_time_ms']:.2f} ms")
                    st.metric("Execution Count", f"{row['execution_count']:,}")
                
                with col2:
                    st.metric("Avg CPU Time", f"{row['avg_cpu_time_ms']:.2f} ms")
                    st.metric("Avg Logical Reads", f"{row['avg_logical_reads']:,.0f}")
                
                with col3:
                    if 'avg_rows_returned' in row:
                        st.metric("Avg Rows", f"{row['avg_rows_returned']:,.0f}")
                    st.metric("Last Execution", row['last_execution_time'].strftime('%Y-%m-%d %H:%M'))
                
                st.subheader("SQL Query")
                st.code(row['query_text'], language='sql')
                
                if 'query_plan' in row and row['query_plan']:
                    with st.expander("📋 Query Plan"):
                        st.text(row['query_plan'])


def create_gui(adapter: GUIAdapter) -> MainWindow:
    """Factory function to create and configure the main GUI window."""
    return MainWindow(adapter)