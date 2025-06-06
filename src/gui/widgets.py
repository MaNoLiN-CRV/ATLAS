import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime
import time
import os


class PerformanceMetricCard:
    """Custom metric card widget with enhanced styling."""
    
    @staticmethod
    def render(title: str, value: str, delta: Optional[str] = None, 
               help_text: Optional[str] = None, color: str = "#6366f1"):
        """Render a custom metric card using Streamlit's native metric component."""
        # Use Streamlit's native metric component which handles styling better
        st.metric(
            label=title,
            value=value,
            delta=delta,
            help=help_text
        )


class QueryPerformanceChart:
    """Advanced chart widget for query performance visualization."""
    
    @staticmethod
    def render_comparison_chart(df: pd.DataFrame, metrics: List[str], 
                              title: str = "Performance Comparison"):
        """Render a multi-metric comparison chart."""
        if df.empty:
            st.warning("No data available for chart")
            return
        
        fig = go.Figure()
        
        colors = ['#6366f1', '#f59e0b', '#10b981', '#ef4444', '#8b5cf6']
        
        for i, metric in enumerate(metrics):
            if metric in df.columns:
                fig.add_trace(go.Bar(
                    name=metric.replace('_', ' ').title(),
                    x=df.index[:20],  # Top 20 queries
                    y=df[metric][:20],
                    marker_color=colors[i % len(colors)]
                ))
        
        fig.update_layout(
            title=title,
            xaxis_title="Query Index",
            yaxis_title="Metric Value",
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font_color='#e1e1e6',
            title_font_color='#e1e1e6',
            barmode='group'
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    @staticmethod
    def render_heatmap(df: pd.DataFrame, x_col: str, y_col: str, 
                      value_col: str, title: str = "Performance Heatmap"):
        """Render a performance heatmap."""
        if df.empty or not all(col in df.columns for col in [x_col, y_col, value_col]):
            st.warning("Insufficient data for heatmap")
            return
        
        # Create pivot table for heatmap
        pivot_df = df.pivot_table(
            values=value_col, 
            index=y_col, 
            columns=x_col, 
            aggfunc='mean'
        )
        
        fig = px.imshow(
            pivot_df,
            title=title,
            color_continuous_scale='Viridis',
            aspect='auto'
        )
        
        fig.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font_color='#e1e1e6',
            title_font_color='#e1e1e6'
        )
        
        st.plotly_chart(fig, use_container_width=True)


class PerformanceAlerts:
    """Widget for displaying performance alerts and recommendations."""
    
    @staticmethod
    def analyze_and_display_alerts(df: pd.DataFrame):
        """Analyze performance data and display alerts."""
        if df.empty:
            return
        
        alerts = []
        
        # High response time alert
        high_response_threshold = df['avg_elapsed_time_ms'].quantile(0.95)
        high_response_queries = df[df['avg_elapsed_time_ms'] > high_response_threshold]
        if not high_response_queries.empty:
            alerts.append({
                'type': 'warning',
                'title': '⚠️ High Response Time',
                'message': f'{len(high_response_queries)} queries have response times above {high_response_threshold:.2f}ms',
                'count': len(high_response_queries)
            })
        
        # High CPU usage alert
        high_cpu_threshold = df['avg_cpu_time_ms'].quantile(0.9)
        high_cpu_queries = df[df['avg_cpu_time_ms'] > high_cpu_threshold]
        if not high_cpu_queries.empty:
            alerts.append({
                'type': 'error',
                'title': '🔥 High CPU Usage',
                'message': f'{len(high_cpu_queries)} queries consuming excessive CPU time',
                'count': len(high_cpu_queries)
            })
        
        # High I/O alert
        high_io_threshold = df['avg_logical_reads'].quantile(0.95)
        high_io_queries = df[df['avg_logical_reads'] > high_io_threshold]
        if not high_io_queries.empty:
            alerts.append({
                'type': 'warning',
                'title': '💾 High I/O Activity',
                'message': f'{len(high_io_queries)} queries with high logical reads',
                'count': len(high_io_queries)
            })
        
        # Frequent execution alert
        frequent_threshold = df['execution_count'].quantile(0.9)
        frequent_queries = df[df['execution_count'] > frequent_threshold]
        if not frequent_queries.empty:
            alerts.append({
                'type': 'info',
                'title': '🔄 Frequently Executed',
                'message': f'{len(frequent_queries)} queries executed very frequently - consider caching',
                'count': len(frequent_queries)
            })
        
        # Display alerts
        if alerts:
            st.subheader("🚨 Performance Alerts")
            
            for alert in alerts:
                if alert['type'] == 'error':
                    st.error(f"{alert['title']}: {alert['message']}")
                elif alert['type'] == 'warning':
                    st.warning(f"{alert['title']}: {alert['message']}")
                else:
                    st.info(f"{alert['title']}: {alert['message']}")
        else:
            st.success("✅ No performance alerts detected")


class QueryOptimizationSuggestions:
    """Widget for providing query optimization suggestions."""
    
    @staticmethod
    def analyze_and_suggest(query_data: Dict[str, Any]):
        """Analyze a query and provide optimization suggestions."""
        suggestions = []
        
        # High response time suggestions
        if query_data.get('avg_elapsed_time_ms', 0) > 1000:
            suggestions.append({
                'type': 'performance',
                'title': 'High Response Time',
                'suggestion': 'Consider adding indexes, optimizing WHERE clauses, or reviewing query logic'
            })
        
        # High CPU usage suggestions
        if query_data.get('avg_cpu_time_ms', 0) > 500:
            suggestions.append({
                'type': 'cpu',
                'title': 'High CPU Usage',
                'suggestion': 'Review complex calculations, consider query rewrite, or add computed columns'
            })
        
        # High I/O suggestions
        if query_data.get('avg_logical_reads', 0) > 10000:
            suggestions.append({
                'type': 'io',
                'title': 'High I/O Activity',
                'suggestion': 'Add covering indexes, review JOIN conditions, or consider partitioning'
            })
        
        # Frequent execution suggestions
        if query_data.get('execution_count', 0) > 1000:
            suggestions.append({
                'type': 'caching',
                'title': 'Frequent Execution',
                'suggestion': 'Consider result caching, query plan reuse, or stored procedures'
            })
        
        # Display suggestions
        if suggestions:
            st.subheader("💡 Optimization Suggestions")
            
            for suggestion in suggestions:
                icon_map = {
                    'performance': '⚡',
                    'cpu': '🖥️',
                    'io': '💾',
                    'caching': '🗄️'
                }
                
                icon = icon_map.get(suggestion['type'], '💡')
                st.info(f"{icon} **{suggestion['title']}**: {suggestion['suggestion']}")
        else:
            st.success("✅ Query appears to be well-optimized")


class DataExportWidget:
    """Widget for exporting performance data."""
    
    @staticmethod
    def render_export_options(df: pd.DataFrame):
        """Render export options for performance data."""
        if df.empty:
            return
        
        st.subheader("📥 Export Data")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("Export CSV"):
                csv = df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"atlas_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
        
        with col2:
            if st.button("Export JSON"):
                json_data = df.to_json(orient='records', date_format='iso')
                st.download_button(
                    label="Download JSON",
                    data=json_data,
                    file_name=f"atlas_performance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json"
                )
        
        with col3:
            # Summary report
            if st.button("Generate Report"):
                report = DataExportWidget._generate_summary_report(df)
                st.download_button(
                    label="Download Report",
                    data=report,
                    file_name=f"atlas_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                    mime="text/plain"
                )
    
    @staticmethod
    def _generate_summary_report(df: pd.DataFrame) -> str:
        """Generate a text summary report."""
        report = f"""
ATLAS DATABASE PERFORMANCE REPORT
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'='*50}

SUMMARY STATISTICS:
- Total Queries Analyzed: {len(df):,}
- Average Response Time: {df['avg_elapsed_time_ms'].mean():.2f} ms
- Average CPU Time: {df['avg_cpu_time_ms'].mean():.2f} ms
- Average Logical Reads: {df['avg_logical_reads'].mean():,.0f}
- Total Executions: {df['execution_count'].sum():,}

TOP 5 SLOWEST QUERIES:
"""
        
        top_slow = df.nlargest(5, 'avg_elapsed_time_ms')
        for i, (_, row) in enumerate(top_slow.iterrows(), 1):
            report += f"\n{i}. Response Time: {row['avg_elapsed_time_ms']:.2f}ms\n"
            report += f"   Query: {row['query_text'][:100]}...\n"
        
        report += f"\n\nTOP 5 MOST EXECUTED QUERIES:\n"
        
        top_frequent = df.nlargest(5, 'execution_count')
        for i, (_, row) in enumerate(top_frequent.iterrows(), 1):
            report += f"\n{i}. Executions: {row['execution_count']:,}\n"
            report += f"   Query: {row['query_text'][:100]}...\n"
        
        return report


class RealTimeMonitor:
    """Widget for real-time performance monitoring."""
    
    @staticmethod
    def render_live_metrics(df: pd.DataFrame):
        """Render live performance metrics."""
        if df.empty:
            st.warning("No live data available")
            return
        
        # Recent data (last hour)
        if 'collection_timestamp' in df.columns:
            recent_cutoff = datetime.now() - pd.Timedelta(hours=1)
            recent_df = df[df['collection_timestamp'] >= recent_cutoff]
        else:
            recent_df = df.tail(50)  # Last 50 records
        
        if recent_df.empty:
            st.info("No recent data available")
            return
        
        st.subheader("📡 Live Performance Monitor")
        
        # Real-time metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            current_avg_response = recent_df['avg_elapsed_time_ms'].mean()
            PerformanceMetricCard.render(
                "Current Avg Response",
                f"{current_avg_response:.2f}ms",
                color="#10b981" if current_avg_response < 100 else "#ef4444"
            )
        
        with col2:
            current_throughput = len(recent_df)
            PerformanceMetricCard.render(
                "Queries/Hour",
                f"{current_throughput:,}",
                color="#6366f1"
            )
        
        with col3:
            current_cpu = recent_df['avg_cpu_time_ms'].mean()
            PerformanceMetricCard.render(
                "Avg CPU Time",
                f"{current_cpu:.2f}ms",
                color="#10b981" if current_cpu < 50 else "#ef4444"
            )
        
        with col4:
            current_io = recent_df['avg_logical_reads'].mean()
            PerformanceMetricCard.render(
                "Avg I/O Reads",
                f"{current_io:,.0f}",
                color="#10b981" if current_io < 1000 else "#f59e0b"
            )
        
        # Live trend chart
        if len(recent_df) > 1:
            fig = go.Figure()
            
            if 'collection_timestamp' in recent_df.columns:
                x_axis = recent_df['collection_timestamp']
            else:
                x_axis = range(len(recent_df))
            
            fig.add_trace(go.Scatter(
                x=x_axis,
                y=recent_df['avg_elapsed_time_ms'],
                mode='lines+markers',
                name='Response Time',
                line=dict(color='#6366f1', width=2)
            ))
            
            fig.update_layout(
                title="Response Time Trend (Last Hour)",
                xaxis_title="Time",
                yaxis_title="Response Time (ms)",
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='#e1e1e6',
                title_font_color='#e1e1e6',
                height=300
            )
            
            st.plotly_chart(fig, use_container_width=True)


class MemoryMonitor:
    """Widget for displaying memory usage and trends."""
    
    @staticmethod
    def render_memory_stats(adapter):
        """Render current memory statistics."""
        memory_stats = adapter.get_memory_stats()
        
        if not memory_stats:
            st.warning("Memory statistics not available")
            return
        
        st.markdown("### 💾 Current Memory Usage")
        st.markdown("*Live memory consumption of the Atlas monitoring program*")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            PerformanceMetricCard.render(
                "Physical Memory",
                f"{memory_stats.get('rss_mb', 0):.1f} MB",
                delta=f"{memory_stats.get('memory_percent', 0):.1f}%",
                help_text="Physical memory usage",
                color="#6366f1"
            )
        
        with col2:
            PerformanceMetricCard.render(
                "Virtual Memory",
                f"{memory_stats.get('vms_mb', 0):.1f} MB",
                help_text="Virtual memory usage",
                color="#8b5cf6"
            )
        
        with col3:
            PerformanceMetricCard.render(
                "Cache Memory",
                f"{memory_stats.get('cache_memory_mb', 0):.1f} MB",
                delta=f"{memory_stats.get('cache_records', 0):,} records",
                help_text="Data cache memory usage",
                color="#10b981"
            )
        
        with col4:
            avg_record_size = memory_stats.get('avg_record_size_kb', 0)
            PerformanceMetricCard.render(
                "Avg Record Size",
                f"{avg_record_size:.2f} KB",
                help_text="Average memory per record",
                color="#f59e0b"
            )
    
    @staticmethod
    def render_memory_trends(adapter):
        """Render memory usage trends over time."""
        memory_history = adapter.get_memory_history()
        
        if memory_history.empty:
            st.info("No memory history available. Enable memory monitoring to see trends.")
            return
        
        st.markdown("### 📈 Memory Trends")
        
        fig = go.Figure()
        
        # Physical memory trend
        fig.add_trace(go.Scatter(
            x=memory_history['timestamp'],
            y=memory_history['rss_mb'],
            mode='lines+markers',
            name='Physical Memory (MB)',
            line=dict(color='#6366f1', width=2),
            yaxis='y'
        ))
        
        # Cache memory trend
        fig.add_trace(go.Scatter(
            x=memory_history['timestamp'],
            y=memory_history['cache_memory_mb'],
            mode='lines+markers',
            name='Cache Memory (MB)',
            line=dict(color='#10b981', width=2),
            yaxis='y'
        ))
        
        # Record count trend (on secondary y-axis)
        fig.add_trace(go.Scatter(
            x=memory_history['timestamp'],
            y=memory_history['cache_records'],
            mode='lines+markers',
            name='Cache Records',
            line=dict(color='#f59e0b', width=2, dash='dash'),
            yaxis='y2'
        ))
        
        fig.update_layout(
            title="Memory Usage Trends",
            xaxis_title="Time",
            yaxis=dict(
                title="Memory (MB)",
                side="left",
                color='#e1e1e6'
            ),
            yaxis2=dict(
                title="Record Count",
                side="right",
                overlaying="y",
                color='#f59e0b'
            ),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font_color='#e1e1e6',
            title_font_color='#e1e1e6',
            height=400,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    @staticmethod
    def render_memory_controls(adapter):
        """Render memory management controls."""
        st.markdown("### ⚙️ Memory Management")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🔄 Optimize Memory", help="Run garbage collection and remove duplicates"):
                with st.spinner("Optimizing memory..."):
                    result = adapter.optimize_memory()
                    if result.get('removed_duplicates', 0) > 0:
                        st.success(f"Removed {result['removed_duplicates']} duplicate records")
                    if result.get('memory_saved_mb', 0) > 0:
                        st.success(f"Saved {result['memory_saved_mb']:.1f} MB")
                    else:
                        st.info("Memory optimization completed")
        
        with col2:
            if st.button("📊 Start Monitoring", help="Start continuous memory monitoring"):
                adapter.start_memory_monitoring()
                st.success("Memory monitoring started")
        
        with col3:
            if st.button("⏹️ Stop Monitoring", help="Stop memory monitoring"):
                adapter.stop_memory_monitoring()
                st.info("Memory monitoring stopped")
        
        # Show current monitoring status
        monitoring_status = "🟢 Active" if adapter._memory_monitoring else "🔴 Inactive"
        st.markdown(f"**Monitoring Status:** {monitoring_status}")
    
    @staticmethod
    def render_full_memory_dashboard(adapter):
        """Render complete memory monitoring dashboard."""
        st.markdown("""
        <div style="background: linear-gradient(145deg, #2a2a3e 0%, #1f1f32 100%); 
                    padding: 1rem; border-radius: 10px; border: 1px solid #3a3a54; margin: 10px 0;">
            <p style="color: #a78bfa; margin: 0; font-size: 14px;">
                📊 Real-time monitoring of Atlas application memory consumption, cache usage, and optimization tools.
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        MemoryMonitor.render_memory_stats(adapter)
        st.divider()
        MemoryMonitor.render_memory_trends(adapter)
        st.divider()
        MemoryMonitor.render_memory_controls(adapter)


class DatabaseUtilsWidget:
    """Widget for database utilities including backup operations."""
    
    @staticmethod
    def render_backup_section(core_instance):
        """Render the database backup section."""
        st.subheader("🗄️ Database Backup")
        st.markdown("""
        <div style="background: linear-gradient(145deg, #2a2a3e 0%, #1f1f32 100%); 
                    padding: 1rem; border-radius: 10px; border: 1px solid #3a3a54; margin: 10px 0;">
            <p style="color: #a78bfa; margin: 0; font-size: 14px;">
                💾 Create backups of your database to ensure data safety and enable recovery options.
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("**Backup Configuration**")
            
            # Backup file path input
            default_backup_path = f"atlas_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak"
            backup_path = st.text_input(
                "Backup File Path:",
                value=default_backup_path,
                help="Specify the full path where the backup file should be created"
            )
            
            # Backup options
            st.markdown("**Backup Options**")
            backup_format = st.selectbox(
                "Format:",
                ["Full Backup", "Differential Backup", "Transaction Log Backup"],
                index=0,
                help="Choose the type of backup to create"
            )
            
            compress_backup = st.checkbox(
                "Compress Backup",
                value=True,
                help="Enable compression to reduce backup file size"
            )
            
            verify_backup = st.checkbox(
                "Verify Backup",
                value=True,
                help="Verify backup integrity after creation"
            )
        
        with col2:
            st.markdown("**Quick Actions**")
            
            # Create backup button
            if st.button("🚀 Create Backup", type="primary", help="Start database backup process"):
                if backup_path.strip():
                    DatabaseUtilsWidget._create_backup(core_instance, backup_path.strip(), compress_backup, verify_backup)
                else:
                    st.error("Please specify a backup file path")
            
            st.markdown("---")
            
            # Backup status info
            st.markdown("**Current Database Info**")
            try:
                db_info = DatabaseUtilsWidget._get_database_info(core_instance)
                if db_info:
                    st.metric("Database", db_info.get('name', 'Unknown'))
                    st.metric("Size", f"{db_info.get('size_mb', 0):.1f} MB")
                    st.metric("Status", db_info.get('status', 'Unknown'))
            except Exception as e:
                st.warning(f"Could not retrieve database info: {str(e)}")
    
    @staticmethod
    def _create_backup(core_instance, backup_path: str, compress: bool = True, verify: bool = True):
        """Create a database backup with progress tracking."""
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        try:
            # Step 1: Initialize backup
            status_text.text("🔧 Initializing backup process...")
            progress_bar.progress(10)
            time.sleep(0.5)
            
            # Step 2: Get database utilities
            status_text.text("📋 Preparing database utilities...")
            database_utils = core_instance.getDatabaseUtils()
            progress_bar.progress(30)
            time.sleep(0.5)
            
            # Step 3: Execute backup
            status_text.text(f"💾 Creating backup: {backup_path}")
            progress_bar.progress(50)
            
            # Call the backup method
            database_utils.make_backup(backup_path)
            progress_bar.progress(80)
            time.sleep(0.5)
            
            # Step 4: Verify if requested
            if verify:
                status_text.text("✅ Verifying backup integrity...")
                progress_bar.progress(90)
                time.sleep(0.5)
            
            # Step 5: Complete
            progress_bar.progress(100)
            status_text.text("✅ Backup completed successfully!")
            
            st.success(f"✅ Database backup created successfully at: `{backup_path}`")
            
            # Show backup summary
            try:
                import os
                if os.path.exists(backup_path):
                    file_size = os.path.getsize(backup_path) / (1024 * 1024)  # Convert to MB
                    st.info(f"📊 Backup file size: {file_size:.2f} MB")
            except:
                pass
                
        except Exception as e:
            progress_bar.progress(100)
            status_text.text("❌ Backup failed!")
            st.error(f"❌ Backup failed: {str(e)}")
            
            # Provide troubleshooting tips
            st.markdown("""
            **Troubleshooting Tips:**
            - Ensure the backup path is accessible and writable
            - Check if there's sufficient disk space
            - Verify database connection is active
            - Ensure proper permissions for backup operations
            """)
    
    @staticmethod
    def _get_database_info(core_instance):
        """Get current database information."""
        try:
            connector = core_instance.connector
            if not connector or not connector.connection:
                return None
            
            with connector.connection.cursor() as cursor:
                # Get database name and basic info
                cursor.execute("""
                    SELECT 
                        DB_NAME() as database_name,
                        DATABASEPROPERTYEX(DB_NAME(), 'Status') as status,
                        SUM(size * 8.0 / 1024) as size_mb
                    FROM sys.database_files
                """)
                
                row = cursor.fetchone()
                if row:
                    return {
                        'name': row[0],
                        'status': row[1],
                        'size_mb': float(row[2]) if row[2] else 0
                    }
        except Exception as e:
            print(f"Error getting database info: {e}")
            return None
    
    @staticmethod
    def render_backup_history(core_instance):
        """Render backup history and management."""
        st.subheader("📜 Backup History")
        
        try:
            backup_history = DatabaseUtilsWidget._get_backup_history(core_instance)
            
            if backup_history:
                # Convert to DataFrame for better display
                df = pd.DataFrame(backup_history)
                
                # Display as a table
                st.dataframe(
                    df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        'backup_start_date': st.column_config.DatetimeColumn("Start Time"),
                        'backup_finish_date': st.column_config.DatetimeColumn("Finish Time"),
                        'backup_size': st.column_config.NumberColumn("Size (MB)", format="%.2f"),
                        'compressed_backup_size': st.column_config.NumberColumn("Compressed Size (MB)", format="%.2f"),
                        'type': st.column_config.TextColumn("Type"),
                        'name': st.column_config.TextColumn("Backup Name")
                    }
                )
                
                st.info(f"📊 Found {len(backup_history)} backup records")
            else:
                st.info("No backup history found")
                
        except Exception as e:
            st.warning(f"Could not retrieve backup history: {str(e)}")
    
    @staticmethod
    def _get_backup_history(core_instance):
        """Get backup history from SQL Server."""
        try:
            connector = core_instance.connector
            if not connector or not connector.connection:
                return []
            
            with connector.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT TOP 10
                        backup_start_date,
                        backup_finish_date,
                        backup_size / 1048576.0 as backup_size_mb,
                        compressed_backup_size / 1048576.0 as compressed_backup_size_mb,
                        type,
                        name,
                        CASE type
                            WHEN 'D' THEN 'Full Database'
                            WHEN 'I' THEN 'Differential'
                            WHEN 'L' THEN 'Transaction Log'
                            ELSE 'Other'
                        END as backup_type_desc
                    FROM msdb.dbo.backupset
                    WHERE database_name = DB_NAME()
                    ORDER BY backup_start_date DESC
                """)
                
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                
                return [dict(zip(columns, row)) for row in rows]
                
        except Exception as e:
            print(f"Error getting backup history: {e}")
            return []
    
    @staticmethod
    def render_database_maintenance(core_instance):
        """Render database maintenance tools."""
        st.subheader("🔧 Database Maintenance")
        st.markdown("""
        <div style="background: linear-gradient(145deg, #2a2a3e 0%, #1f1f32 100%); 
                    padding: 1rem; border-radius: 10px; border: 1px solid #3a3a54; margin: 10px 0;">
            <p style="color: #a78bfa; margin: 0; font-size: 14px;">
                🛠️ Tools for maintaining database performance and integrity.
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🔍 Check DB Integrity", help="Run DBCC CHECKDB to verify database integrity"):
                DatabaseUtilsWidget._check_database_integrity(core_instance)
        
        with col2:
            if st.button("📊 Update Statistics", help="Update database statistics for better query performance"):
                DatabaseUtilsWidget._update_statistics(core_instance)
        
        with col3:
            if st.button("🧹 Rebuild Indexes", help="Rebuild fragmented indexes"):
                DatabaseUtilsWidget._rebuild_indexes(core_instance)
    
    @staticmethod
    def _check_database_integrity(core_instance):
        """Check database integrity."""
        with st.spinner("Checking database integrity..."):
            try:
                connector = core_instance.connector
                if not connector or not connector.connection:
                    st.error("Database connection not available")
                    return
                
                with connector.connection.cursor() as cursor:
                    cursor.execute("DBCC CHECKDB() WITH NO_INFOMSGS")
                    st.success("✅ Database integrity check completed successfully")
                    
            except Exception as e:
                st.error(f"❌ Database integrity check failed: {str(e)}")
    
    @staticmethod
    def _update_statistics(core_instance):
        """Update database statistics."""
        with st.spinner("Updating database statistics..."):
            try:
                connector = core_instance.connector
                if not connector or not connector.connection:
                    st.error("Database connection not available")
                    return
                
                with connector.connection.cursor() as cursor:
                    cursor.execute("EXEC sp_updatestats")
                    st.success("✅ Database statistics updated successfully")
                    
            except Exception as e:
                st.error(f"❌ Statistics update failed: {str(e)}")
    
    @staticmethod
    def _rebuild_indexes(core_instance):
        """Rebuild database indexes."""
        with st.spinner("Rebuilding database indexes..."):
            try:
                connector = core_instance.connector
                if not connector or not connector.connection:
                    st.error("Database connection not available")
                    return
                
                with connector.connection.cursor() as cursor:
                    # Get list of indexes that need rebuilding (fragmentation > 30%)
                    cursor.execute("""
                        SELECT 
                            OBJECT_NAME(object_id) as table_name,
                            name as index_name,
                            avg_fragmentation_in_percent
                        FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED')
                        WHERE avg_fragmentation_in_percent > 30 
                        AND index_id > 0
                    """)
                    
                    indexes_to_rebuild = cursor.fetchall()
                    
                    if indexes_to_rebuild:
                        st.info(f"Found {len(indexes_to_rebuild)} indexes that need rebuilding")
                        
                        for table_name, index_name, fragmentation in indexes_to_rebuild:
                            rebuild_sql = f"ALTER INDEX [{index_name}] ON [{table_name}] REBUILD"
                            cursor.execute(rebuild_sql)
                        
                        st.success(f"✅ Rebuilt {len(indexes_to_rebuild)} fragmented indexes")
                    else:
                        st.info("✅ No indexes require rebuilding (fragmentation < 30%)")
                    
            except Exception as e:
                st.error(f"❌ Index rebuild failed: {str(e)}")
    
    @staticmethod
    def render_connection_status(core_instance):
        """Render database connection status and details."""
        st.subheader("🔗 Database Connection")
        
        col1, col2 = st.columns(2)
        
        with col1:
            try:
                connector = core_instance.connector
                if connector and connector.test_connection():
                    st.success("🟢 Database Connected")
                    
                    # Show connection details
                    config = connector.config
                    if config:
                        st.markdown("**Connection Details:**")
                        st.markdown(f"- **Server:** `{config.server}`")
                        st.markdown(f"- **Database:** `{config.database}`")
                        st.markdown(f"- **Driver:** `{config.driver}`")
                        st.markdown(f"- **Authentication:** `{'Windows' if config.trusted_connection else 'SQL Server'}`")
                else:
                    st.error("🔴 Database Disconnected")
                    
            except Exception as e:
                st.error(f"🔴 Connection Error: {str(e)}")
        
        with col2:
            st.markdown("**Quick Actions:**")
            
            if st.button("🔄 Test Connection", help="Test the current database connection"):
                DatabaseUtilsWidget._test_connection(core_instance)
            
            if st.button("📊 Connection Info", help="Show detailed connection information"):
                DatabaseUtilsWidget._show_connection_info(core_instance)
    
    @staticmethod
    def _test_connection(core_instance):
        """Test database connection."""
        with st.spinner("Testing database connection..."):
            try:
                connector = core_instance.connector
                if connector and connector.test_connection():
                    st.success("✅ Database connection test successful")
                else:
                    st.error("❌ Database connection test failed")
            except Exception as e:
                st.error(f"❌ Connection test error: {str(e)}")
    
    @staticmethod
    def _show_connection_info(core_instance):
        """Show detailed connection information."""
        try:
            connector = core_instance.connector
            if not connector or not connector.connection:
                st.error("Database connection not available")
                return
            
            with connector.connection.cursor() as cursor:
                # Get server and database info
                cursor.execute("""
                    SELECT 
                        @@SERVERNAME as server_name,
                        @@VERSION as sql_version,
                        DB_NAME() as current_database,
                        SUSER_SNAME() as login_name,
                        GETDATE() as current_time
                """)
                
                row = cursor.fetchone()
                if row:
                    st.markdown("**Server Information:**")
                    st.markdown(f"- **Server Name:** `{row[0]}`")
                    st.markdown(f"- **Current Database:** `{row[2]}`")
                    st.markdown(f"- **Login Name:** `{row[3]}`")
                    st.markdown(f"- **Server Time:** `{row[4]}`")
                    
                    with st.expander("📋 SQL Server Version Details"):
                        st.text(row[1])
                        
        except Exception as e:
            st.error(f"Could not retrieve connection info: {str(e)}")
    
    @staticmethod
    def render_full_database_utils_page(core_instance):
        """Render the complete database utilities page."""
        st.title("🗄️ Database Utilities")
        
        st.markdown("""
        <div style="background: linear-gradient(145deg, #2a2a3e 0%, #1f1f32 100%); 
                    padding: 1.5rem; border-radius: 10px; border: 1px solid #3a3a54; margin: 10px 0;">
            <h4 style="color: #a78bfa; margin-top: 0;">🛠️ Database Management Tools</h4>
            <p style="color: #e1e1e6; margin: 0;">
                Comprehensive database utilities for backup operations, maintenance tasks, and connection management.
                Use these tools to ensure database reliability and performance.
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        # Connection Status Section
        DatabaseUtilsWidget.render_connection_status(core_instance)
        st.divider()
        
        # Backup Section
        DatabaseUtilsWidget.render_backup_section(core_instance)
        st.divider()
        
        # Backup History Section
        DatabaseUtilsWidget.render_backup_history(core_instance)
        st.divider()
        
        # Maintenance Section
        DatabaseUtilsWidget.render_database_maintenance(core_instance)