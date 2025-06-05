"""
Loading components for Atlas GUI.
Provides modern and minimalist loading animations for app initialization.
"""

import streamlit as st
import time
from typing import Optional, List, Callable
import threading


class LoadingManager:
    """Modern loading manager with progress tracking."""
    
    def __init__(self):
        self.progress_container = None
        self.progress_bar = None
        self.status_text = None
        self.progress_value = 0
        self.total_steps = 0
        self.current_step = 0
        
    def initialize_loading(self, title: str = "🚀 Initializing Atlas", total_steps: int = 5):
        """Initialize the loading interface."""
        self.total_steps = total_steps
        self.current_step = 0
        self.progress_value = 0
        
        # Clear the page and show loading
        st.markdown("""
        <style>
        .loading-container {
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            min-height: 70vh;
            text-align: center;
            background: linear-gradient(135deg, #1e1e2e 0%, #2d2d44 100%);
            border-radius: 20px;
            padding: 3rem;
            margin: 2rem 0;
            box-shadow: 0 20px 40px rgba(0, 0, 0, 0.3);
        }
        
        .loading-title {
            color: #e1e1e6;
            font-size: 2.5rem;
            font-weight: 600;
            margin-bottom: 1rem;
            background: linear-gradient(135deg, #6366f1, #a78bfa);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }
        
        .loading-subtitle {
            color: #9ca3af;
            font-size: 1.1rem;
            margin-bottom: 2rem;
            opacity: 0.8;
        }
        
        .progress-container {
            width: 100%;
            max-width: 400px;
            margin: 1rem 0;
        }
        
        .status-text {
            color: #a78bfa;
            font-size: 1rem;
            margin-top: 1rem;
            font-weight: 500;
        }
        
        .loading-dots {
            display: inline-block;
            animation: loading-dots 1.5s infinite;
        }
        
        @keyframes loading-dots {
            0%, 20% { opacity: 0; }
            50% { opacity: 1; }
            80%, 100% { opacity: 0; }
        }
        
        .pulse-animation {
            animation: pulse 2s infinite;
        }
        
        @keyframes pulse {
            0% { transform: scale(1); }
            50% { transform: scale(1.05); }
            100% { transform: scale(1); }
        }
        </style>
        """, unsafe_allow_html=True)
        
        # Create the loading container
        with st.container():
            st.markdown(f"""
            <div class="loading-container pulse-animation">
                <div class="loading-title">{title}</div>
                <div class="loading-subtitle">Setting up your performance monitoring dashboard</div>
            </div>
            """, unsafe_allow_html=True)
            
            # Progress tracking section
            self.progress_container = st.container()
            with self.progress_container:
                self.progress_bar = st.progress(0)
                self.status_text = st.empty()
                
        # Initial status
        self.update_status("🔧 Starting initialization...")
        
    def update_progress(self, step_name: str, step_description: str = ""):
        """Update the loading progress."""
        self.current_step += 1
        self.progress_value = min(self.current_step / self.total_steps, 1.0)
        
        # Update progress bar
        if self.progress_bar:
            self.progress_bar.progress(self.progress_value)
            
        # Update status text
        status_message = f"**{step_name}**"
        if step_description:
            status_message += f" - {step_description}"
            
        self.update_status(status_message)
        
        # Small delay for visual feedback
        time.sleep(0.3)
        
    def update_status(self, message: str):
        """Update the status message."""
        if self.status_text:
            self.status_text.markdown(
                f'<div class="status-text">{message} <span class="loading-dots">...</span></div>',
                unsafe_allow_html=True
            )
            
    def complete_loading(self):
        """Complete the loading process."""
        if self.progress_bar:
            self.progress_bar.progress(1.0)
        self.update_status("✅ Initialization complete!")
        time.sleep(0.5)
        
        # Clear loading interface
        if self.progress_container:
            self.progress_container.empty()


class AsyncLoader:
    """Async loading manager for background operations."""
    
    def __init__(self, loading_manager: LoadingManager):
        self.loading_manager = loading_manager
        self.operations: List[tuple] = []
        
    def add_operation(self, name: str, description: str, operation: Callable):
        """Add an operation to the loading queue."""
        self.operations.append((name, description, operation))
        
    def execute_operations(self):
        """Execute all operations with progress tracking."""
        total_ops = len(self.operations)
        self.loading_manager.total_steps = total_ops
        
        for i, (name, description, operation) in enumerate(self.operations):
            try:
                self.loading_manager.update_progress(name, description)
                result = operation()
                if hasattr(result, '__await__'):
                    # Handle async operations if needed in the future
                    pass
            except Exception as e:
                self.loading_manager.update_status(f"❌ Error in {name}: {str(e)}")
                time.sleep(1)
                continue
                
        self.loading_manager.complete_loading()


def show_startup_loading():
    """Show the initial startup loading screen."""
    # Initialize loading manager
    loader = LoadingManager()
    loader.initialize_loading("🚀 Initializing Atlas", 6)
    
    # Return the loader for further use
    return loader


def create_loading_placeholder():
    """Create a placeholder for loading that can be updated."""
    st.markdown("""
    <style>
    .early-loading-container {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        min-height: 80vh;
        text-align: center;
        background: linear-gradient(135deg, #1e1e2e 0%, #2d2d44 100%);
        border-radius: 20px;
        padding: 3rem;
        margin: 1rem;
        box-shadow: 0 20px 40px rgba(0, 0, 0, 0.3);
    }
    
    .atlas-logo {
        background: linear-gradient(135deg, #6366f1, #a78bfa);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        font-size: 4rem;
        font-weight: bold;
        margin-bottom: 1rem;
        animation: pulse-glow 2s ease-in-out infinite alternate;
    }
    
    .atlas-subtitle {
        color: #9ca3af;
        font-size: 1.4rem;
        margin-bottom: 3rem;
        opacity: 0.9;
    }
    
    .loading-spinner {
        width: 60px;
        height: 60px;
        border: 4px solid #374151;
        border-top: 4px solid #6366f1;
        border-radius: 50%;
        animation: spin 1s linear infinite;
        margin: 2rem auto;
    }
    
    .loading-message {
        color: #a78bfa;
        font-size: 1.1rem;
        margin-top: 1rem;
        animation: fade-in-out 2s ease-in-out infinite;
    }
    
    @keyframes spin {
        0% { transform: rotate(0deg); }
        100% { transform: rotate(360deg); }
    }
    
    @keyframes pulse-glow {
        0% { 
            transform: scale(1);
            filter: brightness(1);
        }
        100% { 
            transform: scale(1.05);
            filter: brightness(1.2);
        }
    }
    
    @keyframes fade-in-out {
        0%, 100% { opacity: 0.6; }
        50% { opacity: 1; }
    }
    </style>
    
    <div class="early-loading-container">
        <div class="atlas-logo">⚡ Atlas</div>
        <div class="atlas-subtitle">Performance Monitoring Dashboard</div>
        <div class="loading-spinner"></div>
        <div class="loading-message">Initializing system components...</div>
    </div>
    """, unsafe_allow_html=True)
    
    # Add a small delay to show the loading screen
    time.sleep(0.5)
