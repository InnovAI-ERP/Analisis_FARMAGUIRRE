import os
import sys
import subprocess
from pathlib import Path

def handler(event, context):
    """
    Netlify function to serve Streamlit app
    """
    # Add the project root to Python path
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))
    
    # Set environment variables
    os.environ['STREAMLIT_SERVER_PORT'] = '8501'
    os.environ['STREAMLIT_SERVER_ADDRESS'] = '0.0.0.0'
    os.environ['STREAMLIT_SERVER_HEADLESS'] = 'true'
    
    try:
        # Import and run the Streamlit app
        import streamlit.web.cli as stcli
        import streamlit as st
        
        # Configure Streamlit
        st.set_page_config(
            page_title="Análisis de Inventario - Farmaguirre",
            page_icon="💊",
            layout="wide"
        )
        
        # Import and run the main app
        from app import main
        main()
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'text/html',
            },
            'body': 'Streamlit app is running'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
            },
            'body': f'{{"error": "Failed to start Streamlit app: {str(e)}"}}'
        }
