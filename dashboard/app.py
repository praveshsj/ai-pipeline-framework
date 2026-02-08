"""
AI Pipeline Dashboard
Shows AI analysis of failed tasks with recommendations

Author: Pravesh Sundriyal
"""

from flask import Flask, render_template, jsonify, request
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import sys
from datetime import datetime

sys.path.append('/opt/airflow')
from ai_analyzer.failure_analyzer import analyze_airflow_failure

app = Flask(__name__)

# Database connection
DB_CONN = os.environ.get('AIRFLOW_DB_CONN', 'postgresql://airflow:airflow@postgres/airflow')


def get_db_connection():
    """Get PostgreSQL connection"""
    return psycopg2.connect(DB_CONN, cursor_factory=RealDictCursor)


def get_failed_tasks(limit=20):
    """Get recent failed tasks from Airflow metadata"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
    SELECT 
        ti.task_id,
        ti.dag_id,
        ti.execution_date,
        ti.state,
        ti.try_number,
        ti.duration,
        ti.end_date,
        d.description as dag_description
    FROM task_instance ti
    JOIN dag d ON ti.dag_id = d.dag_id
    WHERE ti.state = 'failed'
    ORDER BY ti.end_date DESC
    LIMIT %s
    """
    
    cursor.execute(query, (limit,))
    results = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return results


@app.route('/')
def index():
    """Main dashboard page"""
    failed_tasks = get_failed_tasks(limit=20)
    return render_template('dashboard.html', failed_tasks=failed_tasks)


@app.route('/api/analyze/<dag_id>/<task_id>/<execution_date>')
def analyze_task(dag_id, task_id, execution_date):
    """
    Analyze a specific failed task using AI
    """
    try:
        # Run AI analysis
        result = analyze_airflow_failure(
            dag_id=dag_id,
            task_id=task_id,
            execution_date=execution_date,
        )
        
        return jsonify({
            "success": True,
            "analysis": result
        })
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route('/api/apply-recommendation', methods=['POST'])
def apply_recommendation():
    """
    Apply AI recommendation (retry, scale, etc.)
    """
    data = request.json
    dag_id = data.get('dag_id')
    task_id = data.get('task_id')
    action = data.get('action')
    
    # TODO: Implement action application
    # - RETRY: Trigger Airflow clear + re-run
    # - SCALE_UP: Update cluster config
    # - FAIL: Mark as failed
    # - FIX_CODE: Create Jira ticket or notify engineer
    
    return jsonify({
        "success": True,
        "message": f"Action '{action}' queued for {dag_id}.{task_id}"
    })


@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
