"""
AI Failure Analyzer
Analyzes data pipeline failures and recommends intelligent actions

Author: Pravesh Sundriyal
"""

import anthropic
import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from enum import Enum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FailureAction(Enum):
    """Recommended actions for failures"""
    RETRY = "retry"
    SCALE_UP = "scale_up"
    FAIL = "fail"
    FIX_CODE = "fix_code"
    CHECK_DATA = "check_data"
    INCREASE_TIMEOUT = "increase_timeout"


class FailureCategory(Enum):
    """Categories of failures"""
    TRANSIENT = "transient"  # Network, timeout
    RESOURCE = "resource"  # OOM, CPU
    DATA_QUALITY = "data_quality"  # Schema, null values
    CODE_ERROR = "code_error"  # Syntax, logic
    DEPENDENCY = "dependency"  # External service down
    CONFIGURATION = "configuration"  # Wrong params


class DeterministicRulesEngine:
    """
    Deterministic rules for safety checks before AI analysis
    """
    
    @staticmethod
    def should_auto_retry(error_message: str, retry_count: int) -> bool:
        """
        Deterministic check: Should we auto-retry without AI?
        """
        # Safety: Never auto-retry more than 3 times
        if retry_count >= 3:
            return False
        
        # Known transient errors - safe to retry
        transient_errors = [
            'connection reset',
            'timeout',
            'temporarily unavailable',
            'network unreachable',
            'connection refused',
            'broken pipe',
        ]
        
        error_lower = error_message.lower()
        return any(err in error_lower for err in transient_errors)
    
    @staticmethod
    def is_critical_failure(error_message: str) -> bool:
        """
        Deterministic check: Is this a critical failure that should never retry?
        """
        critical_errors = [
            'permission denied',
            'access denied',
            'authentication failed',
            'invalid credentials',
            'table not found',
            'database does not exist',
        ]
        
        error_lower = error_message.lower()
        return any(err in error_lower for err in critical_errors)
    
    @staticmethod
    def categorize_failure(error_message: str, task_logs: str) -> FailureCategory:
        """
        Deterministic categorization of failure
        """
        error_lower = error_message.lower()
        logs_lower = task_logs.lower()
        
        # Check for resource issues
        if any(keyword in error_lower or keyword in logs_lower for keyword in [
            'out of memory', 'oom', 'memory error', 'heap space',
            'disk space', 'no space left'
        ]):
            return FailureCategory.RESOURCE
        
        # Check for data quality issues
        if any(keyword in error_lower or keyword in logs_lower for keyword in [
            'null', 'schema', 'datatype', 'parse error', 'invalid format',
            'constraint violation', 'duplicate key'
        ]):
            return FailureCategory.DATA_QUALITY
        
        # Check for code errors
        if any(keyword in error_lower or keyword in logs_lower for keyword in [
            'syntax error', 'name error', 'import error', 'attribute error',
            'type error', 'key error', 'index error'
        ]):
            return FailureCategory.CODE_ERROR
        
        # Check for dependency issues
        if any(keyword in error_lower or keyword in logs_lower for keyword in [
            'connection', 'timeout', 'unreachable', 'refused', 'unavailable'
        ]):
            return FailureCategory.DEPENDENCY
        
        # Check for configuration issues
        if any(keyword in error_lower or keyword in logs_lower for keyword in [
            'configuration', 'config', 'parameter', 'invalid argument'
        ]):
            return FailureCategory.CONFIGURATION
        
        # Default to transient if unclear
        return FailureCategory.TRANSIENT


class AIFailureAnalyzer:
    """
    AI-powered failure analysis using Claude
    """
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY not set")
        
        self.client = anthropic.Anthropic(api_key=self.api_key)
        self.rules_engine = DeterministicRulesEngine()
    
    def analyze_failure(
        self,
        task_id: str,
        dag_id: str,
        error_message: str,
        task_logs: str,
        dag_code: Optional[str] = None,
        retry_count: int = 0,
        task_duration_seconds: Optional[int] = None,
        previous_success: bool = False,
    ) -> Dict:
        """
        Complete failure analysis with deterministic rules + AI
        """
        logger.info(f"Analyzing failure for {dag_id}.{task_id}")
        
        # Step 1: Deterministic safety checks
        if self.rules_engine.is_critical_failure(error_message):
            return {
                "analysis_type": "deterministic",
                "category": FailureCategory.CONFIGURATION.value,
                "severity": "critical",
                "root_cause": "Critical configuration or permission error",
                "explanation": error_message,
                "recommended_action": FailureAction.FAIL.value,
                "reasoning": "This is a critical error that cannot be resolved by retry. Manual intervention required.",
                "auto_apply": False,
                "confidence": 1.0,
            }
        
        # Step 2: Check if safe to auto-retry
        if self.rules_engine.should_auto_retry(error_message, retry_count):
            return {
                "analysis_type": "deterministic",
                "category": FailureCategory.TRANSIENT.value,
                "severity": "low",
                "root_cause": "Transient error (network/timeout)",
                "explanation": "Known transient error pattern detected",
                "recommended_action": FailureAction.RETRY.value,
                "reasoning": "This is a transient error that typically resolves on retry.",
                "auto_apply": True,
                "confidence": 0.95,
                "retry_delay_seconds": min(300, 60 * (2 ** retry_count)),  # Exponential backoff
            }
        
        # Step 3: Categorize failure
        category = self.rules_engine.categorize_failure(error_message, task_logs)
        
        # Step 4: AI-powered deep analysis
        logger.info(f"Category: {category.value} - Calling AI for deep analysis...")
        
        ai_analysis = self._call_ai_analyzer(
            task_id=task_id,
            dag_id=dag_id,
            error_message=error_message,
            task_logs=task_logs,
            dag_code=dag_code,
            category=category,
            retry_count=retry_count,
            task_duration_seconds=task_duration_seconds,
            previous_success=previous_success,
        )
        
        # Combine deterministic + AI analysis
        result = {
            "analysis_type": "ai_assisted",
            "category": category.value,
            **ai_analysis,
            "timestamp": datetime.now().isoformat(),
            "analyzer_version": "1.0.0",
        }
        
        logger.info(f"Analysis complete: {result['recommended_action']}")
        return result
    
    def _call_ai_analyzer(
        self,
        task_id: str,
        dag_id: str,
        error_message: str,
        task_logs: str,
        dag_code: Optional[str],
        category: FailureCategory,
        retry_count: int,
        task_duration_seconds: Optional[int],
        previous_success: bool,
    ) -> Dict:
        """
        Call Claude API for intelligent failure analysis
        """
        # Prepare context for AI
        prompt = f"""You are an expert data engineer analyzing a failed Airflow task.

DAG ID: {dag_id}
Task ID: {task_id}
Failure Category: {category.value}
Retry Count: {retry_count}
Previous Runs: {"✅ Previously successful" if previous_success else "❌ Never succeeded"}
Task Duration: {task_duration_seconds}s (before failure)

ERROR MESSAGE:
{error_message}

TASK LOGS (last 2000 chars):
{task_logs[-2000:]}

{"DAG CODE:" if dag_code else ""}
{dag_code[:1000] if dag_code else ""}

Provide a detailed analysis in JSON format:
{{
  "severity": "critical" | "high" | "medium" | "low",
  "root_cause": "Brief explanation of what went wrong",
  "explanation": "Detailed explanation for the engineer",
  "recommended_action": "retry" | "scale_up" | "fail" | "fix_code" | "check_data" | "increase_timeout",
  "reasoning": "Why this action is recommended",
  "specific_steps": ["Step 1", "Step 2", ...],
  "code_fix": "Suggested code changes (if applicable)",
  "auto_apply": true | false,
  "confidence": 0.0-1.0,
  "estimated_fix_time_minutes": int,
  "similar_issues": ["Link to similar issue 1", ...],
  "prevention": "How to prevent this in future"
}}

Be specific and actionable. Focus on helping the engineer fix this quickly."""

        try:
            message = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}]
            )
            
            # Parse AI response
            response_text = message.content[0].text
            
            # Extract JSON from response (handle markdown code blocks)
            if "```json" in response_text:
                json_start = response_text.find("```json") + 7
                json_end = response_text.find("```", json_start)
                response_text = response_text[json_start:json_end].strip()
            elif "```" in response_text:
                json_start = response_text.find("```") + 3
                json_end = response_text.find("```", json_start)
                response_text = response_text[json_start:json_end].strip()
            
            ai_result = json.loads(response_text)
            
            logger.info(f"AI Analysis: {ai_result['root_cause']}")
            return ai_result
            
        except Exception as e:
            logger.error(f"AI analysis failed: {str(e)}")
            
            # Fallback to deterministic recommendation
            return self._fallback_recommendation(category, retry_count)
    
    def _fallback_recommendation(
        self,
        category: FailureCategory,
        retry_count: int
    ) -> Dict:
        """
        Fallback recommendations if AI fails
        """
        recommendations = {
            FailureCategory.TRANSIENT: {
                "severity": "medium",
                "root_cause": "Transient failure detected",
                "explanation": "Network or temporary service issue",
                "recommended_action": FailureAction.RETRY.value if retry_count < 3 else FailureAction.FAIL.value,
                "reasoning": "Transient errors usually resolve on retry",
                "auto_apply": retry_count < 2,
                "confidence": 0.7,
            },
            FailureCategory.RESOURCE: {
                "severity": "high",
                "root_cause": "Resource exhaustion (memory/CPU/disk)",
                "explanation": "Task ran out of resources",
                "recommended_action": FailureAction.SCALE_UP.value,
                "reasoning": "Need more resources to complete successfully",
                "auto_apply": False,
                "confidence": 0.8,
            },
            FailureCategory.DATA_QUALITY: {
                "severity": "high",
                "root_cause": "Data quality issue detected",
                "explanation": "Invalid data format or constraint violation",
                "recommended_action": FailureAction.CHECK_DATA.value,
                "reasoning": "Need to investigate and fix data issues",
                "auto_apply": False,
                "confidence": 0.75,
            },
            FailureCategory.CODE_ERROR: {
                "severity": "critical",
                "root_cause": "Code error detected",
                "explanation": "Syntax or logic error in the code",
                "recommended_action": FailureAction.FIX_CODE.value,
                "reasoning": "Code needs to be fixed before retry",
                "auto_apply": False,
                "confidence": 0.9,
            },
            FailureCategory.DEPENDENCY: {
                "severity": "medium",
                "root_cause": "External dependency failure",
                "explanation": "External service or database unavailable",
                "recommended_action": FailureAction.RETRY.value if retry_count < 5 else FailureAction.FAIL.value,
                "reasoning": "Wait for external service to recover",
                "auto_apply": retry_count < 3,
                "confidence": 0.65,
            },
            FailureCategory.CONFIGURATION: {
                "severity": "high",
                "root_cause": "Configuration error",
                "explanation": "Invalid configuration or parameters",
                "recommended_action": FailureAction.FIX_CODE.value,
                "reasoning": "Configuration needs to be corrected",
                "auto_apply": False,
                "confidence": 0.85,
            },
        }
        
        return recommendations.get(category, recommendations[FailureCategory.TRANSIENT])


def analyze_airflow_failure(
    task_id: str,
    dag_id: str,
    execution_date: str,
    logs_path: str = "/opt/airflow/logs",
) -> Dict:
    """
    Main entry point for analyzing Airflow task failures
    """
    try:
        # Read task logs
        log_file = f"{logs_path}/{dag_id}/{task_id}/{execution_date}/1.log"
        with open(log_file, 'r') as f:
            task_logs = f.read()
        
        # Extract error message (last 500 chars usually contain the error)
        error_message = task_logs[-500:]
        
        # Initialize analyzer
        analyzer = AIFailureAnalyzer()
        
        # Analyze failure
        result = analyzer.analyze_failure(
            task_id=task_id,
            dag_id=dag_id,
            error_message=error_message,
            task_logs=task_logs,
            retry_count=0,  # Get from Airflow metadata
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to analyze: {str(e)}")
        return {
            "error": str(e),
            "recommended_action": FailureAction.FAIL.value,
        }


if __name__ == "__main__":
    # Test the analyzer
    import sys
    
    if len(sys.argv) < 4:
        print("Usage: python ai_analyzer.py <dag_id> <task_id> <execution_date>")
        sys.exit(1)
    
    dag_id = sys.argv[1]
    task_id = sys.argv[2]
    execution_date = sys.argv[3]
    
    result = analyze_airflow_failure(dag_id, task_id, execution_date)
    
    print("\n" + "="*80)
    print("AI FAILURE ANALYSIS RESULT")
    print("="*80)
    print(json.dumps(result, indent=2))
    print("="*80)
