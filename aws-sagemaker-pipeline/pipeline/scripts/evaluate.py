#!/usr/bin/env python3
"""
Evaluation Script for SageMaker Pipeline

This script is executed by the ScriptProcessor in the EvaluateModel step.
It loads the trained model and test data, computes evaluation metrics,
and writes an evaluation report.

Input:  /opt/ml/processing/model/  (model.tar.gz from training)
        /opt/ml/processing/test/   (test CSV)
Output: /opt/ml/processing/output/evaluation/evaluation.json
"""

import argparse
import os
import json
import logging
import tarfile
import pickle
import xgboost as xgb
import pandas as pd
import numpy as np
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, confusion_matrix, classification_report,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Evaluate XGBoost model')
    parser.add_argument('--model-path', default='/opt/ml/processing/model', help='Path to model artifacts')
    parser.add_argument('--test-path', default='/opt/ml/processing/test', help='Path to test data')
    parser.add_argument('--output-path', default='/opt/ml/processing/output/evaluation', help='Output path for evaluation')
    return parser.parse_args()


def load_model(model_path: str):
    """Load the XGBoost model from model.tar.gz."""
    # Find and extract model.tar.gz
    tar_files = [f for f in os.listdir(model_path) if f.endswith('.tar.gz')]
    if not tar_files:
        # Try loading .xgb directly
        xgb_files = [f for f in os.listdir(model_path) if f.endswith('.xgb')]
        if xgb_files:
            model_file = os.path.join(model_path, xgb_files[0])
            model = xgb.Booster()
            model.load_model(model_file)
            logger.info(f"Loaded model from: {model_file}")
            return model
        raise FileNotFoundError(f"No model files found in {model_path}")

    # Extract tar.gz
    tar_path = os.path.join(model_path, tar_files[0])
    extract_dir = os.path.join(model_path, 'extracted')
    os.makedirs(extract_dir, exist_ok=True)

    with tarfile.open(tar_path, 'r:gz') as tar:
        tar.extractall(path=extract_dir)

    # Find the model file
    xgb_files = []
    for root, dirs, files in os.walk(extract_dir):
        for f in files:
            if f.endswith('.xgb') or f.endswith('.model') or f == 'model':
                xgb_files.append(os.path.join(root, f))

    if not xgb_files:
        raise FileNotFoundError(f"No model file found in extracted tar: {extract_dir}")

    model = xgb.Booster()
    model.load_model(xgb_files[0])
    logger.info(f"Loaded model from: {xgb_files[0]}")
    return model


def load_test_data(test_path: str) -> tuple:
    """Load test data. CSV has header, target is first column."""
    files = [f for f in os.listdir(test_path) if f.endswith('.csv')]
    if not files:
        raise FileNotFoundError(f"No CSV files in {test_path}")

    df = pd.read_csv(os.path.join(test_path, files[0]))
    X_test = df.iloc[:, 1:].values
    y_test = df.iloc[:, 0].values
    logger.info(f"Loaded {len(df)} test rows, {X_test.shape[1]} features")
    return X_test, y_test


def evaluate_model(model, X_test: np.ndarray, y_test: np.ndarray) -> dict:
    """Compute evaluation metrics."""
    dtest = xgb.DMatrix(X_test)
    y_pred_proba = model.predict(dtest)
    y_pred = (y_pred_proba >= 0.5).astype(int)

    metrics = {
        'accuracy': float(accuracy_score(y_test, y_pred)),
        'precision': float(precision_score(y_test, y_pred, zero_division=0)),
        'recall': float(recall_score(y_test, y_pred, zero_division=0)),
        'f1_score': float(f1_score(y_test, y_pred, zero_division=0)),
        'roc_auc': float(roc_auc_score(y_test, y_pred_proba)),
        'confusion_matrix': confusion_matrix(y_test, y_pred).tolist(),
        'true_count': int(np.sum(y_test)),
        'false_count': int(len(y_test) - np.sum(y_test)),
        'total_samples': len(y_test),
    }

    logger.info(f"Evaluation metrics:")
    for key, value in metrics.items():
        if key != 'confusion_matrix':
            logger.info(f"  {key}: {value:.4f}" if isinstance(value, float) else f"  {key}: {value}")

    return metrics


def save_evaluation(metrics: dict, output_path: str):
    """Save evaluation metrics as JSON."""
    os.makedirs(output_path, exist_ok=True)
    output_file = os.path.join(output_path, 'evaluation.json')

    with open(output_file, 'w') as f:
        json.dump(metrics, f, indent=2)

    logger.info(f"Evaluation saved to: {output_file}")


def main():
    args = parse_args()
    logger.info("Starting model evaluation")

    # Load model
    model = load_model(args.model_path)

    # Load test data
    X_test, y_test = load_test_data(args.test_path)

    # Evaluate
    metrics = evaluate_model(model, X_test, y_test)

    # Save
    save_evaluation(metrics, args.output_path)

    logger.info("Evaluation complete!")


if __name__ == '__main__':
    main()
