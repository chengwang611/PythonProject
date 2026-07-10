#!/usr/bin/env python3
"""
Training Script for SageMaker Pipeline (XGBoost)

This script is executed by the XGBoost Estimator in the TrainModel step.
It reads preprocessed training and validation data, trains an XGBoost model,
and saves the model artifact.

Input:  /opt/ml/input/data/train/     (training CSV)
        /opt/ml/input/data/validation/ (validation CSV)
Output: /opt/ml/model/                 (model.tar.gz)
"""

import argparse
import os
import json
import logging
import pickle
import xgboost as xgb
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Train XGBoost model')
    parser.add_argument('--max_depth', type=int, default=5)
    parser.add_argument('--eta', type=float, default=0.2)
    parser.add_argument('--gamma', type=float, default=4)
    parser.add_argument('--min_child_weight', type=int, default=6)
    parser.add_argument('--subsample', type=float, default=0.8)
    parser.add_argument('--objective', type=str, default='binary:logistic')
    parser.add_argument('--num_round', type=int, default=100)
    parser.add_argument('--eval_metric', type=str, default='auc')
    parser.add_argument('--early_stopping_rounds', type=int, default=10)
    parser.add_argument('--model-dir', type=str, default=os.environ.get('SM_MODEL_DIR', '/opt/ml/model'))
    parser.add_argument('--train', type=str, default=os.environ.get('SM_CHANNEL_TRAIN', '/opt/ml/input/data/train'))
    parser.add_argument('--validation', type=str, default=os.environ.get('SM_CHANNEL_VALIDATION', '/opt/ml/input/data/validation'))
    return parser.parse_args()


def load_dataset(path: str) -> tuple:
    """Load CSV data. XGBoost expects target as first column, no header."""
    files = [f for f in os.listdir(path) if f.endswith('.csv')]
    if not files:
        raise FileNotFoundError(f"No CSV files in {path}")

    df = pd.read_csv(os.path.join(path, files[0]), header=None)
    X = df.iloc[:, 1:].values  # Features (all columns except first)
    y = df.iloc[:, 0].values   # Target (first column)
    logger.info(f"Loaded {len(df)} rows, {X.shape[1]} features from {path}")
    return X, y


def main():
    args = parse_args()
    logger.info(f"Starting XGBoost training with params: max_depth={args.max_depth}, eta={args.eta}, num_round={args.num_round}")

    # Load data
    X_train, y_train = load_dataset(args.train)
    X_val, y_val = load_dataset(args.validation)

    # Create DMatrix objects
    dtrain = xgb.DMatrix(X_train, label=y_train)
    dval = xgb.DMatrix(X_val, label=y_val)

    # Training parameters
    params = {
        'max_depth': args.max_depth,
        'eta': args.eta,
        'gamma': args.gamma,
        'min_child_weight': args.min_child_weight,
        'subsample': args.subsample,
        'objective': args.objective,
        'eval_metric': args.eval_metric,
        'verbosity': 1,
    }

    # Train with early stopping
    model = xgb.train(
        params=params,
        dtrain=dtrain,
        num_boost_round=args.num_round,
        evals=[(dtrain, 'train'), (dval, 'validation')],
        early_stopping_rounds=args.early_stopping_rounds,
        verbose_eval=10,
    )

    # Save model
    model_path = os.path.join(args.model_dir, 'model.xgb')
    model.save_model(model_path)
    logger.info(f"Model saved to: {model_path}")

    # Save model metadata
    metadata = {
        'best_iteration': model.best_iteration,
        'best_score': model.best_score,
        'feature_count': X_train.shape[1],
        'params': params,
    }
    metadata_path = os.path.join(args.model_dir, 'metadata.json')
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    logger.info(f"Metadata saved: {metadata}")


if __name__ == '__main__':
    main()
