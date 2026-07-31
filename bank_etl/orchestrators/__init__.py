"""Orchestrator modules — reusable business logic shared by notebooks and wheel-task entry points.

Each module exposes a ``run()`` function that accepts keyword arguments (config_path,
trade_date, etc.) and returns a JSON-serializable summary dict.  Notebooks and
entry_points are thin wrappers that only handle parameter plumbing (widgets or
argparse) and delegate everything else to these orchestrators.
"""
