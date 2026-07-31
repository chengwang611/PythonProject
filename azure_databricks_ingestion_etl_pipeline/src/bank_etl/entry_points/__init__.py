"""Wheel task entry points for Databricks Workflows.

Each module exposes a ``main()`` function that accepts the same parameters
as the corresponding notebook, but via ``sys.argv`` or keyword arguments
rather than ``dbutils.widgets``.  These are registered as ``console_scripts``
in ``setup.py`` so they can be used with ``python_wheel_task`` in DAB jobs.
"""
