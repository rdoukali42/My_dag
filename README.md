# dagster_pipeline

This project implements an end-to-end MLOps workflow using a Dagster pipeline. It demonstrates how to orchestrate various stages of a machine learning lifecycle, from data acquisition to model deployment and prediction.

The key stages of this pipeline include:
- **Data Loading**: Ingesting raw data from various sources.
- **Preprocessing**: Cleaning, transforming, and preparing the data for model training.
- **Model Training (XGBoost)**: Training a classification or regression model using the XGBoost library.
- **Model Deployment (MLflow)**: Logging the trained model and its artifacts to MLflow for tracking and versioning, preparing it for deployment.
- **Prediction**: Using the deployed model to make predictions on new data.

This project also integrates with [LakeFS](https://lakefs.io/) for robust data versioning, ensuring reproducibility and traceability of datasets throughout the MLOps workflow.

## Getting started

First, install your Dagster code location, `dagster_pipeline`, as a Python package. By using the --editable flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

You can start writing assets in `dagster_pipeline/assets.py`. The assets are automatically loaded into the Dagster code location as you define them.

## Development

### Adding new Python dependencies

You can specify new Python dependencies in `setup.py`.

### Unit testing

Tests are in the `dagster_pipeline_tests` directory and you can run tests using `pytest`:

```bash
pytest dagster_pipeline_tests
```

### Schedules and sensors

If you want to enable Dagster [Schedules](https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules) or [Sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors) for your jobs, the [Dagster Daemon](https://docs.dagster.io/deployment/dagster-daemon) process must be running. This is done automatically when you run `dagster dev`.

Once your Dagster Daemon is running, you can start turning on schedules and sensors for your jobs.

## Testing Your Pipeline

Beyond unit tests, you'll want to test the full pipeline functionality.

### Running Unit Tests

As mentioned in the Development section, unit tests are located in the `dagster_pipeline_tests` directory. You can run them using `pytest`:

```bash
pytest dagster_pipeline_tests
```

### End-to-End Pipeline Testing

To test the pipeline components and their integrations:

1.  **Manual Pipeline Runs**:
    *   Ensure the Dagster UI is running (`dagster dev`).
    *   Navigate to `http://localhost:3000`.
    *   You can manually trigger pipeline runs or individual asset materializations from the "Assets" or "Jobs" tabs.
    *   This allows you to test the flow of data and the execution of each step.

2.  **Inspecting Outputs**:
    *   After a run, inspect the asset materializations in the Dagster UI. Check the "Activity" tab and click on individual runs to see detailed logs and outputs.
    *   For assets stored via I/O managers (e.g., data versioned with LakeFS), you can also verify the data directly in the configured storage or LakeFS repository.

3.  **Checking Jobs and Sensors**:
    *   Monitor the status of scheduled jobs and sensors in the Dagster UI under their respective tabs.
    *   You can manually trigger sensors to test their logic or observe their behavior based on external conditions.

### Configuration for Testing

When testing the pipeline, especially components interacting with external services, ensure the necessary configurations and environment variables are set. This project relies on:

*   **LakeFS**:
    *   `LAKEFS_HOST`: The URL of your LakeFS server.
    *   `LAKEFS_USERNAME`: Your LakeFS access key ID.
    *   `LAKEFS_PASSWORD`: Your LakeFS secret access key.
    *   `LAKEFS_REPOSITORY`: The name of the LakeFS repository used by the pipeline.
    *   `LAKEFS_DEFAULT_BRANCH`: The default branch for the LakeFS repository (e.g., `main`).
    *   `LAKEFS_URI`: The full URI for connecting to LakeFS, often in the format `lakefs://<repository>/<branch>/`.
*   **MLflow**:
    *   Ensure your MLflow tracking server is accessible. The pipeline's `mlflow_resource` will typically connect to it using environment variables like `MLFLOW_TRACKING_URI`.

Refer to the `dagster_pipeline/resources.py` and `dagster_pipeline/repository.py` files for details on how these resources are configured. You might need to set these environment variables in your local development environment or CI/CD system for tests to run correctly.

## Deploy on Dagster Cloud

The easiest way to deploy your Dagster project is to use Dagster Cloud.

Check out the [Dagster Cloud Documentation](https://docs.dagster.cloud) to learn more.
