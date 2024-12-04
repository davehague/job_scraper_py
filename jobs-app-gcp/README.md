# Deployment Guide (GCP Functions)

This project contains a Google Cloud Function that runs on-demand via API calls. Follow these instructions to deploy the
function.

## Prerequisites

1. Install the [Google Cloud SDK](https://cloud.google.com/sdk)
2. Ensure you have Python 3.11 installed
3. Configure your project directory with:
    - `main.py` - Contains the function code
    - `requirements.txt` - Lists project dependencies
    - `.env.yaml` - Contains environment variables

## Set Project

Before deploying, ensure you're using the correct Google Cloud project:

```bash
# List available projects
gcloud projects list

# Set the active project
gcloud config set project <<your-project-id>>
```

## Deployment Steps

1. Navigate to your project directory:
   ```bash
   cd C:\Users\david\source\JobScraperApp\jobs-app-gcp
   ```

2. Deploy the function using the following command:
   ```bash
   gcloud functions deploy jobs_app_function --runtime python311 --trigger-http --allow-unauthenticated --gen2 --env-vars-file .env.yaml --memory 512M --timeout=180 --source .
   ```

### Command Options Explained

- `jobs_app_function`: The name of your function in `main.py`
- `--runtime python311`: Specifies Python 3.11 runtime
- `--trigger-http`: Sets up HTTP trigger for the function
- `--allow-unauthenticated`: Allows unauthenticated access (can be secured later)
- `--gen2`: Uses 2nd generation cloud functions
- `--env-vars-file .env.yaml`: Specifies environment variables file
- `--memory 512M`: Allocates 512MB of memory
- `--timeout=180`: Sets timeout to 180 seconds (3 minutes)
- `--source .`: Deploys from current directory

## Alternative Deployment Options

If your function is in a different file, you can specify it using the `--entry-point` flag:

```bash
gcloud functions deploy jobs_app_function --runtime python311 --entry-point my_function_file.py --trigger-http
```

## Configuration Notes

- Default timeout is 60 seconds. Adjust `--timeout` as needed (up to 540 seconds)
- Memory can be adjusted using `--memory` flag (128MB to 8192MB)
- For custom Python environments, set the `CLOUDSDK_PYTHON` environment variable to your Python executable path

## Function Structure

Your `main.py` should contain two main functions:

```python
# For scheduled jobs
def jobs_app_scheduled(event, context):
    print(event)
    print(context)
    return "Hello world!"


# For HTTP jobs
def jobs_app_function(context):
    print(context)
    return "Hello world!"
```

## Troubleshooting

- If deployment fails, check the Google Cloud Console logs
- Ensure all required dependencies are listed in `requirements.txt`
- Verify `.env.yaml` contains all necessary environment variables
- Check that the function name in the deployment command matches the function name in `main.py`