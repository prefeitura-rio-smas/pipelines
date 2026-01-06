import os
import tempfile

gcp_credentials = os.getenv("GCP_CREDENTIALS")

if gcp_credentials:
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
        f.write(gcp_credentials)
        cred_path = f.name
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path
        