import os
import tempfile

from prefect import task

@task
def configure_gcp_credentials():
    gcp_credentials = os.getenv("GCP_CREDENTIALS")

    # Caso CI/CD: JSON direto no env
    if gcp_credentials and gcp_credentials.strip().startswith("{"):
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json") as f:
            f.write(gcp_credentials)
            cred_path = f.name

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path
        return cred_path

    # Caso dev local: caminho já definido
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if cred_path:
        return cred_path

    # Fallback: Application Default Credentials
    return None
