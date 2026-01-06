import os
import sys
import glob
import typer
from pathlib import Path
from importlib.machinery import SourceFileLoader
from prefect import Flow

app = typer.Typer()

def load_flows_from_file(file_path: Path, module_name: str):
    """Loads flow objects from a Python file using a specific module name."""
    try:
        loader = SourceFileLoader(module_name, str(file_path))
        module = loader.load_module()
        
        flows = []
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if isinstance(attr, Flow):
                flows.append(attr)
        return flows
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return []

def get_deployment_tags(file_path: Path):
    """Generates tags based on file path structure."""
    parts = file_path.parts
    tags = []
    
    # Ex: pipelines/acolherio/flows.py -> acolherio
    if "pipelines" in parts:
        idx = parts.index("pipelines")
        if idx + 1 < len(parts):
            tags.append(parts[idx+1]) 
            
    return list(set(tags))

@app.command()
def register(
    env: str = typer.Option(..., help="Environment (staging or prod)"),
    image_tag: str = typer.Option(..., help="Docker image tag (SHA)")
):
    """
    Finds and deploys all flows in pipelines/**/flows.py
    """
    # Root relative to this script location: .github/workflows/scripts/register_flows.py
    project_root = Path(__file__).parent.parent.parent.parent
    pipeline_dir = project_root / "pipelines"
    
    # Add project root to sys.path to allow absolute imports (from pipelines.xxx)
    # This matches the Docker container structure where /app is WORKDIR and contains pipelines/
    sys.path.append(str(project_root))
    
    print(f"Searching for flows in {pipeline_dir}...")
    
    flow_files = glob.glob(str(pipeline_dir / "**" / "flows.py"), recursive=True)
    
    # Construct Image Name
    github_repo = os.getenv("GITHUB_REPOSITORY")
    if not github_repo:
        print("Warning: GITHUB_REPOSITORY env var not set. Using default 'rj-smas/pipelines-rj-smas'")
        github_repo = "rj-smas/pipelines-rj-smas"
        
    full_image_name = f"ghcr.io/{github_repo.lower()}:{image_tag}"
    print(f"Target Docker Image: {full_image_name}")

    # Validate Authentication (Self-hosted support)
    api_key = os.getenv("PREFECT_API_KEY")
    auth_string = os.getenv("PREFECT_API_AUTH_STRING")
    
    if not api_key and not auth_string:
        print("\n❌ CRITICAL ERROR: Authentication missing!")
        print("Neither 'PREFECT_API_KEY' nor 'PREFECT_API_AUTH_STRING' found.")
        print("Please check your GitHub Secrets.")
        exit(1)

    deployment_count = 0
    error_count = 0

    for file_path in flow_files:
        path_obj = Path(file_path)
        
        # Calculate full dotted module name (e.g. pipelines.acolherio.flows)
        # This ensures:
        # 1. Unique module names (avoid cache collisions)
        # 2. Correct entrypoint calculation by Prefect (matches Docker structure)
        # 3. Correct relative imports inside the module
        try:
            rel_path = path_obj.relative_to(project_root)
            module_name = str(rel_path).replace(os.sep, ".").replace(".py", "")
        except ValueError:
            # Fallback if file is somehow outside root (shouldn't happen with glob)
            module_name = f"flow_module_{path_obj.stem}"

        # Load flows
        flows = load_flows_from_file(path_obj, module_name)
        
        if not flows:
            continue

        for flow in flows:
            # Naming convention: folder-name-env
            base_name = path_obj.parent.name.replace("_", "-")
            deployment_name = f"{base_name}-{env}"
            
            tags = get_deployment_tags(path_obj)
            tags.append(env)
            
            # Common Environment Variables
            env_vars = {
                "MODE": env,
                # Use internal API URL for the worker inside the docker network (matches legacy prefect.yaml)
                "PREFECT_API_URL": "http://prefect-api:4200/api",
                "PREFECT_API_AUTH_STRING": auth_string or "",
                "GCP_CREDENTIALS": os.getenv("GCP_CREDENTIALS", ""),
                "SIURB_URL": os.getenv("SIURB_URL", ""),
                "SIURB_USER": os.getenv("SIURB_USER", ""),
                "SIURB_PWD": os.getenv("SIURB_PWD", ""),
            }
            
            # Job Variables (including Docker Network)
            job_vars = {
                "env": env_vars,
                "networks": ["prefect_prefect-stack"] # Critical for self-hosted to reach API/DB
            }

            # Handle Scheduling
            schedules = flow.schedules if env == "prod" else []
            
            print(f"Deploying {flow.name} -> {deployment_name}...")
            try:
                # Flow deploy automatically infers entrypoint from the flow object function
                # Since we loaded the module with the correct dotted path (pipelines.xxx.flows),
                # Prefect will correctly set the entrypoint to 'pipelines/xxx/flows.py:flow_fn'
                flow.deploy(
                    name=deployment_name,
                    work_pool_name="docker-pool",
                    image=full_image_name,
                    tags=tags,
                    job_variables=job_vars,
                    schedules=schedules,
                    build=False
                )
                print(f"✅ Successfully deployed {deployment_name}")
                deployment_count += 1
            except Exception as e:
                print(f"❌ Failed to deploy {deployment_name}: {e}")
                error_count += 1

    print(f"\nSummary: {deployment_count} deployments successful, {error_count} failed.")
    if error_count > 0:
        exit(1)

if __name__ == "__main__":
    app()
