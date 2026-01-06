import os
import glob
import typer
from pathlib import Path
from importlib.machinery import SourceFileLoader
from prefect import Flow

app = typer.Typer()

def load_flows_from_file(file_path: Path):
    """Loads flow objects from a Python file."""
    try:
        module_name = file_path.stem
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
    
    print(f"Searching for flows in {pipeline_dir}...")
    
    flow_files = glob.glob(str(pipeline_dir / "**" / "flows.py"), recursive=True)
    
    # Construct Image Name
    # GitHub Actions sets GITHUB_REPOSITORY (e.g., owner/repo)
    github_repo = os.getenv("GITHUB_REPOSITORY")
    if not github_repo:
        print("Warning: GITHUB_REPOSITORY env var not set. Using default 'rj-smas/pipelines-rj-smas'")
        github_repo = "rj-smas/pipelines-rj-smas"
        
    # Lowercase is required for docker image names
    full_image_name = f"ghcr.io/{github_repo.lower()}:{image_tag}"
    print(f"Target Docker Image: {full_image_name}")

    deployment_count = 0
    error_count = 0

    for file_path in flow_files:
        path_obj = Path(file_path)
        flows = load_flows_from_file(path_obj)
        
        if not flows:
            continue

        for flow in flows:
            # Naming convention: folder-name-env
            # If nested like arcgis/abordagem, we might want abordagem-env or arcgis-abordagem-env
            # Using parent folder name is safer for now to avoid huge names
            base_name = path_obj.parent.name.replace("_", "-")
            deployment_name = f"{base_name}-{env}"
            
            tags = get_deployment_tags(path_obj)
            tags.append(env)
            
            # Common Environment Variables
            env_vars = {
                "MODE": env,
                "PREFECT_API_URL": os.getenv("PREFECT_API_URL", "http://prefect-api:4200/api"),
                "GCP_CREDENTIALS": os.getenv("GCP_CREDENTIALS", ""),
                "SIURB_URL": os.getenv("SIURB_URL", ""),
                "SIURB_USER": os.getenv("SIURB_USER", ""),
                "SIURB_PWD": os.getenv("SIURB_PWD", ""),
            }

            # Handle Scheduling
            # Only enable schedules in prod
            schedules = flow.schedules if env == "prod" else []
            
            # Prefect 3.x deployment logic
            print(f"Deploying {flow.name} -> {deployment_name}...")
            try:
                # We define entrypoint relative to project root because that's how it's copied in Dockerfile
                # /app/pipelines/...
                # script is running from root in CI, file_path is absolute or relative to root
                rel_path = path_obj.relative_to(project_root)
                entrypoint = f"{rel_path}:{flow.fn.__name__}"
                
                flow.deploy(
                    name=deployment_name,
                    work_pool_name="docker-pool",
                    image=full_image_name,
                    tags=tags,
                    job_variables={"env": env_vars},
                    schedules=schedules,
                    entrypoint=entrypoint,
                    print_next_steps_message=False
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
