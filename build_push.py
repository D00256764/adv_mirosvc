import sys
import subprocess

def build_script(version="latest", build_env="prod"):
    services = ["inventory-service","order-service","payment-service"]
    repo = "hafsa22/adv_microsvc"

    for service in services:
        tag = f"{repo}:{service}"
        try:
            print(f"\nBuilding and pushing {service}...")
            subprocess.run(
                [
                    "docker", "buildx", "build",
                    "--platform", "linux/amd64",
                    "-t", tag,
                    "--push",
                    f"./{service}",
                ],
                check=True,
            )
            print(f"Done: {tag}")

        
        except subprocess.CalledProcessError as e:
            print(f"Failed on {service}")
            sys.exit(1)

if __name__ == "__main__":
    build_script()